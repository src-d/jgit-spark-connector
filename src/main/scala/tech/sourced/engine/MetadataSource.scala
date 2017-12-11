package tech.sourced.engine

import java.nio.file.Paths
import java.sql.{Date, Timestamp}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{MetadataBuilder, StructType}
import org.apache.spark.sql.{Row, SQLContext, SparkSession}
import org.apache.spark._
import tech.sourced.engine.provider.{RepositoryProvider, RepositoryRDDProvider}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, Expression}
import org.apache.spark.sql.sources._
import org.apache.spark.unsafe.types.UTF8String
import tech.sourced.engine.iterator._

/**
  * Data source to provide new metadata relations.
  */
class MetadataSource extends RelationProvider with DataSourceRegister {

  /** @inheritdoc */
  override def shortName: String = "metadata"

  /** @inheritdoc */
  override def createRelation(sqlContext: SQLContext,
                              parameters: Map[String, String]): BaseRelation = {
    val table = parameters.getOrElse(
      DefaultSource.tableNameKey,
      throw new SparkException(s"parameter '${DefaultSource.tableNameKey}' must be provided")
    )

    val dbPath = Paths.get(
      parameters.getOrElse(
        MetadataSource.DbPathKey,
        throw new SparkException(s"parameter '${MetadataSource.DbPathKey}' must be provided")
      ),
      parameters.getOrElse(
        MetadataSource.DbNameKey,
        throw new SparkException(s"parameter '${MetadataSource.DbNameKey}' must be provided")
      )
    )

    if (!dbPath.toFile.exists()) {
      throw new SparkException(s"database at '$dbPath' does not exist")
    }

    val schema: StructType = Schema(table)

    MetadataRelation(sqlContext.sparkSession, schema, dbPath.toString, tableSource = Some(table))
  }

}

case class MetadataRelation(session: SparkSession,
                            schema: StructType,
                            dbPath: String,
                            joinConditions: Option[Expression] = None,
                            tableSource: Option[String] = None)
  extends BaseRelation with CatalystScan {

  private val localPath: String = UtilsWrapper.getLocalDir(session.sparkContext.getConf)
  private val path: String = session.conf.get(RepositoriesPathKey)
  private val skipCleanup: Boolean = session.conf.
    get(SkipCleanupKey, default = "false").toBoolean

  override def sqlContext: SQLContext = session.sqlContext

  override def unhandledFilters(filters: Array[Filter]): Array[Filter] = {
    super.unhandledFilters(filters)
  }

  /**
    * Returns the number of executors that are active right now.
    *
    * @return number of active executors
    */
  def currentActiveExecutors(): Int = {
    val sc = session.sparkContext
    val driver = sc.getConf.get("spark.driver.host")
    val executors = sc.getExecutorMemoryStatus
      .keys
      .filter(ex => ex.split(":").head != driver)
      .toArray
      .distinct
      .length

    // If there are no executors, it means it's a local job
    // so there's just one node to get the data from.
    if (executors > 0) executors else 1
  }

  override def buildScan(requiredColumns: Seq[Attribute],
                         filters: Seq[Expression]): RDD[Row] = {
    val sc = session.sparkContext
    val (qb, shouldGetBlobs) = getQueryBuilder(requiredColumns, filters)
    val metadataCols = sc.broadcast(qb.fields)
    val sql = sc.broadcast(qb.sql)
    val reposRDD = RepositoryRDDProvider(sc).get(path)

    val metadataRDD = sc.emptyRDD[Unit]
      .repartition(currentActiveExecutors())
      .mapPartitions[Map[String, Any]](_ => {
      val iter = new MetadataIterator(metadataCols.value, dbPath, sql.value)
      new CleanupIterator[Map[String, Any]](iter, iter.close())
    }, preservesPartitioning = true)

    if (shouldGetBlobs) {
      val reposLocalPath = sc.broadcast(localPath)
      val filtersBySource = sc.broadcast(Sources.getFiltersBySource(filters))
      val requiredCols = sc.broadcast(requiredColumns.toArray)

      reposRDD
        .groupBy(_.root)
        .join(metadataRDD.groupBy(r => r("repository_path").toString))
        .flatMap {
          case (p, (repoSources, rows)) =>
            // The relationship between repository sources and paths is 1:1, so it's not
            // possible to have more than a single element in repo sources, to habe more
            // means there's a bug somewhere
            // In theory, this should never happen, but better be safe than sorry.
            if (repoSources.isEmpty || repoSources.tail.nonEmpty) {
              throw new SparkException("this is likely a bug: a repository path should match " +
                s"a single repository source, but '$p' matched more")
            }

            val source = repoSources.head
            val provider = RepositoryProvider(reposLocalPath.value, skipCleanup)
            val repo = provider.get(source)

            val finalCols = requiredCols.value.map(_.name)
            val iter = new BlobIterator(
              finalCols,
              repo,
              new MetadataTreeEntryIterator(finalCols, rows.toIterator),
              filtersBySource.value.getOrElse("blobs", Seq())
            )

            new CleanupIterator[Row](iter, repo.close())
        }
    } else {
      metadataRDD
        .map(row => Row(metadataCols.value.map(c => row(c.name)): _*))
    }
  }

  def getQueryBuilder(requiredColumns: Seq[Attribute],
                      filters: Seq[Expression]): (QueryBuilder, Boolean) = {
    var shouldGetBlobs = false
    val queryBuilder = Sources.getSources(tableSource, schema)
      .foldLeft(QueryBuilder().addFields(requiredColumns)) {
        case (builder, "repositories") =>
          builder.addTable("repositories")
        case (builder, "references") =>
          builder.addTable("references")
            .join("repositories", "references", Seq(
              JoinCondition("repositories", "id", "references", "repository_id")
            ))
        case (builder, "commits") =>
          builder.addTable("commits")
            .join("references", "commits", Seq(
              JoinCondition("references", "name", "commits", "reference_name"),
              JoinCondition("repositories", "id", "commits", "repository_id")
            ))
        case (builder, "tree_entries") =>
          builder.addTable("tree_entries")
            .join("commits", "tree_entries", Seq(
              JoinCondition("references", "name", "tree_entries", "reference_name"),
              JoinCondition("repositories", "id", "tree_entries", "repository_id"),
              JoinCondition("commits", "hash", "tree_entries", "commit_hash")
            ))
        case (QueryBuilder(_, tables, joinConds, f), "blobs") =>
          shouldGetBlobs = true
          // we're getting the blobs, so that means the fields will be those of the blobs table
          // which we cant retrieve because metadata only stores until the tree_entries level.
          // So we need to remove all the columns that are from blobs (IMPORTANT: not the others)
          // and then add all the columns in tree_entries (because they are needed in BlobIterator
          // and repositories.repository_path because it's needed to join the RDDs.
          val nonBlobsColumns = requiredColumns
            .filterNot(_.metadata.getString(Sources.SourceKey) == "blobs")
          val repoPathField = Schema.repositories(Schema.repositories.fieldIndex("repository_path"))
          val fields = nonBlobsColumns ++
            (Schema.treeEntries.map((_, "tree_entries")) ++ Array((repoPathField, "repositories")))
              .map {
                case (sf, table) =>
                  AttributeReference(
                    sf.name,
                    sf.dataType,
                    sf.nullable,
                    new MetadataBuilder().putString(Sources.SourceKey, table).build()
                  )().asInstanceOf[Attribute]
              }

          // because of the previous step we might have introduced some duplicated columns,
          // we need to remove them
          val uniqueFields = fields
            .groupBy(attr => (attr.name, attr.metadata.getString(Sources.SourceKey)))
            .map(_._2.head)
            .toSeq

          QueryBuilder(uniqueFields, tables, joinConds, f)
        case (_, other) =>
          throw new SparkException(s"unable to find source $other")
      }
      .addFilters(filters ++ joinConditions)

    // if there is one step missing the query can't be correctly built
    // as there is no data to join all the steps
    if (!Sources.orderedSources.slice(0, queryBuilder.tables.length)
      .sameElements(queryBuilder.tables)) {
      throw new SparkException(s"unable to build a query with the following " +
        s"tables: ${queryBuilder.tables.mkString(",")}")
    }

    (queryBuilder, shouldGetBlobs)
  }
}

object MetadataSource {
  val DbPathKey: String = "metadatadbpath"
  val DbNameKey: String = "metadatadbname"
  val DefaultDbName: String = "engine_metadata.db"
}


