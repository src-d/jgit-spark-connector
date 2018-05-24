package tech.sourced.engine

import java.nio.file.Paths

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, MetadataBuilder, StructType}
import org.apache.spark.sql.{Row, SQLContext, SparkSession}
import org.apache.spark._
import tech.sourced.engine.provider.{RepositoryProvider, RepositoryRDDProvider}
import org.apache.spark.sql.catalyst.expressions.{
  EqualTo, Expression, Attribute, AttributeReference, Literal
}
import org.apache.spark.sql.sources._
import tech.sourced.engine.iterator._
import tech.sourced.engine.util.{Filter, Filters}

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
      DefaultSource.TableNameKey,
      throw new SparkException(s"parameter '${DefaultSource.TableNameKey}' must be provided")
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
  private val repositoriesFormat: String = session.conf.get(RepositoriesFormatKey)
  private val skipCleanup: Boolean = session.conf.
    get(SkipCleanupKey, default = "false").toBoolean
  private val skipReadErrors: Boolean = session.conf.
    get(SkipReadErrorsKey, default = "false").toBoolean
  private val parallelism: Int = session.sparkContext.defaultParallelism

  override def sqlContext: SQLContext = session.sqlContext

  override def unhandledFilters(filters: Array[Filter]): Array[Filter] = {
    super.unhandledFilters(filters)
  }

  override def buildScan(requiredColumns: Seq[Attribute],
                         filters: Seq[Expression]): RDD[Row] = {
    val sc = session.sparkContext
    val (qb, shouldGetBlobs) = getQueryBuilder(requiredColumns, filters)
    val metadataCols = sc.broadcast(qb.fields)
    val sql = sc.broadcast(qb.sql)
    val reposRDD = RepositoryRDDProvider(sc).get(path, repositoriesFormat)

    val metadataRDD = sc.emptyRDD[Unit]
      .repartition(session.currentActiveExecutors())
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
            val provider = RepositoryProvider(reposLocalPath.value, skipCleanup, parallelism * 2)
            val repo = provider.get(source)

            val finalCols = requiredCols.value.map(_.name)
            val iter = new BlobIterator(
              finalCols,
              repo,
              new MetadataTreeEntryIterator(finalCols, rows.toIterator),
              filtersBySource.value.getOrElse("blobs", Seq()),
              skipReadErrors
            )

            new CleanupIterator[Row](iter, provider.close(source, repo))
        }
    } else {
      metadataRDD
        .map(row => Row(metadataCols.value.map(c => row(c.name)): _*))
    }
  }

  def getQueryBuilder(requiredColumns: Seq[Attribute],
                      filters: Seq[Expression]): (QueryBuilder, Boolean) = {
    val sources = Sources.getSources(tableSource, schema)
    val queryBuilder = sources
      .foldLeft(QueryBuilder().addFields(requiredColumns)) {
        case (builder, "repositories") =>
          builder.addTable(RepositoriesTable)
        case (builder, "references") =>
          builder.addTable(ReferencesTable)
            .join(RepositoriesTable, ReferencesTable, Seq(
              JoinCondition(RepositoriesTable, "id", ReferencesTable, "repository_id")
            ))
        case (builder, "commits") =>
          builder.addTable(RepositoryHasCommitsTable)
            .join(ReferencesTable, RepositoryHasCommitsTable, Seq(
              JoinCondition(ReferencesTable, "name", RepositoryHasCommitsTable, "reference_name"),
              JoinCondition(RepositoriesTable, "id", RepositoryHasCommitsTable, "repository_id")
            ))
            .addTable(CommitsTable)
            .join(RepositoryHasCommitsTable, CommitsTable, Seq(
              JoinCondition(RepositoryHasCommitsTable, "hash", CommitsTable, "hash")
            ))
        case (builder, "tree_entries") =>
          builder.addTable(TreeEntriesTable)
            .join(CommitsTable, TreeEntriesTable, Seq(
              JoinCondition(CommitsTable, "hash", TreeEntriesTable, "commit_hash")
            ))
        case (QueryBuilder(_, tables, joinConds, f), "blobs") =>
          // we're getting the blobs, so that means the fields will be those of the blobs table
          // which we cant retrieve because metadata only stores until the tree_entries level.
          // So we need to remove all the columns that are from blobs (IMPORTANT: not the others)
          // and then add all the columns in tree_entries (because they are needed in BlobIterator
          // and repositories.repository_path because it's needed to join the RDDs.
          val nonBlobsColumns = requiredColumns
            .filterNot(_.metadata.getString(Sources.SourceKey) == "blobs")
          val repoPathField = Schema.repositories(Schema.repositories.fieldIndex("repository_path"))
          val fields = nonBlobsColumns ++
            (Schema.treeEntries.map((_, TreeEntriesTable))
              ++ Array((repoPathField, RepositoriesTable)))
              .map {
                case (sf, table) =>
                  AttributeReference(
                    sf.name, sf.dataType, sf.nullable,
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
      .addFilters({
        val fs = filters ++ joinConditions
        // since we don't use the CommitIterator with the MetadataSource,
        // we need to hack our way into making retrieving just the last commit
        // the default thing returned by the engine
        if (Filters(fs.flatMap(Filter.compile)).hasFilters("index")
          || !sources.contains("commits")) {
          fs
        } else {
          fs ++ Seq(EqualTo(
            AttributeReference("index", IntegerType,
              metadata = new MetadataBuilder().putString(Sources.SourceKey, "commits").build()
            )(), Literal(0)
          ))
        }
      })

    // if there is one step missing the query can't be correctly built
    // as there is no data to join all the steps
    if (!Sources.orderedSources.slice(0, sources.length)
      .sameElements(sources)) {
      throw new SparkException(s"unable to build a query with the following " +
        s"tables: ${queryBuilder.tables.mkString(",")}")
    }

    (queryBuilder, sources.contains(BlobsTable))
  }
}

object MetadataSource {
  val DbPathKey: String = "metadatadbpath"
  val DbNameKey: String = "metadatadbname"
  val DefaultDbName: String = "engine_metadata.db"
}


