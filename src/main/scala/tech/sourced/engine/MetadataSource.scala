package tech.sourced.engine

import java.nio.file.Paths
import java.sql.{Date, Timestamp}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{Metadata, MetadataBuilder, StructType}
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

    val dbPath = Paths.get(parameters.getOrElse(
      MetadataSource.dbPathKey,
      throw new SparkException(s"parameter '${MetadataSource.dbPathKey}' must be provided")
    )).resolve(MetadataSource.dbName)

    if (!dbPath.toFile.exists()) {
      throw new SparkException(s"database at '$dbPath' does not exist")
    }

    val schema: StructType = table match {
      case "repositories" => Schema.repositories
      case "references" => Schema.references
      case "commits" => Schema.commits
      case "tree_entries" => Schema.treeEntries
      case "blobs" => Schema.blobs
      case other => throw new SparkException(s"table '$other' is not supported")
    }

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
  private val path: String = session.conf.get(repositoriesPathKey)
  private val skipCleanup: Boolean = session.conf.
    get(skipCleanupKey, default = "false").toBoolean

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

            val rowsIter = new MetadataRowsIterator(requiredCols.value, rows.toIterator)
            val iter = new BlobIterator(
              requiredCols.value.map(_.name),
              repo,
              Right(rowsIter),
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
            .filterNot(_.metadata.getString(Sources.sourceKey) == "blobs")
          val repoPathField = Schema.repositories(Schema.repositories.fieldIndex("repository_path"))
          val fields = nonBlobsColumns ++
            (Schema.treeEntries.map((_, "tree_entries")) ++ Array((repoPathField, "repositories")))
              .map {
                case (sf, table) =>
                  AttributeReference(
                    sf.name,
                    sf.dataType,
                    sf.nullable,
                    new MetadataBuilder().putString(Sources.sourceKey, table).build()
                  )().asInstanceOf[Attribute]
              }

          // because of the previous step we might have introduced some duplicated columns,
          // we need to remove them
          val uniqueFields = fields
            .groupBy(attr => (attr.name, attr.metadata.getString(Sources.sourceKey)))
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
  val dbPathKey: String = "dbpath"
  val dbName: String = "engine_metadata.db"
}

case class Join(left: String, right: String, conditions: Seq[JoinCondition])

case class JoinCondition(leftTable: String, leftCol: String, rightTable: String, rightCol: String)

/**
  * Immutable select query builder.
  *
  * @param fields  fields to select
  * @param tables  tables to select data from
  * @param joins   joins with other tables, if any
  * @param filters filters to apply
  */
case class QueryBuilder(fields: Seq[Attribute] = Array[Attribute](),
                        tables: Seq[String] = Array[String](),
                        joins: Seq[Join] = Array[Join](),
                        filters: Seq[Expression] = Array[Expression]()) {

  import QueryBuilder._

  /**
    * Creates a new QueryBuilder with the given fields added to the current ones.
    *
    * @param fields new fields to add
    * @return a new QueryBuilder with the given fields added to the current fields
    */
  def addFields(fields: Seq[Attribute]): QueryBuilder =
    QueryBuilder(this.fields ++ fields, tables, joins, filters)

  /**
    * Creates a new QueryBuilder with the given table added to the current ones.
    *
    * @param table new table to add
    * @return a new QueryBuilder with the given table added to the current fields
    */
  def addTable(table: String): QueryBuilder =
    QueryBuilder(fields, tables ++ Seq(table), joins, filters)

  def join(left: String, right: String, conditions: Seq[JoinCondition]): QueryBuilder =
    QueryBuilder(
      fields,
      tables,
      joins ++ Some(Join(left, right, conditions)),
      filters
    )

  /**
    * Creates a new QueryBuilder with the given filter added to the current ones.
    *
    * @param filters filters to add
    * @return new QueryBuilder with the filter added to the current ones
    */
  def addFilters(filters: Seq[Expression]): QueryBuilder =
    QueryBuilder(fields, tables, joins, this.filters ++ filters)

  private def selectedFields: String =
    if (fields.nonEmpty) {
      fields.map(qualify).mkString(", ")
    } else {
      "null"
    }

  private def whereClause: String = {
    val compiledFilters = filters.flatMap(compileFilter)
    if (compiledFilters.isEmpty) {
      ""
    } else {
      s"WHERE ${compiledFilters.mkString(" AND ")}"
    }
  }

  private def getOnClause(cond: JoinCondition): String =
    s"${qualify(cond.leftTable, cond.leftCol)} = ${qualify(cond.rightTable, cond.rightCol)}"

  private def selectedTables: String =
    if (joins.isEmpty && tables.length == 1) {
      prefixTable(tables.head)
    } else if (joins.nonEmpty) {
      joins.zipWithIndex.map {
        case (Join(left, right, conditions), 0) =>
          s"${prefixTable(left)} INNER JOIN ${prefixTable(right)} " +
            s"ON (${conditions.map(getOnClause).mkString(" AND ")})"
        case (Join(_, right, conditions), _) =>
          s" INNER JOIN ${prefixTable(right)} " +
            s"ON (${conditions.map(getOnClause).mkString(" AND ")})"
      }.mkString(" ")
    } else {
      // should not happen
      throw new SparkException("this is likely a bug: no join conditions found, " +
        s"but multiple tables: '${tables.mkString(", ")}'")
    }

  /**
    * Returns the built select SQL query.
    *
    * @return select SQL query
    */
  def sql: String =
    s"SELECT $selectedFields FROM $selectedTables $whereClause"

}

object QueryBuilder {

  private def qualify(col: Attribute): String = {
    val table = col.metadata.getString(Sources.sourceKey)
    s"${prefixTable(table)}.`${col.name}`"
  }

  private def qualify(table: String, col: String): String =
    s"${prefixTable(table)}.`$col`"

  private def escapeSql(value: String): String =
    if (value == null) null else StringUtils.replace(value, "'", "''")

  private def compileValue(value: Any): Any = value match {
    case v: UTF8String => compileValue(v.toString)
    case v: String => s"'${escapeSql(v)}'"
    case v: Timestamp => "'" + v + "'"
    case v: Date => "'" + v + "'"
    case v: Seq[Any] => v.map(compileValue).mkString(", ")
    case v: Boolean => if (v) 1 else 0
    case _ => value
  }

  /**
    * Compiles a filter expression into a SQL string if the filter can be handled.
    * Slightly modified from the JDBCRDD source code of spark.
    *
    * @param filter filter to compile
    * @return compiled filter
    */
  private def compileFilter(filter: Expression): Option[String] = {
    // org.apache.spark.sql.sources._ is imported at the top level and
    // names collision with the expressions, so we import them here.
    import org.apache.spark.sql.catalyst.expressions._
    Option(filter match {
      case EqualTo(attr: AttributeReference, Literal(value, _)) =>
        s"${qualify(attr)} = ${compileValue(value)}"
      case EqualNullSafe(attr: AttributeReference, Literal(value, _)) =>
        val col = qualify(attr)
        s"(NOT ($col != ${compileValue(value)} OR $col IS NULL OR " +
          s"${compileValue(value)} IS NULL) OR " +
          s"($col IS NULL AND ${compileValue(value)} IS NULL))"
      case LessThan(attr: AttributeReference, Literal(value, _))
      => s"${qualify(attr)} < ${compileValue(value)}"
      case GreaterThan(attr: AttributeReference, value) =>
        s"${qualify(attr)} > ${compileValue(value)}"
      case LessThanOrEqual(attr: AttributeReference, Literal(value, _)) =>
        s"${qualify(attr)} <= ${compileValue(value)}"
      case GreaterThanOrEqual(attr: AttributeReference, Literal(value, _)) =>
        s"${qualify(attr)} >= ${compileValue(value)}"
      case IsNull(attr: AttributeReference) => s"${qualify(attr)} IS NULL"
      case IsNotNull(attr: AttributeReference) => s"${qualify(attr)} IS NOT NULL"
      case In(attr: AttributeReference, values) if values.isEmpty =>
        s"CASE WHEN ${qualify(attr)} IS NULL THEN NULL ELSE FALSE END"
      case In(attr: AttributeReference, values: Seq[Expression]) =>
        val literals = values.flatMap {
          case Literal(value, _) => Some(value)
          case _ => None
        }

        if (literals.length != values.length) {
          null
        } else {
          s"${qualify(attr)} IN (${compileValue(literals)})"
        }
      case Not(f) => compileFilter(f).map(p => s"(NOT ($p))").orNull
      case Or(f1, f2) =>
        // We can't compile Or filter unless both sub-filters are compiled successfully.
        // It applies too for the following And filter.
        // If we can make sure compileFilter supports all filters, we can remove this check.
        val or = Seq(f1, f2).flatMap(compileFilter)
        if (or.size == 2) {
          or.map(p => s"($p)").mkString(" OR ")
        } else {
          null
        }
      case And(f1, f2) =>
        val and = Seq(f1, f2).flatMap(compileFilter)
        if (and.size == 2) {
          and.map(p => s"($p)").mkString(" AND ")
        } else {
          null
        }
      case _ => null
    })
  }

  private def prefixTable(table: String): String = s"engine_$table"

}
