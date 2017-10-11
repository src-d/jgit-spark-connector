package tech.sourced.api

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkException, UtilsWrapper}
import tech.sourced.api.iterator._
import tech.sourced.api.provider.{RepositoryProvider, SivaRDDProvider}
import tech.sourced.api.util.Filter

/**
  * Default source to provide new git relations.
  */
class DefaultSource extends RelationProvider with DataSourceRegister {

  /** @inheritdoc*/
  override def shortName: String = "git"

  /** @inheritdoc*/
  override def createRelation(sqlContext: SQLContext,
                              parameters: Map[String, String]): BaseRelation = {
    val table = parameters.getOrElse(
      DefaultSource.tableNameKey,
      throw new SparkException("parameter 'table' must be provided")
    )

    val schema: StructType = table match {
      case "repositories" => Schema.repositories
      case "references" => Schema.references
      case "commits" => Schema.commits
      case "files" => Schema.files
      case other => throw new SparkException(s"table '$other' is not supported")
    }

    GitRelation(sqlContext, schema, tableSource = Some(table))
  }

}

/**
  * Just contains some useful constants for the DefaultSource class to use.
  */
object DefaultSource {
  val tableNameKey = "table"
  val pathKey = "path"
}

/**
  * A relation based on git data from rooted repositories in siva files. The data this relation
  * will offer depends on the given `tableSource`, which controls the table that will be accessed.
  * Also, the [[GitOptimizer]] might merge some table sources into one by squashing joins, so the
  * result will be the resultant table chained with the previous one using chained iterators.
  *
  * @param sqlContext     Spark SQL Context
  * @param schema         schema of the relation
  * @param joinConditions join conditions, if any
  * @param tableSource    source table if any
  */
case class GitRelation(sqlContext: SQLContext,
                       schema: StructType,
                       joinConditions: Option[Expression] = None,
                       tableSource: Option[String] = None)
  extends BaseRelation with CatalystScan {

  private val localPath: String = UtilsWrapper.getLocalDir(sqlContext.sparkContext.getConf)
  private val path: String = sqlContext.getConf(repositoriesPathKey)
  private val skipCleanup: Boolean = sqlContext.sparkContext.getConf
    .getBoolean(skipCleanupKey, defaultValue = false)

  override def unhandledFilters(filters: Array[Filter]): Array[Filter] = {
    super.unhandledFilters(filters)
  }

  override def buildScan(requiredColumns: Seq[Attribute], filters: Seq[Expression]): RDD[Row] = {
    val sc = sqlContext.sparkContext
    val sivaRDD = SivaRDDProvider(sc).get(path)

    val requiredCols = sc.broadcast(requiredColumns.map(_.name).toArray)
    val reposLocalPath = sc.broadcast(localPath)
    val sources = sc.broadcast(GitRelation.getSources(tableSource, schema))
    val filtersBySource = sc.broadcast(GitRelation.getFiltersBySource(filters))

    sivaRDD.flatMap(pds => {
      val provider = RepositoryProvider(reposLocalPath.value, skipCleanup)
      val repo = provider.get(pds)

      // since the sources are ordered by their hierarchy, we can chain them like this
      // using the last used iterator as input for the current one
      var iter: Option[RootedRepoIterator[_]] = None
      sources.value.foreach({
        case k@"repositories" =>
          iter = Some(new RepositoryIterator(
            requiredCols.value,
            repo,
            filtersBySource.value.getOrElse(k, Seq())
          ))

        case k@"references" =>
          iter = Some(new ReferenceIterator(
            requiredCols.value,
            repo,
            iter.map(_.asInstanceOf[RepositoryIterator]).orNull,
            filtersBySource.value.getOrElse(k, Seq())
          ))

        case k@"commits" =>
          iter = Some(new CommitIterator(
            requiredCols.value,
            repo,
            iter.map(_.asInstanceOf[ReferenceIterator]).orNull,
            filtersBySource.value.getOrElse(k, Seq())
          ))

        case k@"files" =>
          iter = Some(new BlobIterator(
            requiredCols.value,
            repo,
            iter.map(_.asInstanceOf[CommitIterator]).orNull,
            filtersBySource.value.getOrElse(k, Seq())
          ))

        case other => throw new SparkException(s"required cols for '$other' is not supported")
      })

      new CleanupIterator(iter.getOrElse(Seq().toIterator), provider.close(pds.getPath()))
    })
  }
}

/**
  * Contains some useful methods to be used inside [[GitRelation]].
  */
private object GitRelation {

  /**
    * Returns the list of sources in the schema or the table source if any.
    *
    * @param tableSource optional source table
    * @param schema      resultant schema
    * @return sequence with table sources
    */
  private def getSources(tableSource: Option[String],
                         schema: StructType): Seq[String] =
    tableSource match {
      case Some(ts) => Seq(ts)
      case None =>
        schema
          .map(_.metadata.getString("source"))
          .distinct
          .sortWith(Sources.compare(_, _) < 0)
    }

  /**
    * Returns the filters compiled and grouped by their table source.
    *
    * @param filters list of expression to compile the filters
    * @return compiled and grouped filters
    */
  private def getFiltersBySource(filters: Seq[Expression]) =
    filters.map(Filter.compile)
      .flatMap(_.filters)
      .map(e => (e.sources.distinct, e))
      .filter(_._1.length == 1)
      .groupBy(_._1)
      .map { case (k, v) => (k.head, v.map(_._2)) }
}

/**
  * Defines the hierarchy between data sources.
  */
object Sources {

  /** Sources ordered by their position in the hierarchy. */
  val orderedSources = Array(
    "repositories",
    "references",
    "commits",
    "files"
  )

  /**
    * Compares two sources.
    *
    * @param a first source
    * @param b second source
    * @return comparison result
    */
  def compare(a: String, b: String): Int = orderedSources.indexOf(a)
    .compareTo(orderedSources.indexOf(b))

}
