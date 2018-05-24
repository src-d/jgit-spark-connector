package tech.sourced.engine

import org.apache.spark.groupon.metrics.UserMetricsSystem
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Row, SQLContext, SparkSession}
import org.apache.spark.{SparkException, UtilsWrapper}
import tech.sourced.engine.iterator._
import tech.sourced.engine.provider.{RepositoryProvider, RepositoryRDDProvider}

/**
  * Default source to provide new git relations.
  */
class DefaultSource extends RelationProvider with DataSourceRegister {

  /** @inheritdoc */
  override def shortName: String = "git"

  /** @inheritdoc */
  override def createRelation(sqlContext: SQLContext,
                              parameters: Map[String, String]): BaseRelation = {
    val table = parameters.getOrElse(
      DefaultSource.TableNameKey,
      throw new SparkException("parameter 'table' must be provided")
    )

    val schema: StructType = Schema(table)

    GitRelation(sqlContext.sparkSession, schema, tableSource = Some(table))
  }

}

/**
  * Just contains some useful constants for the DefaultSource class to use.
  */
object DefaultSource {
  val TableNameKey = "table"
  val PathKey = "path"
}

/**
  * A relation based on git data from rooted repositories in siva files. The data this relation
  * will offer depends on the given `tableSource`, which controls the table that will be accessed.
  * Also, the [[tech.sourced.engine.rule.GitOptimizer]] might merge some table sources into one by
  * squashing joins, so the result will be the resultant table chained with the previous one using
  * chained iterators.
  *
  * @param session        Spark session
  * @param schema         schema of the relation
  * @param joinConditions join conditions, if any
  * @param tableSource    source table if any
  */
case class GitRelation(session: SparkSession,
                       schema: StructType,
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

  // this needs to be overridden to extend BaseRelataion,
  // though is not very useful since already we have the SparkSession
  override def sqlContext: SQLContext = session.sqlContext

  override def unhandledFilters(filters: Array[Filter]): Array[Filter] = {
    super.unhandledFilters(filters)
  }

  override def buildScan(requiredColumns: Seq[Attribute],
                         filters: Seq[Expression]): RDD[Row] = {
    val sc = session.sparkContext
    val reposRDD = RepositoryRDDProvider(sc).get(path, repositoriesFormat)

    val requiredCols = sc.broadcast(requiredColumns.map(_.name).toArray)
    val reposLocalPath = sc.broadcast(localPath)
    val sources = sc.broadcast(Sources.getSources(tableSource, schema))
    val filtersBySource = sc.broadcast(Sources.getFiltersBySource(filters))

    reposRDD.flatMap(source => {
      val provider = RepositoryProvider(reposLocalPath.value, skipCleanup, parallelism * 2)

      val repo = UserMetricsSystem.timer("RepositoryProvider").time({
        provider.get(source)
      })

      // since the sources are ordered by their hierarchy, we can chain them like this
      // using the last used iterator as input for the current one
      var iter: Option[ChainableIterator[_]] = None
      sources.value.foreach({
        case k@"repositories" =>
          iter = Some(new RepositoryIterator(
            source.root,
            requiredCols.value,
            repo,
            filtersBySource.value.getOrElse(k, Seq()),
            skipReadErrors
          ))

        case k@"references" =>
          iter = Some(new ReferenceIterator(
            requiredCols.value,
            repo,
            iter.map(_.asInstanceOf[RepositoryIterator]).orNull,
            filtersBySource.value.getOrElse(k, Seq()),
            skipReadErrors
          ))

        case k@"commits" =>
          iter = Some(new CommitIterator(
            requiredCols.value,
            repo,
            iter.map(_.asInstanceOf[ReferenceIterator]).orNull,
            filtersBySource.value.getOrElse(k, Seq()),
            skipReadErrors
          ))

        case k@"tree_entries" =>
          iter = Some(new GitTreeEntryIterator(
            requiredCols.value,
            repo,
            iter.map(_.asInstanceOf[CommitIterator]).orNull,
            filtersBySource.value.getOrElse(k, Seq()),
            skipReadErrors
          ))

        case k@"blobs" =>
          iter = Some(new BlobIterator(
            requiredCols.value,
            repo,
            iter.map(_.asInstanceOf[GitTreeEntryIterator]).orNull,
            filtersBySource.value.getOrElse(k, Seq()),
            skipReadErrors
          ))

        case other => throw new SparkException(s"required cols for '$other' is not supported")
      })

      // FIXME: when the RDD is persisted to disk the last element of this iterator is closed twice
      new CleanupIterator(iter.getOrElse(Seq().toIterator), provider.close(source, repo))
    })
  }
}
