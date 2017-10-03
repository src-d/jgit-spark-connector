package tech.sourced.api

import org.apache.spark.{InterruptibleIterator, SparkException, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Row, SQLContext}
import tech.sourced.api.iterator._
import tech.sourced.api.provider.{RepositoryProvider, SivaRDDProvider}
import tech.sourced.api.util.ColumnFilter

/**
  * Default source to provide new git relations.
  */
class DefaultSource extends RelationProvider with DataSourceRegister {

  /**
    * @inheritdoc
    */
  override def shortName(): String = "git"

  /**
    * @inheritdoc
    */
  override def createRelation(sqlContext: SQLContext,
                              parameters: Map[String, String]): BaseRelation = {
    val table = parameters.getOrElse(
      DefaultSource.tableNameKey,
      throw new SparkException("parameter 'table' must be provided")
    )

    val path = parameters.getOrElse(
      DefaultSource.pathKey,
      throw new SparkException("parameter 'path' must be provided")
    )
    val localPath = sqlContext.getConf("spark.local.dir", "/tmp")

    GitRelation(sqlContext, table, path, localPath)
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
  * will offer depends on the given `tableName`, which controls the table that will be accessed.
  *
  * @param sqlContext Spark SQL Context
  * @param tableName  name of the table that will be accessed
  * @param path       path where the repositories in .siva format are stored
  * @param localPath  Spark local directory
  */
case class GitRelation(sqlContext: SQLContext,
                       tableName: String,
                       path: String,
                       localPath: String) extends BaseRelation with PrunedFilteredScan {

  /**
    * Contains the schema for the table given to this GitRelation.
    *
    * @return the schema of the given table
    */
  override def schema: StructType = tableName match {
    case "repositories" => Schema.repositories
    case "references" => Schema.references
    case "commits" => Schema.commits
    case "files" => Schema.files
    case other => throw new SparkException(s"table '$other' is not supported")
  }

  /**
    * @inheritdoc
    */
  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    val sc = sqlContext.sparkContext

    val tableB = sc.broadcast(tableName)
    val requiredB = sc.broadcast(requiredColumns)
    val localPathB = sc.broadcast(localPath)
    // TODO broadcast filters

    val sivaRDD = SivaRDDProvider(sc).get(path)
    val skipCleanup = sqlContext.sparkContext.getConf.getBoolean(skipCleanupKey, false)

    sivaRDD.flatMap(pds => {
      val provider = RepositoryProvider(localPathB.value, skipCleanup)
      val repo = provider.get(pds)

      val iter = tableB.value match {
        case "repositories" => new RepositoryIterator(requiredB.value, repo)
        case "references" => new ReferenceIterator(requiredB.value, repo)
        case "commits" => new CommitIterator(requiredB.value, repo)
        case "files" => new BlobIterator(
          requiredB.value,
          repo,
          filters.map(ColumnFilter.compileFilter)
        )
        case other => throw new SparkException(s"table '$other' is not supported")
      }

      new CleanupIterator(iter, provider.close(pds.getPath()))
    })
  }
}
