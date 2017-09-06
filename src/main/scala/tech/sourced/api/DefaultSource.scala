package tech.sourced.api

import org.apache.spark.SparkException
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Row, SQLContext}
import tech.sourced.api.iterator.{CommitIterator, ReferenceIterator, RepositoryIterator}
import tech.sourced.api.provider.{RepositoryProvider, SivaRDDProvider}

class DefaultSource extends RelationProvider with DataSourceRegister {

  override def shortName() = "git"

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
    val table = parameters.getOrElse(DefaultSource.tableNameKey, throw new SparkException("parameter 'table' must be provided"))
    val path = parameters.getOrElse(DefaultSource.pathKey, throw new SparkException("parameter 'path' must be provided"))
    val localPath = sqlContext.getConf("spark.local.dir", "/tmp")

    GitRelation(sqlContext, table, path, localPath)
  }
}

object DefaultSource {
  val tableNameKey = "table"
  val pathKey = "path"
}

case class GitRelation(sqlContext: SQLContext, tableName: String, path: String, localPath: String) extends BaseRelation with PrunedFilteredScan {

  override def schema: StructType = tableName match {
    case "repositories" => Schema.repositories
    case "references" => Schema.references
    case "commits" => Schema.commits
    case other => throw new SparkException(s"table '$other' not supported")
  }

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    val sc = sqlContext.sparkContext

    val tableB = sc.broadcast(tableName)
    val requiredB = sc.broadcast(requiredColumns)
    val localPathB = sc.broadcast(localPath)
    // TODO broadcast filters

    val sivaRDD = SivaRDDProvider(sc).get(path)

    sivaRDD.flatMap(pds => {
      val repo = RepositoryProvider(localPathB.value).get(pds)

      tableB.value match {
        case "repositories" => new RepositoryIterator(requiredB.value, repo)
        case "references" => new ReferenceIterator(requiredB.value, repo)
        case "commits" => new CommitIterator(requiredB.value, repo)
        case other => throw new SparkException(s"table '$other' not supported")
      }
    })
  }
}
