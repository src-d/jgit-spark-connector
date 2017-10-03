package tech.sourced.api

import org.apache.spark.SparkException
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Row, SQLContext}

class DefaultSource extends RelationProvider with DataSourceRegister {

  override def shortName = "git"

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
    val table = parameters.getOrElse(DefaultSource.tableNameKey, throw new SparkException("parameter 'table' must be provided"))

    val schema: StructType = table match {
      case "repositories" => Schema.repositories
      case "references" => Schema.references
      case "commits" => Schema.commits
      case "files" => Schema.files
      case other => throw new SparkException(s"table '$other' is not supported")
    }

    GitRelation(sqlContext, schema)
  }
}

object DefaultSource {
  val tableNameKey = "table"
  val pathKey = "path"
}

case class GitRelation(sqlContext: SQLContext, schema: StructType, joinConditions: Option[Expression] = None) extends BaseRelation with CatalystScan {
  private val localPath: String = sqlContext.getConf(localPathKey, "/tmp")
  private val path: String = sqlContext.getConf(repositoriesPathKey)

  // TODO implement this correctly to avoid a lot of required columns push up to spark
  override def unhandledFilters(filters: Array[Filter]): Array[Filter] = {
    super.unhandledFilters(filters)
  }

  override def buildScan(requiredColumns: Seq[Attribute], filters: Seq[Expression]): RDD[Row] = {
    println("JOIN CONDITIONS: ", joinConditions)
    println("SCHEMA: ", schema)
    println("FILTERS:", filters)
    println("REQUIRED COLS:", requiredColumns)
    println("PATH AND LOCAL PATH:", path, localPath)

    // TODO add logic to concatenate iterators depending of join conditions
    sqlContext.emptyDataFrame.rdd
  }
}

//case class OldGitRelation(sqlContext: SQLContext, tableName: String, path: String, localPath: String) extends BaseRelation with PrunedFilteredScan {
//
//  override def schema: StructType = tableName match {
//    case "repositories" => Schema.repositories
//    case "references" => Schema.references
//    case "commits" => Schema.commits
//    case "files" => Schema.files
//    case other => throw new SparkException(s"table '$other' is not supported")
//  }
//
//  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
//    val sc = sqlContext.sparkContext
//
//    val tableB = sc.broadcast(tableName)
//    val requiredB = sc.broadcast(requiredColumns)
//    val localPathB = sc.broadcast(localPath)
//    // TODO broadcast filters
//
//    val sivaRDD = SivaRDDProvider(sc).get(path)
//    val skipCleanup = sqlContext.sparkContext.getConf.getBoolean(skipCleanupKey, false)
//
//    sivaRDD.flatMap(pds => {
//      val provider = RepositoryProvider(localPathB.value, skipCleanup)
//      val repo = provider.get(pds)
//
//      val iter = tableB.value match {
//        case "repositories" => new RepositoryIterator(requiredB.value, repo)
//        case "references" => new ReferenceIterator(requiredB.value, repo)
//        case "commits" => new CommitIterator(requiredB.value, repo)
//        case "files" => new BlobIterator(requiredB.value, repo, filters.map(ColumnFilter.compileFilter))
//        case other => throw new SparkException(s"table '$other' is not supported")
//      }
//
//      new CleanupIterator(iter, provider.close(pds.getPath()))
//    })
//  }
//}