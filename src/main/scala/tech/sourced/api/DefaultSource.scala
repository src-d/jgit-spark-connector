package tech.sourced.api

import org.apache.spark.SparkException
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Row, SQLContext}
import tech.sourced.api.iterator._
import tech.sourced.api.provider.{RepositoryProvider, SivaRDDProvider}

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

    GitRelation(sqlContext, schema, tableSource = Some(table))
  }
}

object DefaultSource {
  val tableNameKey = "table"
  val pathKey = "path"
}

case class GitRelation(
                        sqlContext: SQLContext,
                        schema: StructType,
                        joinConditions: Option[Expression] = None,
                        tableSource: Option[String] = None)
  extends BaseRelation with CatalystScan {

  private val localPath: String = sqlContext.getConf(localPathKey, "/tmp")
  private val path: String = sqlContext.getConf(repositoriesPathKey)
  private val skipCleanup: Boolean = sqlContext.sparkContext.getConf.getBoolean(skipCleanupKey, false)

  // TODO implement this correctly to avoid a lot of required columns push up to spark
  override def unhandledFilters(filters: Array[Filter]): Array[Filter] = {
    super.unhandledFilters(filters)
  }

  override def buildScan(requiredColumns: Seq[Attribute], filters: Seq[Expression]): RDD[Row] = {
    // TODO remove prints
    println("JOIN CONDITIONS: ", joinConditions)
    println("SCHEMA: ", schema)
    println("SCHEMA METADATA: ", schema.map(_.metadata))
    println("FILTERS:", filters)
    println("REQUIRED COLS:", requiredColumns)
    println("REQUIRED COLS METADATA:", requiredColumns.map(_.metadata))
    println("PATH AND LOCAL PATH:", path, localPath)

    // TODO process join conditions
    // TODO process filters

    // get required columns per data source
    val rc: Map[String, Array[String]] = tableSource match {
      case Some(ts) => Map(ts -> requiredColumns.map(_.name).toArray)
      case None =>
        requiredColumns
          .map(a => a.metadata.getString("source") -> a.name)
          .groupBy(_._1).map { case (k, v) => (k, v.map(_._2).toArray) }
    }

    val sc = sqlContext.sparkContext
    val sivaRDD = SivaRDDProvider(sc).get(path)

    val requiredColsB = sc.broadcast(rc)
    val localPathB = sc.broadcast(localPath)

    sivaRDD.flatMap(pds => {
      val provider = RepositoryProvider(localPathB.value, skipCleanup)
      val repo = provider.get(pds)

      // TODO send filters to each iterator
      val iterators = requiredColsB.value.map {
        case ("repositories", c) => new RepositoryIterator(c, repo)
        case ("references", c) => new ReferenceIterator(c, repo)
        case ("commits", c) => new CommitIterator(c, repo)
        case ("files", c) => new BlobIterator(c, repo, Array())
        case other => throw new SparkException(s"required cols for '$other' is not supported")
      }

      if (iterators.size == 1) {
        new CleanupIterator(iterators.head, provider.close(pds.getPath()))
      } else {
        // TODO process each iterator in order to send data to the next one
        new CleanupIterator(iterators.head, provider.close(pds.getPath()))
      }
    })
  }
}
