package tech.sourced.api

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SCWrapper, SparkContext, SparkException}

class DefaultSource extends RelationProvider with DataSourceRegister {

  override def shortName() = "git"

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
    val table = parameters.getOrElse(DefaultSource.tableNameKey, throw new SparkException("parameter 'table' must be provided"))
    val baseFolder = parameters.getOrElse(DefaultSource.pathKey, throw new SparkException("parameter 'path' must be provided"))

    val localPath = "/tmp/processing-repositories"
    val sc = sqlContext.sparkContext

    //val folders = this.getRepositoryFolders(sc, baseFolder)
    val folders =
      "file:/home/antonio/work/src/github.com/src-d/enry" ::
        "file:/home/antonio/work/src/github.com/src-d/fetcher" ::
        "file:/home/antonio/work/src/github.com/src-d/proteus" ::
        Nil
sqlContext.sparkContext.binaryFiles("/path")
    val broadcastedHadoopConf = sqlContext.sparkContext.broadcast(new SCWrapper(sc.hadoopConfiguration))

    table match {
      case "repositories" =>
        MetadataRelation(sqlContext, Schema.repositories, new RepositoryRDD(sc, broadcastedHadoopConf, folders, localPath))
      case "references" =>
        MetadataRelation(sqlContext, Schema.references, new ReferenceRDD(sc, broadcastedHadoopConf, folders, localPath))
      case "commits" =>
        MetadataRelation(sqlContext, Schema.commits, new CommitRDD(sc, broadcastedHadoopConf, folders, localPath))
      case "files" =>
        MetadataRelation(sqlContext, Schema.files, new BlobRDD(sc, broadcastedHadoopConf, folders, localPath))
      case other => throw new SparkException(s"table '$other' not supported")
    }
  }

  private def getRepositoryFolders(sc: SparkContext, path: String): Seq[String] = {
    val folderIterator = FileSystem.get(sc.hadoopConfiguration).listFiles(new Path(path), false)

    Stream.continually(folderIterator)
      .takeWhile(_.hasNext)
      .map(_.next())
      .filter(_.isDirectory)
      .map(_.getPath.toString)
  }
}

object DefaultSource {
  val tableNameKey = "table"
  val pathKey = "path"
}

case class MetadataRelation(sqlContext: SQLContext, schema: StructType, rdd: RDD[Row]) extends BaseRelation with TableScan {

  override def buildScan() = rdd
}
