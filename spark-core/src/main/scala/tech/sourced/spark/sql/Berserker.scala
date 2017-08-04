package tech.sourced.spark.sql

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._
import tech.sourced.spark._
import tech.sourced.spark.sql.udfs._
import org.apache.spark.sql.functions._

object Berserker {

  val conf = new SparkConf(true)
  conf.setMaster("local[4]")
  conf.setAppName("berserker")

  def main(args: Array[String]): Unit = {
    berserkerDF()
  }

  def berserkerRDD(): Unit = {
    val sc = SparkContext.getOrCreate(conf)

    val tmpDir = "/tmp/repositories"
    val urlRDD = sc.parallelize(Seq(
      "git://github.com/src-d/go-git.git",
      "git://github.com/src-d/enry.git"
    ))

    val repositories = GitLoad.clone(tmpDir, urlRDD)
    val fullFilesRDD = loadAllFiles(repositories)
  }

  def berserkerDF(): Unit = {
    val sc = SparkContext.getOrCreate(conf)
    val ss = SparkSession.builder().getOrCreate()
    setupSparkSession(ss)

    val tmpDir = "/tmp/repositories"
    val fullFilesDir = "/tmp/parquet/fullfiles"
    val fullFilesTable = "full_files"

    val urlRDD = sc.parallelize(Seq(
      "git://github.com/src-d/go-git.git",
      "git://github.com/src-d/enry.git"))

    val repositories = GitLoad.clone(tmpDir, urlRDD)
    val fullFilesDF = createDataFrame(ss, repositories)
    fullFilesDF.createOrReplaceTempView(fullFilesTable)

    var fullFilesInHeadDF = fullFilesDF.where(col("reference.name").equalTo("HEAD"))
    fullFilesInHeadDF.write.parquet(fullFilesDir)

    fullFilesInHeadDF = ss.read.parquet(fullFilesDir)
    fullFilesInHeadDF.createOrReplaceTempView(fullFilesTable)

    fullFilesInHeadDF.printSchema()

    val fullFilesParsedDF = fullFilesInHeadDF
        .withColumn("language", detectLanguage(col("file.path"), col("blob.content")))
        .withColumn("uast", uast(col("language"), col("blob.content")))

    for (row <- fullFilesParsedDF.select("*").limit(5).collect()) {
      println(s"row: $row")
    }
  }
}
