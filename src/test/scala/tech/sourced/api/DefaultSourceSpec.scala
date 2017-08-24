package tech.sourced.api

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StringType
import org.scalatest._

class DefaultSourceSpec extends FlatSpec with Matchers {

  "Default source" should "load correctly" in {

    val spark = SparkSession.builder().appName("test").master("local[2]").getOrCreate()

    val reposDf = spark.read.format("tech.sourced.api")
      .option("table", "repositories")
      .load("/path/to/repositories/folder")

    reposDf.filter("is_fork=true or is_fork is null").show()

    reposDf.filter("array_contains(urls, 'urlA')").show()

    val referencesDf = spark.read.format("tech.sourced.api")
      .option("table", "references")
      .load("/path/to/repositories/folder")

    referencesDf.filter("repository_uuid = 'ID1'").show()

    val commitsDf = spark.read.format("tech.sourced.api")
      .option("table", "commits")
      .load("/path/to/repositories/folder")

    commitsDf.show()

    val filesDf = spark.read.format("tech.sourced.api")
      .option("table", "files")
      .load("/path/to/repositories/folder")

    filesDf.withColumn("content string", filesDf("content").cast(StringType)).show()

  }

  "Adittional methods" should "work correctly" in {
    val spark = SparkSession.builder().appName("test").master("local[2]").getOrCreate()

    import Implicits._
    import spark.implicits._

    val reposDf = spark.getRepositories().filter($"id" === "urlA" || $"id" === "urlB")
    val refsDf = reposDf.getReferences().filter($"name".equalTo("HEAD"))
    val commitsDf = refsDf.getCommits()

    commitsDf.show()
  }
}