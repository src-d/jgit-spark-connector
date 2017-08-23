package tech.sourced.api

import org.apache.spark.sql.SparkSession
import org.scalatest._

class DefaultSourceSpec extends FlatSpec with Matchers {

  "Default source" should "load correctly" in {
    val spark = SparkSession.builder().appName("test").master("local[2]").getOrCreate()
    spark.conf.set("spark.sql.streaming.checkpointLocation", "CL")

    val reposDf = spark.read.format("tech.sourced.api")
      .option("table", "repositories")
      .load("/path/to/repositories/folder")

    reposDf.filter("is_fork=true or is_fork is null").show()

    reposDf.filter("array_contains(urls, 'urlA')").show()

    val referencesDf = spark.read.format("tech.sourced.api")
      .option("table", "references")
      .load("/path/to/repositories/folder")

    referencesDf.filter("repository_uuid = 'ID1'").show()
  }
}
