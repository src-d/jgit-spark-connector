package tech.sourced.api

import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, Suite}

trait BaseSparkSpec extends BeforeAndAfterAll {
  this: Suite =>

  var ss: SparkSession = _

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    ss = SparkSession.builder().appName("test").master("local[*]").getOrCreate()
  }

  override protected def afterAll(): Unit = {
    super.afterAll()
    ss = null
  }
}
