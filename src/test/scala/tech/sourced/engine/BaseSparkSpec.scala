package tech.sourced.engine

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, Suite}
import tech.sourced.engine.provider.RepositoryProvider

trait BaseSparkSpec extends BeforeAndAfterAll {
  this: Suite =>

  var ss: SparkSession = _

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    ss = SparkSession.builder()
      .appName("test").master("local[*]")
      .config("spark.driver.host", "localhost")
      .getOrCreate()
    ss.registerUDFs()

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger(classOf[RepositoryProvider]).setLevel(Level.OFF)
  }

  override protected def afterAll(): Unit = {
    super.afterAll()
    ss = null
  }
}
