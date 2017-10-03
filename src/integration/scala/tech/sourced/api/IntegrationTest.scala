package tech.sourced.api

import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpec}

import scala.io.Source

class IntegrationTest extends WordSpec with Matchers with BeforeAndAfterAll {

  var ss: SparkSession = _
  val resourcesPath = getClass.getResource("/siva-files").toString

  "SQL queries" should {
    Source.fromInputStream(getClass.getResourceAsStream("/queries.sql"))
      .getLines
      .foreach(sql => s"query: $sql" in {
        val df = ss.sql(sql)
        println(sql)
        df.show
        assert(df.count > 0)
      })
  }

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    ss = SparkSession.builder()
      .appName("test").master("local[*]")
      .config("spark.driver.host", "localhost")
      .getOrCreate()

    // just so all the things are registered correctly
    val _ = SparkAPI(ss, resourcesPath)

    getDataSource("repositories", ss).createOrReplaceTempView("repositories")
    getDataSource("references", ss).createOrReplaceTempView("_references")
    getDataSource("commits", ss).createOrReplaceTempView("commits")
    getDataSource("files", ss).createOrReplaceTempView("files")
  }

  override protected def afterAll(): Unit = {
    super.afterAll()
    ss = null
  }

}
