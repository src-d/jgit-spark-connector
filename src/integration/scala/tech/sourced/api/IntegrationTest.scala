package tech.sourced.api

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpec}

class IntegrationTest extends WordSpec with Matchers with BeforeAndAfterAll {

  type DataFrameCheck = (DataFrame) => Unit

  var ss: SparkSession = _
  val resourcesPath = getClass.getResource("/siva-files").toString

  val tests = Map[String, (Int, DataFrameCheck)](
    "SELECT * FROM repositories WHERE is_fork = false" -> (4, noop _),
    "SELECT * FROM _references WHERE name='refs/heads/master'" -> (5, noop _),
    "SELECT * FROM commits WHERE hash='fff7062de8474d10a67d417ccea87ba6f58ca81d'" -> (43, noop _),
    "SELECT * FROM files WHERE path='README.md'" -> (2039, noop _),
    """SELECT r.*, r2.*
      | FROM repositories r
      | JOIN _references r2
      |   ON r.id = r2.repository_id
      | WHERE r2.name = 'refs/heads/master'
    """.stripMargin -> (5, noop _)
  )

  def noop(df: DataFrame): Unit = {}

  "SQL queries" should {
    tests.foreach(test => {
      val (sql, (count, checker)) = test
      sql in {
        testQuery(sql, count, checker)
      }
    })
  }

  def testQuery(sql: String, count: Int, checker: DataFrameCheck = noop): Unit = {
    val df = ss.sql(sql)
    df.show
    assert(df.count == count)
    checker(df)
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
