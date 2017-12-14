package tech.sourced.engine.iterator

import java.nio.file.Paths
import java.util.{Properties, UUID}

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.types.{Metadata, StringType, StructType}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import tech.sourced.engine.{BaseSparkSpec, Schema}

class JDBCQueryIteratorSpec
  extends FlatSpec with Matchers with BeforeAndAfterAll with BaseSparkSpec {
  private val tmpPath = Paths.get(
    System.getProperty("java.io.tmpdir"),
    UUID.randomUUID.toString
  )

  private val dbPath = tmpPath.resolve("test.db")

  override def beforeAll(): Unit = {
    super.beforeAll()
    tmpPath.toFile.mkdir()
    val rdd = ss.sparkContext.parallelize(Seq(
      Row("id1"),
      Row("id2"),
      Row("id3")
    ))

    val properties = new Properties()
    properties.put("driver", "org.sqlite.JDBC")
    val df = ss.createDataFrame(rdd, StructType(Seq(Schema.repositories.head)))
    df.write.jdbc(s"jdbc:sqlite:${dbPath.toString}", "repositories", properties)
  }

  override def afterAll(): Unit = {
    super.afterAll()
    FileUtils.deleteQuietly(tmpPath.toFile)
  }

  "JDBCQueryIterator" should "return all rows for the query" in {
    val iter = new JDBCQueryIterator(
      Seq(attr("id")),
      dbPath.toString,
      "SELECT id FROM repositories ORDER BY id"
    )

    // calling hasNext more than one time does not cause rows to be lost
    iter.hasNext
    iter.hasNext
    val rows = (for (row <- iter) yield row).toArray
    rows.length should be(3)
    rows(0).length should be(1)
    rows(0)(0).toString should be("id1")
    rows(1)(0).toString should be("id2")
    rows(2)(0).toString should be("id3")
  }

  private def attr(name: String): Attribute = AttributeReference(
    name, StringType, nullable = false, Metadata.empty
  )()
}
