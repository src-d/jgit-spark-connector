package tech.sourced.api

import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

import scala.collection.JavaConverters._

object Schema {
  val repositories = StructType(
    StructField("id", StringType) ::
      StructField("urls", ArrayType(StringType, containsNull = false)) ::
      StructField("is_fork", BooleanType) ::
      Nil
  )

  val references = StructType(
    StructField("repository_id", StringType) ::
      StructField("name", StringType) ::
      StructField("hash", StringType) ::
      Nil
  )

  val commits = StructType(
    StructField("repository_id", StringType) ::
      StructField("reference_name", StringType) ::
      StructField("index", LongType) ::
      StructField("hash", StringType) ::
      StructField("message", StringType) ::
      StructField("parents", ArrayType(StringType, containsNull = false)) ::
      StructField("tree", MapType(StringType, StringType, valueContainsNull = false)) ::
      StructField("blobs", ArrayType(StringType, containsNull = false)) ::
      StructField("time", TimestampType) ::
      StructField("parents_count", IntegerType) ::

      StructField("author_email", StringType) ::
      StructField("author_name", StringType) ::
      StructField("author_date", TimestampType) ::

      StructField("commiter_email", StringType) ::
      StructField("commiter_name", StringType) ::
      StructField("commiter_date", TimestampType) ::

      Nil
  )

  val files = StructType(
    StructField("file_hash", StringType) ::
      StructField("content", BinaryType) ::
      StructField("is_binary", BooleanType, nullable = false) ::
      StructField("path", StringType) ::
      StructField("lang", StringType) ::
      StructField("uast", BinaryType) ::

      Nil
  )
}

object MockedData {
  val repositories = List(
    Row("urlA", Array("urlA", "urlB", "urlC"), null),
    Row("urlA1", Array("urlA1", "urlB1", "urlC1"), true)
  ).asJava

  val references = List(
    Row("urlA", "HEAD", "RH1"),
    Row("urlA", "refs/master", "RH1"),
    Row("urlA1", "refs/master", "RH2")
  ).asJava

  val commits = List(
    Row(
      "urlA",
      "RH1",
      0L,
      "CH1",
      "commit message",
      Array[String]("CH2"),
      Map("/test.txt" -> "FH1", "/test2.txt" -> "FH2"),
      Array("FH1", "FH2"),
      null,
      1,
      "author@email.com",
      "author name",
      null,
      "commiter@email.com",
      "commiter name",
      null
    )
  ).asJava

  val files = List(
    Row("FH1", "test".getBytes, false, "/test.txt", "TEXT", null),
    Row("FH2", "test2".getBytes, false, "/test2.txt", "TEXT", null)).asJava
}
