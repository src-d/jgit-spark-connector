package tech.sourced.api

import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import scala.collection.JavaConverters._

object Schema {
  val repositories = StructType(
    StructField("uuid", StringType) ::
      StructField("urls", ArrayType(StringType, containsNull = false)) ::
      StructField("is_fork", BooleanType) ::
      Nil
  )

  val references = StructType(
    StructField("repository_uuid", StringType) ::
      StructField("name", StringType) ::
      StructField("hash", StringType) ::
      Nil
  )

  val commits = StructType(
    StructField("hash", StringType) ::
      StructField("message", StringType) ::
      StructField("parents", ArrayType(StringType, containsNull = false)) ::
      StructField("tree", ArrayType(StringType, containsNull = false)) ::
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
    StructField("hash", StringType) ::
      StructField("content", ArrayType(ByteType, containsNull = false)) ::
      StructField("path", StringType) ::
      StructField("lang", StringType) ::
      StructField("uast", ArrayType(ByteType, containsNull = false)) ::

      Nil
  )
}

object MockedData {

  val repositories = List(Row("ID1", Array("urlA", "urlB", "urlC"), null), Row("ID2", Array("urlA1", "urlB1", "urlC1"), true)).asJava
  val references = List(Row("ID1", "HEAD", "RH1"), Row("ID1", "refs/master", "RH1"), Row("ID2", "refs/master", "RH2")).asJava
  val commits = List(Row()).asJava
  val files = List(Row()).asJava
}
