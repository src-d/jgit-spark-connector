package tech.sourced.api

import org.apache.spark.sql.types._

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
      StructField("index", IntegerType) ::
      StructField("hash", StringType) ::
      StructField("message", StringType) ::
      StructField("parents", ArrayType(StringType, containsNull = false)) ::
      StructField("tree", MapType(StringType, StringType, valueContainsNull = false)) ::
      StructField("blobs", ArrayType(StringType, containsNull = false)) ::
      StructField("parents_count", IntegerType) ::

      StructField("author_email", StringType) ::
      StructField("author_name", StringType) ::
      StructField("author_date", TimestampType) ::

      StructField("committer_email", StringType) ::
      StructField("committer_name", StringType) ::
      StructField("committer_date", TimestampType) ::

      Nil
  )

  val files = StructType(
    StructField("file_hash", StringType) ::
      StructField("content", BinaryType) ::
      StructField("commit_hash", StringType) ::
      StructField("is_binary", BooleanType, nullable = false) ::
      StructField("path", StringType) ::

      Nil
  )

  //  StructField("lang", StringType) ::
  //  StructField("uast", BinaryType) ::

}
