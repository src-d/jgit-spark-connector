package tech.sourced.api

import org.apache.spark.sql.types._

/**
  * Schema contains all the schemas of the multiple tables offered by this library.
  */
private[api] object Schema {
  /**
    * Repositories table schema. Contains just the identifier of the repository,
    * its URLs and whether it's a fork or not.
    */
  val repositories = StructType(
    StructField("id", StringType) ::
      StructField("urls", ArrayType(StringType, containsNull = false)) ::
      StructField("is_fork", BooleanType) ::
      Nil
  )

  /**
    * References table schema containing the repository to which they belong,
    * the name and the hash of the object they point to.
    */
  val references = StructType(
    StructField("repository_id", StringType) ::
      StructField("name", StringType) ::
      StructField("hash", StringType) ::
      Nil
  )

  /**
    * Commits table schema containing all the data about commits.
    */
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

  /**
    * Files table schema containing all the files data.
    * Even though it contains repository_id and reference_name as columns, they
    * are not always returned! They are exposed merely for performance reasons
    * in the `getFiles` API method.
    */
  val files = StructType(
    StructField("repository_id", StringType) ::
      StructField("reference_name", StringType) ::
      StructField("file_hash", StringType) ::
      StructField("content", BinaryType) ::
      StructField("commit_hash", StringType) ::
      StructField("is_binary", BooleanType, nullable = false) ::
      StructField("path", StringType) ::

      Nil
  )

}
