package tech.sourced.engine

import org.apache.spark.sql.types._

/**
  * Schema contains all the schemas of the multiple tables offered by this library.
  */
private[engine] object Schema {

  /**
    * Repositories table schema. Contains just the identifier of the repository,
    * its URLs and whether it's a fork or not.
    */
  val repositories = StructType(
    StructField("id", StringType, nullable = false) ::
      StructField("urls", ArrayType(StringType, containsNull = false), nullable = false) ::
      StructField("is_fork", BooleanType) ::
      Nil
  )

  /**
    * References table schema containing the repository to which they belong,
    * the name and the hash of the object they point to.
    */
  val references = StructType(
    StructField("repository_id", StringType, nullable = false) ::
      StructField("name", StringType, nullable = false) ::
      StructField("hash", StringType, nullable = false) ::
      Nil
  )

  /**
    * Commits table schema containing all the data about commits.
    */
  val commits = StructType(
    StructField("repository_id", StringType, nullable = false) ::
      StructField("reference_name", StringType, nullable = false) ::
      StructField("index", IntegerType, nullable = false) ::
      StructField("hash", StringType, nullable = false) ::
      StructField("message", StringType, nullable = false) ::
      StructField("parents", ArrayType(StringType, containsNull = false)) ::
      StructField("parents_count", IntegerType, nullable = false) ::

      StructField("author_email", StringType) ::
      StructField("author_name", StringType) ::
      StructField("author_date", TimestampType) ::

      StructField("committer_email", StringType) ::
      StructField("committer_name", StringType) ::
      StructField("committer_date", TimestampType) ::

      Nil
  )

  /**
    * Tree Entries table schema containing all the tree entries data.
    */
  val treeEntries = StructType(
    StructField("commit_hash", StringType, nullable = false) ::
      StructField("repository_id", StringType, nullable = false) ::
      StructField("reference_name", StringType, nullable = false) ::
      StructField("path", StringType, nullable = false) ::
      StructField("blob", StringType, nullable = false) ::
      Nil
  )

  /**
    * Blobs table schema containing all the blobs data.
    */
  val blobs = StructType(
    StructField("blob_id", StringType, nullable = false) ::
      StructField("commit_hash", StringType, nullable = false) ::
      StructField("repository_id", StringType, nullable = false) ::
      StructField("reference_name", StringType, nullable = false) ::
      StructField("content", BinaryType) ::
      StructField("is_binary", BooleanType, nullable = false) ::
      Nil
  )

}
