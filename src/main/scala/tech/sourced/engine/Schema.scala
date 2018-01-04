package tech.sourced.engine

import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.expressions.Attribute
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
      StructField("repository_path", StringType) ::
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
      StructField("is_remote", BooleanType, nullable = false) ::
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

  /**
    * Return the schema for the table with the given name. Throws a SparkException
    * if there is no schema for the given table.
    *
    * @param table name
    * @return schema for the table
    * @throws SparkException if the table does not exist
    */
  def apply(table: String): StructType = table match {
    case "repositories" => Schema.repositories
    case "references" => Schema.references
    case "commits" => Schema.commits
    case "tree_entries" => Schema.treeEntries
    case "blobs" => Schema.blobs
    case other => throw new SparkException(s"table '$other' is not supported")
  }

  /**
    * Returns a tuple with the table and column names for the given attribute.
    * Because metadata tables are different from git relation tables, some fields
    * need to be mapped to match one schema with the other.
    *
    * @param attr attribute from the git relation schema
    * @return table and column names
    */
  def metadataTableAndCol(attr: Attribute): (String, String) = {
    val name = attr.name
    val table = attr.metadata.getString(Sources.SourceKey)
    metadataMappings(table, name).getOrElse((table, name))
  }

  /**
    * Mappings between a table name and column name in the git relation schema
    * and their counterpart in the metadata schema.
    *
    * @param table table name
    * @param name  column name
    * @return a tuple with table and column name or None if there is no mapping
    */
  def metadataMappings(table: String, name: String): Option[(String, String)] =
    Option((table, name) match {
      case ("commits", "index") =>
        (RepositoryHasCommitsTable, "index")
      case ("commits", "repository_id") =>
        (RepositoryHasCommitsTable, "repository_id")
      case ("commits", "reference_name") =>
        (RepositoryHasCommitsTable, "reference_name")
      case ("tree_entries", "repository_id") =>
        (RepositoryHasCommitsTable, "repository_id")
      case ("tree_entries", "reference_name") =>
        (RepositoryHasCommitsTable, "reference_name")
      case _ => null
    })

}
