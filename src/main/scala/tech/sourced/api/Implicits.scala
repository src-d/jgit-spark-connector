package tech.sourced.api

import org.apache.spark.SparkException
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object Implicits {
  implicit def addSessionFunctions(session: SparkSession) = {
    new SessionFunctions(session)
  }

  implicit class ApiDataFrame(df: DataFrame) {

    import df.sparkSession.implicits._

    def getReferences(): DataFrame = {
      Implicits.checkCols(df, "id")
      val reposIdsDf = df.select($"id").distinct()
      Implicits.getDataSource("references", df.sparkSession).join(reposIdsDf, $"repository_id" === $"id").drop($"id")
    }

    def getCommits(): DataFrame = {
      Implicits.checkCols(df, "repository_id")
      val refsIdsDf = df.select($"hash", $"repository_id").distinct()
      val commitsDf = Implicits.getDataSource("commits", df.sparkSession)
      commitsDf.join(refsIdsDf, refsIdsDf("repository_id") === commitsDf("repository_id") &&
        commitsDf("reference_hash") === refsIdsDf("hash"))
        .drop(refsIdsDf("hash")).drop(refsIdsDf("repository_id"))
    }

    def getFiles(): DataFrame = {
      Implicits.checkCols(df, "tree")
      val blobsIdsDf = df.select($"blobs").distinct()
      val filesDf = Implicits.getDataSource("files", df.sparkSession)
      filesDf.join(blobsIdsDf, array_contains(blobsIdsDf("blobs"), filesDf("file_hash"))).drop($"blobs")
    }
  }

  def getDataSource(table: String, session: SparkSession): DataFrame =
    session.read.format("tech.sourced.api.DefaultSource").option("table", table).load()

  def checkCols(df: DataFrame, cols: String*): Unit = {
    if (!df.schema.fieldNames.containsSlice(cols)) {
      throw new SparkException("method cannot be applied into this DataFrame")
    }
  }
}

class SessionFunctions(session: SparkSession) {
  def getRepositories(): DataFrame = Implicits.getDataSource("repositories", session)
}
