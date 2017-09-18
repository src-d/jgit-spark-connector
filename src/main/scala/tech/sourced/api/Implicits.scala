package tech.sourced.api

import org.apache.spark.SparkException
import org.apache.spark.sql.{DataFrame, SparkSession}
import tech.sourced.api.customudf.{CustomUDF, ClassifyLanguagesUDF}
import tech.sourced.api.udf.BblfshUDF

object Implicits {

  implicit class SessionFunctions(session: SparkSession) {
    def registerUDFs(): Unit = {
      SessionFunctions.UDFtoRegister.foreach(customUDF => session.udf.register(customUDF.name, customUDF.function))
    }

    def getRepositories(): DataFrame = Implicits.getDataSource("repositories", session)
  }

  object SessionFunctions {
    val UDFtoRegister = List[CustomUDF](
      ClassifyLanguagesUDF
    )
  }

  implicit class ApiDataFrame(df: DataFrame) {

    import df.sparkSession.implicits._

    def getReferences: DataFrame = {
      Implicits.checkCols(df, "id")
      val reposIdsDf = df.select($"id").distinct()
      Implicits.getDataSource("references", df.sparkSession).join(reposIdsDf, $"repository_id" === $"id").drop($"id")
    }

    def getCommits: DataFrame = {
      Implicits.checkCols(df, "repository_id")
      val refsIdsDf = df.select($"name", $"repository_id").distinct()
      val commitsDf = Implicits.getDataSource("commits", df.sparkSession)
      commitsDf.join(refsIdsDf, refsIdsDf("repository_id") === commitsDf("repository_id") &&
        commitsDf("reference_name") === refsIdsDf("name"))
        .drop(refsIdsDf("name")).drop(refsIdsDf("repository_id"))
    }

    def getFiles: DataFrame = {
      val filesDf = Implicits.getDataSource("files", df.sparkSession)

      if (df.schema.fieldNames.contains("hash")) {
        val commitsDf = df.drop("tree").distinct()
        filesDf.join(commitsDf, filesDf("commit_hash") === commitsDf("hash")).drop($"hash")
      } else {
        Implicits.checkCols(df, "reference_name")
        filesDf
      }
    }

    def classifyLanguages: DataFrame = {
      Implicits.checkCols(df, "is_binary", "path", "content")
      df.withColumn("lang", ClassifyLanguagesUDF.function('is_binary, 'path, 'content))
    }

    def extractUASTs(): DataFrame = {
      Implicits.checkCols(df, "path", "content")
      import org.apache.spark.sql.functions.udf
      val extractUastUDF = udf(BblfshUDF.extractUAST)
      df.withColumn("uast", extractUastUDF('path, 'content))
    }

  }

  def getDataSource(table: String, session: SparkSession): DataFrame =
    session.read.format("tech.sourced.api.DefaultSource")
      .option("table", table)
      .load(session.sqlContext.getConf("tech.sourced.api.repositories.path"))

  def checkCols(df: DataFrame, cols: String*): Unit = {
    if (!df.schema.fieldNames.containsSlice(cols)) {
      throw new SparkException("method cannot be applied into this DataFrame")
    }
  }

}

