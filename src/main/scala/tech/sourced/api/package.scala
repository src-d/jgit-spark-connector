package tech.sourced

import org.apache.spark.SparkException
import org.apache.spark.sql.{DataFrame, SparkSession}
import tech.sourced.api.customudf.{ClassifyLanguagesUDF, CustomUDF}

package object api {
  private[api] val repositoriesPathKey = "tech.sourced.api.repositories.path"

  implicit class SessionFunctions(session: SparkSession) {
    def registerUDFs(): Unit = {
      SessionFunctions.UDFtoRegister.foreach(customUDF => session.udf.register(customUDF.name, customUDF.function))
    }
  }

  implicit class ApiDataFrame(df: DataFrame) {

    import df.sparkSession.implicits._

    def getReferences: DataFrame = {
      checkCols(df, "id")
      val reposIdsDf = df.select($"id").distinct()
      getDataSource("references", df.sparkSession).join(reposIdsDf, $"repository_id" === $"id").drop($"id")
    }

    def getCommits: DataFrame = {
      checkCols(df, "repository_id")
      val refsIdsDf = df.select($"name", $"repository_id").distinct()
      val commitsDf = getDataSource("commits", df.sparkSession)
      commitsDf.join(refsIdsDf, refsIdsDf("repository_id") === commitsDf("repository_id") &&
        commitsDf("reference_name") === refsIdsDf("name"))
        .drop(refsIdsDf("name")).drop(refsIdsDf("repository_id"))
    }

    def getFiles: DataFrame = {
      val filesDf = getDataSource("files", df.sparkSession)

      if (df.schema.fieldNames.contains("hash")) {
        val commitsDf = df.drop("tree").distinct()
        filesDf.join(commitsDf, filesDf("commit_hash") === commitsDf("hash")).drop($"hash")
      } else {
        checkCols(df, "reference_name")
        filesDf
      }
    }

    def classifyLanguages: DataFrame = {
      checkCols(df, "is_binary", "path", "content")
      df.withColumn("lang", ClassifyLanguagesUDF.function('is_binary, 'path, 'content))
    }
  }

  private[api] def getDataSource(table: String, session: SparkSession): DataFrame =
    session.read.format("tech.sourced.api.DefaultSource")
      .option("table", table)
      .load(session.sqlContext.getConf(repositoriesPathKey))

  private[api] def checkCols(df: DataFrame, cols: String*): Unit = {
    if (!df.schema.fieldNames.containsSlice(cols)) {
      throw new SparkException("method cannot be applied into this DataFrame")
    }
  }

  private[api] object SessionFunctions {
    val UDFtoRegister = List[CustomUDF](
      ClassifyLanguagesUDF
    )
  }
}