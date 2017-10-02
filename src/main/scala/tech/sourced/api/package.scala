package tech.sourced

import org.apache.spark.SparkException
import org.apache.spark.sql.{DataFrame, SparkSession}
import tech.sourced.api.udf.{ClassifyLanguagesUDF, CustomUDF, ExtractUASTsUDF}

/**
  * Provides the [[tech.sourced.api.SparkAPI]] class, which is the main entry point
  * of all the analysis you might do using this library as well as some implicits
  * to make it easier to use. In particular, it adds some methods to be able to
  * join with other "tables" directly from any [[org.apache.spark.sql.DataFrame]].
  *
  * {{{
  * import tech.sourced.api._
  *
  * val api = SparkAPI(sparkSession, "/path/to/repositories")
  * }}}
  *
  * If you don't want to import everything in the package, even though it only exposes
  * what's truly needed to not pollute the user namespace, you can do it by just importing
  * the [[tech.sourced.api.SparkAPI]] class and the [[tech.sourced.api.ApiDataFrame]]
  * implicit class.
  *
  * {{{
  * import tech.sourced.api.{SparkAPI, ApiDataFrame}
  *
  * val api = SparkAPI(sparkSession, "/path/to/repositories")
  * }}}
  */
package object api {

  private[api] val repositoriesPathKey = "tech.sourced.api.repositories.path"
  private[api] val bblfshHostKey = "tech.sourced.bblfsh.grpc.host"
  private[api] val bblfsPortKey = "tech.sourced.bblfsh.grpc.port"
  private[api] val skipCleanupKey = "tech.sourced.api.cleanup.skip"

  implicit class SessionFunctions(session: SparkSession) {
    def registerUDFs(): Unit = {
      ExtractUASTsUDF.bblfshHost = session.sparkContext.getConf.get(bblfshHostKey, "0.0.0.0")
      ExtractUASTsUDF.bblfshPort = session.sparkContext.getConf.getInt(bblfsPortKey, 9432)

      SessionFunctions.UDFtoRegister.foreach(customUDF => session.udf.register(customUDF.name, customUDF.function))
    }
  }

  /**
    * Adds some utility methods to the [[org.apache.spark.sql.DataFrame]] class
    * so you can, for example, get the references, commits, etc from a data frame
    * containing repositories.
    *
    * @param df the DataFrame
    */
  implicit class ApiDataFrame(df: DataFrame) {

    import df.sparkSession.implicits._

    /**
      * Returns a new [[org.apache.spark.sql.DataFrame]] with the product of joining the
      * current dataframe with the references dataframe.
      * It requires the dataframe to have an "id" column, which should be the repository
      * identifier.
      *
      * {{{
      * val refsDf = reposDf.getReferences
      * }}}
      *
      * @return new DataFrame containing also references data.
      */
    def getReferences: DataFrame = {
      checkCols(df, "id")
      val reposIdsDf = df.select($"id").distinct()
      getDataSource("references", df.sparkSession).join(reposIdsDf, $"repository_id" === $"id").drop($"id")
    }

    /**
      * Returns a new [[org.apache.spark.sql.DataFrame]] with the product of joining the
      * current dataframe with the commits dataframe.
      * It requires the current dataframe to have a "repository_id" column, which is the
      * identifier of the repository.
      *
      * {{{
      * val commitDf = refsDf.getCommits
      * }}}
      *
      * @return new DataFrame containing also commits data.
      */
    def getCommits: DataFrame = {
      checkCols(df, "repository_id")
      val refsIdsDf = df.select($"name", $"repository_id").distinct()
      val commitsDf = getDataSource("commits", df.sparkSession)
      commitsDf.join(refsIdsDf, refsIdsDf("repository_id") === commitsDf("repository_id") &&
        commitsDf("reference_name") === refsIdsDf("name"))
        .drop(refsIdsDf("name")).drop(refsIdsDf("repository_id"))
    }

    /**
      * Returns a new [[org.apache.spark.sql.DataFrame]] with the product of joining the
      * current dataframe with the files dataframe.
      * It requires the current dataframe to have a "files" column, which is are the
      * files of a commit.
      *
      * {{{
      * val filesDf = commitsDf.getFiles
      * }}}
      *
      * Right now this method is very slow. If you are processing a lot of repositories
      * you might have to consider pre-processing the repositories, references and hashes
      * and then using [[tech.sourced.api.SparkAPI.getFiles]] to get the files, instead.
      * This is likely to change in the near future.
      *
      * @return new DataFrame containing also files data.
      */
    def getFiles: DataFrame = {
      var filesDf = getDataSource("files", df.sparkSession)

      if (df.schema.fieldNames.contains("hash")) {
        val commitsDf = df.drop("tree").distinct()
        filesDf = filesDf.drop("repository_id", "reference_name")
        filesDf.join(commitsDf, filesDf("commit_hash") === commitsDf("hash")).drop($"hash")
      } else {
        checkCols(df, "reference_name")
        filesDf.join(df, filesDf("reference_name") === df("name")).drop($"name")
      }
    }

    /**
      * Returns a new [[org.apache.spark.sql.DataFrame]] containing only the rows
      * with a HEAD reference.
      *
      * {{{
      * val headDf = refsDf.getHEAD
      * }}}
      *
      * @return new dataframe with only HEAD reference rows
      */
    def getHEAD: DataFrame = getReference("refs/heads/HEAD")

    /**
      * Returns a new [[org.apache.spark.sql.DataFrame]] containing only the rows
      * with a master reference.
      *
      * {{{
      * val masterDf = refsDf.getMaster
      * }}}
      *
      * @return new dataframe with only the master reference rows
      */
    def getMaster: DataFrame = getReference("refs/heads/master")

    /**
      * Returns a new [[org.apache.spark.sql.DataFrame]] containing only the rows
      * with a reference whose name equals the one provided.
      *
      * {{{
      * val developDf = refsDf.getReference("refs/heads/develop")
      * }}}
      *
      * @param name name of the reference to filter by
      * @return new dataframe with only the given reference rows
      */
    def getReference(name: String): DataFrame = {
      if (df.schema.fieldNames.contains("reference_name")) {
        df.filter($"reference_name" === name)
      } else if (df.schema.fieldNames.contains("name")) {
        df.filter($"name" === name)
      } else {
        df.getReferences.getReference(name)
      }
    }

    /**
      * Returns a new [[org.apache.spark.sql.DataFrame]] with a new column "lang" added
      * containing the language of the non-binary files.
      * It requires the current dataframe to have the files data.
      *
      * {{{
      * val languagesDf = filesDf.classifyLanguages
      * }}}
      *
      * @return new DataFrame containing also language data.
      */
    def classifyLanguages: DataFrame = {
      checkCols(df, "is_binary", "path", "content")
      df.withColumn("lang", ClassifyLanguagesUDF.function('is_binary, 'path, 'content))
    }

    /**
      * Returns a new [[org.apache.spark.sql.DataFrame]] with a new column "uast" added,
      * that contains Protobuf serialized UAST.
      * It requires the current dataframe to have file's data: path and content.
      * If language is available, it's going to be used i.e to avoid parsing Text and Markdown.
      *
      * {{{
      * val uastsDf = filesDf.extractUASTs
      * }}}
      *
      * @return new DataFrame that contains Protobuf serialized UAST.
      */
    def extractUASTs(): DataFrame = {
      checkCols(df, "path", "content")
      if (df.columns.contains("lang")) {
        df.withColumn("uast", ExtractUASTsUDF.functionMoreArgs('path, 'content, 'lang))
      } else {
        df.withColumn("uast", ExtractUASTsUDF.function('path, 'content))
      }
    }

  }

  private[api] def getDataSource(table: String, session: SparkSession): DataFrame =
    session.read.format("tech.sourced.api.DefaultSource")
      .option("table", table)
      .load(session.sqlContext.getConf(repositoriesPathKey))

  private[api] def checkCols(df: DataFrame, cols: String*): Unit = {
    if (!df.columns.exists(cols.contains)) {
      throw new SparkException(s"Method can not be applied to this DataFrame: required:'${cols.mkString(" ")}'," +
        s" actual columns:'${df.columns.mkString(" ")}'")
    }
  }


  private[api] object SessionFunctions {
    val UDFtoRegister = List[CustomUDF](ClassifyLanguagesUDF, ExtractUASTsUDF)
  }

}