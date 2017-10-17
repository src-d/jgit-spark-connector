package tech.sourced

import gopkg.in.bblfsh.sdk.v1.uast.generated.Node
import org.apache.spark.SparkException
import org.apache.spark.sql.{DataFrame, SparkSession}
import tech.sourced.api.udf._

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

  /**
    * Key used for the option to specify the path of siva files.
    */
  private[api] val repositoriesPathKey = "spark.tech.sourced.api.repositories.path"

  /**
    * Key used for the option to specify whether files should be deleted after
    * their usage or not.
    */
  private[api] val skipCleanupKey = "spark.tech.sourced.api.cleanup.skip"

  // The keys repositoriesPathKey, bblfshHostKey, bblfshPortKey and skipCleanupKey must
  // start by "spark." to be able to be loaded from the "spark-defaults.conf" file.


  /** Local spark directory. */
  private[api] val localPathKey = "spark.local.dir"

  /**
    * Implicit class that adds some functions to the [[org.apache.spark.sql.SparkSession]].
    *
    * @param session Spark Session
    */
  implicit class SessionFunctions(session: SparkSession) {

    /**
      * Registers some user defined functions in the [[org.apache.spark.sql.SparkSession]].
      */
    def registerUDFs(): Unit = {
      SessionFunctions.UDFtoRegister.foreach(customUDF => session.udf.register(
        customUDF.name,
        customUDF.function(session)
      ))
    }

  }

  /**
    * Create a new [[Node]] from a binary-encoded node as a byte array.
    *
    * @param data binary-encoded node as byte array
    * @return parsed [[Node]]
    */
  def parseUASTNode(data: Array[Byte]): Node = Node.parseFrom(data)

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
      val reposIdsDf = df.select($"id")
      getDataSource("references", df.sparkSession)
        .join(reposIdsDf, $"repository_id" === $"id")
        .drop($"id")
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
      val refsIdsDf = df.select($"name", $"repository_id")
      val commitsDf = getDataSource("commits", df.sparkSession)
      commitsDf.join(refsIdsDf, refsIdsDf("repository_id") === commitsDf("repository_id") &&
        commitsDf("reference_name") === refsIdsDf("name"))
        .drop(refsIdsDf("name")).drop(refsIdsDf("repository_id"))
    }

    /**
      * Returns a new [[org.apache.spark.sql.DataFrame]] with only the first commit in a reference.
      * Without calling this method, commits may appear multiple times in your [[DataFrame]],
      * because most of your commits will be shared amongst references. Calling this, your
      * [[DataFrame]] will only contain the HEAD commit of each reference.
      *
      * For the next example, consider we have a master branch with 100 commits and a "foo" branch
      * whose parent is the HEAD of master and has two more commits.
      * {{{
      * > commitsDf.count()
      * 202
      * > commitsDf.getFirstReferenceCommit.count()
      * 2
      * }}}
      *
      * @return dataframe with only the HEAD commits of each reference
      */
    def getFirstReferenceCommit: DataFrame = {
      checkCols(df, "index")
      df.filter($"index" === 0)
    }

    /**
      * Returns a new [[org.apache.spark.sql.DataFrame]] with the product of joining the
      * current dataframe with the files dataframe.
      *
      * {{{
      * val filesDf = commitsDf.getFiles
      * }}}
      *
      * @return new DataFrame containing also files data.
      */
    def getFiles: DataFrame = {
      val filesDf = getDataSource("files", df.sparkSession)

      if (df.schema.fieldNames.contains("index")) {
        val commitsDf = df.select("hash")
        filesDf.join(commitsDf, filesDf("commit_hash") === commitsDf("hash"))
          .drop($"hash")
          .distinct()
      } else {
        checkCols(df, "name")
        df.getCommits.getFiles
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
      df.withColumn(
        "lang",
        ClassifyLanguagesUDF.function(df.sparkSession)('is_binary, 'path, 'content)
      )
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
        df.withColumn(
          "uast",
          ExtractUASTsUDF.functionWithLang(df.sparkSession)('path, 'content, 'lang)
        )
      } else {
        df.withColumn("uast", ExtractUASTsUDF.function(df.sparkSession)('path, 'content))
      }
    }

    /**
      * Queries a list of UAST nodes with the given query to get specific nodes,
      * and puts the result in another column.
      *
      * {{{
      * import gopkg.in.bblfsh.sdk.v1.uast.generated.{Node, Role}
      *
      * // get all identifiers
      * var identifiers = uastsDf.queryUAST("//\*[@roleIdentifier and not(@roleIncomplete)]")
      *   .collect()
      *   .map(row => row(row.fieldIndex("result")))
      *   .flatMap(_.asInstanceOf[Seq[Array[Byte]]])
      *   .map(Node.from)
      *   .map(_.token)
      *
      * // get all identifiers from column "foo" and put them in "bar"
      * identifiers = uastsDf.queryUAST("//\*[@roleIdentifier and not(@roleIncomplete)]",
      *                                 "foo", "bar")
      *   .collect()
      *   .map(row => row(row.fieldIndex("result")))
      *   .flatMap(_.asInstanceOf[Seq[Array[Byte]]])
      *   .map(Node.from)
      *   .map(_.token)
      * }}}
      *
      * @param query        xpath query
      * @param queryColumn  column where the list of UAST nodes to query are
      * @param outputColumn column where the result of the query will be placed
      * @return [[DataFrame]] with "result" column containing the nodes
      */
    def queryUAST(query: String,
                  queryColumn: String = "uast",
                  outputColumn: String = "result"): DataFrame = {
      checkCols(df, "uast", queryColumn)
      if (df.columns.contains(outputColumn)) {
        throw new SparkException(s"DataFrame already contains a column named $outputColumn")
      }

      df.withColumn(
        outputColumn,
        QueryXPathUDF(df.sparkSession, query)(df(queryColumn))
      )
    }

    /**
      * Extracts the tokens in all nodes of a given column and puts the list of retrieved
      * tokens in a new column.
      *
      * {{{
      * val tokensDf = uastDf.queryUAST("//\*[@roleIdentifier and not(@roleIncomplete)]")
      *     .extractTokens()
      * }}}
      *
      * @param queryColumn  column where the UAST nodes are.
      * @param outputColumn column to put the result
      * @return new [[DataFrame]] with the extracted tokens
      */
    def extractTokens(queryColumn: String = "result",
                      outputColumn: String = "tokens"): DataFrame = {
      checkCols(df, queryColumn)
      if (df.columns.contains(outputColumn)) {
        throw new SparkException(s"DataFrame already contains a column named $outputColumn")
      }

      df.withColumn(outputColumn, ExtractTokensUDF()(df(queryColumn)))
    }

  }

  /**
    * Returns a [[org.apache.spark.sql.DataFrame]] for the given table using the provided
    * [[org.apache.spark.sql.SparkSession]].
    *
    * @param table   name of the table
    * @param session spark session
    * @return dataframe for the given table
    */
  private[api] def getDataSource(table: String, session: SparkSession): DataFrame =
    session.read.format("tech.sourced.api.DefaultSource")
      .option("table", table)
      .load(session.sqlContext.getConf(repositoriesPathKey))

  /**
    * Ensures the given [[org.apache.spark.sql.DataFrame]] contains some required columns.
    *
    * @param df   dataframe that must have the columns
    * @param cols names of the columns that are required
    * @throws SparkException if any column is missing
    */
  private[api] def checkCols(df: DataFrame, cols: String*): Unit = {
    if (!df.columns.exists(cols.contains)) {
      throw new SparkException(s"Method can not be applied to this DataFrame: "
        + s"required:'${cols.mkString(" ")}',"
        + s" actual columns:'${df.columns.mkString(" ")}'")
    }
  }

  /**
    * Object that contains the user defined functions that will be added to the session.
    */
  private[api] object SessionFunctions {

    /**
      * List of custom functions to be registered.
      */
    val UDFtoRegister: List[CustomUDF] = List(
      ClassifyLanguagesUDF,
      ExtractUASTsUDF,
      QueryXPathUDF
    )
  }

}
