package tech.sourced

import gopkg.in.bblfsh.sdk.v1.uast.generated.Node
import org.apache.spark.SparkException
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import tech.sourced.engine.udf._

/**
  * Provides the [[tech.sourced.engine.Engine]] class, which is the main entry point
  * of all the analysis you might do using this library as well as some implicits
  * to make it easier to use. In particular, it adds some methods to be able to
  * join with other "tables" directly from any [[org.apache.spark.sql.DataFrame]].
  *
  * {{{
  * import tech.sourced.engine._
  *
  * val engine = Engine(sparkSession, "/path/to/repositories")
  * }}}
  *
  * If you don't want to import everything in the engine, even though it only exposes
  * what's truly needed to not pollute the user namespace, you can do it by just importing
  * the [[tech.sourced.engine.Engine]] class and the [[tech.sourced.engine.EngineDataFrame]]
  * implicit class.
  *
  * {{{
  * import tech.sourced.engine.{Engine, EngineDataFrame}
  *
  * val engine = Engine(sparkSession, "/path/to/repositories")
  * }}}
  */
package object engine {

  /**
    * Key used for the option to specify the path of siva files.
    */
  private[engine] val repositoriesPathKey = "spark.tech.sourced.engine.repositories.path"

  /**
    * Key used for the option to specify whether files should be deleted after
    * their usage or not.
    */
  private[engine] val skipCleanupKey = "spark.tech.sourced.engine.cleanup.skip"

  // The keys repositoriesPathKey, bblfshHostKey, bblfshPortKey and skipCleanupKey must
  // start by "spark." to be able to be loaded from the "spark-defaults.conf" file.

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
  implicit class EngineDataFrame(df: DataFrame) {

    import df.sparkSession.implicits._

    implicit val session = df.sparkSession

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
      * Take into account that getting all the commits will lead to a lot of repeated tree
      * entries and blobs, thus making your query very slow.
      * Most of the time what you probably want is to get the latest state of the files in
      * a specific reference.
      * You can use [[tech.sourced.engine.EngineDataFrame#getFirstReferenceCommit]] for
      * that purpose, which only gets the first commit of a reference, that is, the latest
      * status of the reference.
      *
      * {{{
      * val commitsDf = refsDf.getCommits.getFirstReferenceCommit
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
      * current dataframe with the tree entries dataframe.
      *
      * {{{
      * val entriesDf = commitsDf.getTreeEntries
      * }}}
      *
      * @return new DataFrame containing also tree entries data.
      */
    def getTreeEntries: DataFrame = {
      checkCols(df, "index", "hash") // references also has hash, index makes sure that is commits
      val commitsDf = df.select("hash")
      val entriesDf = getDataSource("tree_entries", df.sparkSession)
      entriesDf.join(commitsDf, entriesDf("commit_hash") === commitsDf("hash"))
        .drop($"hash")
    }

    /**
      * Returns a new [[org.apache.spark.sql.DataFrame]] with the product of joining the
      * current dataframe with the blobs dataframe. If the current dataframe does not contain
      * the tree entries data, getTreeEntries will be called automatically.
      *
      * {{{
      * val blobsDf = treeEntriesDf.getBlobs
      * val blobsDf2 = commitsDf.getBlobs // can be obtained from commits too
      * }}}
      *
      * @return new DataFrame containing also blob data.
      */
    def getBlobs: DataFrame = {
      if (!df.columns.contains("blob")) {
        df.getTreeEntries.getBlobs
      } else {
        val treesDf = df.select("path", "blob")
        val blobsDf = getDataSource("blobs", df.sparkSession)
        blobsDf.join(treesDf, treesDf("blob") === blobsDf("blob_id"))
          .drop($"blob")
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
      df.withColumn("lang", ClassifyLanguagesUDF('is_binary, 'path, 'content))
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
      val lang: Column = if (df.columns.contains("lang")) {
        df("lang")
      } else {
        null
      }

      df.withColumn("uast", ExtractUASTsUDF('path, 'content, lang))
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

      import org.apache.spark.sql.functions.lit
      df.withColumn(outputColumn, QueryXPathUDF(df(queryColumn), lit(query)))
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

      df.withColumn(outputColumn, ExtractTokensUDF(df(queryColumn)))
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
  private[engine] def getDataSource(table: String, session: SparkSession): DataFrame =
    session.read.format("tech.sourced.engine.DefaultSource")
      .option("table", table)
      .load(session.sqlContext.getConf(repositoriesPathKey))

  /**
    * Ensures the given [[org.apache.spark.sql.DataFrame]] contains some required columns.
    *
    * @param df   dataframe that must have the columns
    * @param cols names of the columns that are required
    * @throws SparkException if any column is missing
    */
  private[engine] def checkCols(df: DataFrame, cols: String*): Unit = {
    if (!df.columns.exists(cols.contains)) {
      throw new SparkException(s"Method can not be applied to this DataFrame: "
        + s"required:'${cols.mkString(" ")}',"
        + s" actual columns:'${df.columns.mkString(" ")}'")
    }
  }

  /**
    * Object that contains the user defined functions that will be added to the session.
    */
  private[engine] object SessionFunctions {

    /**
      * List of custom functions to be registered.
      */
    val UDFtoRegister: List[CustomUDF] = List(
      ClassifyLanguagesUDF,
      ExtractUASTsWithoutLangUDF,
      ExtractUASTsWithLangUDF,
      QueryXPathUDF,
      ExtractTokensUDF
    )
  }

}
