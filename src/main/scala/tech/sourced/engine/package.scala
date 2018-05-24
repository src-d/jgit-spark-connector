package tech.sourced

import gopkg.in.bblfsh.sdk.v1.uast.generated.Node
import org.apache.spark.SparkException
import org.apache.spark.api.java.function.MapFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import tech.sourced.engine.udf._
import tech.sourced.engine.util.Bblfsh

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
  private[engine] val RepositoriesPathKey = "spark.tech.sourced.engine.repositories.path"

  /**
    * Key used for the option to specify the type of repositories. It can be siva, bare or standard
    */
  private[engine] val RepositoriesFormatKey = "spark.tech.sourced.engine.repositories.format"

  /**
    * Key used for the option to specify whether files should be deleted after
    * their usage or not.
    */
  private[engine] val SkipCleanupKey = "spark.tech.sourced.engine.cleanup.skip"

  /**
    * Key used for the option to specify whether read errors should be skipped
    * or not.
    */
  private[engine] val SkipReadErrorsKey = "spark.tech.sourced.engine.skip.read.errors"

  // DataSource names
  val DefaultSourceName: String = "tech.sourced.engine"
  val MetadataSourceName: String = "tech.sourced.engine.MetadataSource"

  // Table names
  val RepositoriesTable: String = "repositories"
  val ReferencesTable: String = "references"
  val CommitsTable: String = "commits"
  val TreeEntriesTable: String = "tree_entries"
  val BlobsTable: String = "blobs"
  val RepositoryHasCommitsTable: String = "repository_has_commits"


  // The keys RepositoriesPathKey, bblfshHostKey, bblfshPortKey and SkipCleanupKey must
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
        customUDF(session)
      ))
    }

    /**
      * Returns the number of executors that are active right now.
      *
      * @return number of active executors
      */
    def currentActiveExecutors(): Int = {
      val sc = session.sparkContext
      val driver = sc.getConf.get("spark.driver.host")
      val executors = sc.getExecutorMemoryStatus
        .keys
        .filter(ex => ex.split(":").head != driver)
        .toArray
        .distinct
        .length

      // If there are no executors, it means it's a local job
      // so there's just one node to get the data from.
      if (executors > 0) executors else 1
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
      getDataSource(ReferencesTable, df.sparkSession)
        .join(reposIdsDf, $"repository_id" === $"id")
        .drop($"id")
    }

    /**
      * Returns a new [[org.apache.spark.sql.DataFrame]] with only remote references in it.
      * If the DataFrame contains repository data it will automatically get the references for
      * those repositories.
      *
      * {{{
      * val remoteRefs = reposDf.getRemoteReferences
      * val remoteRefs2 = reposDf.getReferences.getRemoteReferences
      * }}}
      *
      * @return
      */
    def getRemoteReferences: DataFrame = {
      if (df.columns.contains("is_remote")) {
        df.filter(df("is_remote") === true)
      } else {
        df.getReferences.getRemoteReferences
      }
    }

    /**
      * Returns a new [[org.apache.spark.sql.DataFrame]] with the product of joining the
      * current dataframe with the commits dataframe, returning only the last commit in a
      * reference (aka the current state).
      * It requires the current dataframe to have a "repository_id" column, which is the
      * identifier of the repository.
      *
      * {{{
      * val commitDf = refsDf.getCommits
      * }}}
      *
      * You can use [[tech.sourced.engine.EngineDataFrame#getAllReferenceCommits]] to
      * get all the commits in the references, but do so knowing that is a very costly operation.
      *
      * {{{
      * val allCommitsDf = refsDf.getCommits.getAllReferenceCommits
      * }}}
      *
      * @return new DataFrame containing also commits data.
      */
    def getCommits: DataFrame = {
      checkCols(df, "repository_id")
      val refsIdsDf = df.select($"name", $"repository_id")
      val commitsDf = getDataSource(CommitsTable, df.sparkSession)
      commitsDf.join(refsIdsDf, refsIdsDf("repository_id") === commitsDf("repository_id") &&
        commitsDf("reference_name") === refsIdsDf("name"))
        .drop(refsIdsDf("name")).drop(refsIdsDf("repository_id"))
    }

    /**
      * Returns a new [[org.apache.spark.sql.DataFrame]] with all the commits in a reference.
      * After calling this method, commits may appear multiple times in your [[DataFrame]],
      * because most of your commits will be shared amongst references. Take into account that
      * calling this method will make all your further operations way slower, as there is much
      * more data, specially if you query blobs, which will be repeated over and over.
      *
      * For the next example, consider we have a master branch with 100 commits and a "foo" branch
      * whose parent is the HEAD of master and has two more commits.
      * {{{
      * > commitsDf.count()
      * 2
      * > commitsDf.getAllReferenceCommits.count()
      * 202
      * }}}
      *
      * @return dataframe with all reference commits
      */
    def getAllReferenceCommits: DataFrame = {
      if (df.columns.contains("index")) {
        df.filter("index <> '-1'")
      } else {
        df.getCommits.getAllReferenceCommits
      }
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
      val entriesDf = getDataSource(TreeEntriesTable, df.sparkSession)
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
        val blobsDf = getDataSource(BlobsTable, df.sparkSession)
        blobsDf.join(treesDf, treesDf("blob") === blobsDf("blob_id"))
          .drop($"blob")
      }
    }

    private val HeadRef = "refs/heads/HEAD"
    private val LocalHeadRef = "HEAD"

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
    def getHEAD: DataFrame = {
      if (df.schema.fieldNames.contains("reference_name")) {
        df.filter(($"reference_name" === HeadRef).or($"reference_name" === LocalHeadRef))
      } else if (df.schema.fieldNames.contains("name")) {
        df.filter(($"name" === HeadRef).or($"name" === LocalHeadRef))
      } else {
        df.getReferences.getHEAD
      }
    }

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
      val newDf = df.withColumn("lang", typedLit(null: String))
      val encoder = RowEncoder(newDf.schema)
      newDf.map(new MapFunction[Row, Row] {
        override def call(row: Row): Row = {
          val (isBinaryIdx, pathIdx, contentIdx) = try {
            (row.fieldIndex("is_binary"), row.fieldIndex("path"), row.fieldIndex("content"))
          } catch {
            case _: IllegalArgumentException =>
              throw new SparkException(s"classifyLanguages can not be applied to this DataFrame: "
                + "unable to find all these columns: is_binary, path, content")
          }

          val (isBinary, path, content) = (
            row.getBoolean(isBinaryIdx),
            row.getString(pathIdx),
            row.getAs[Array[Byte]](contentIdx)
          )

          val lang = ClassifyLanguagesUDF.getLanguage(isBinary, path, content).orNull
          Row(row.toSeq.dropRight(1) ++ Seq(lang): _*)
        }
      }, encoder)
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
      val newDf = df.withColumn("uast", typedLit(Seq[Array[Byte]]()))
      val configB = df.sparkSession.sparkContext.broadcast(Bblfsh.getConfig(df.sparkSession))
      val encoder = RowEncoder(newDf.schema)
      newDf.map(new MapFunction[Row, Row] {
        override def call(row: Row): Row = {
          val lang = (try {
            Some(row.fieldIndex("lang"))
          } catch {
            case _: IllegalArgumentException =>
              None
          }).map(row.getString).orNull

          val (pathIdx, contentIdx) = try {
            (row.fieldIndex("path"), row.fieldIndex("content"))
          } catch {
            case _: IllegalArgumentException =>
              throw new SparkException(s"extractUASTs can not be applied to this DataFrame: "
                + "unable to find all these columns: path, content")
          }

          val (path, content) = (row.getString(pathIdx), row.getAs[Array[Byte]](contentIdx))
          val uast = ExtractUASTsUDF.extractUASTs(path, content, lang, configB.value)
          Row(row.toSeq.dropRight(1) ++ Seq(uast): _*)
        }
      }, encoder)
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
      df.withColumn(outputColumn, QueryXPathUDF(df.sparkSession)(df(queryColumn), lit(query)))
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
  private[engine] def getDataSource(table: String, session: SparkSession): DataFrame =
    session.table(table)

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
      ExtractUASTsUDF,
      QueryXPathUDF,
      ExtractTokensUDF,
      ConcatArrayUDF
    )
  }

}
