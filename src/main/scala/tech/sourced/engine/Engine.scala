package tech.sourced.engine

import java.nio.file.Paths
import java.util.Properties

import org.apache.spark.sql.functions.{lit, when}
import org.apache.spark.SparkException
import org.apache.spark.groupon.metrics.UserMetricsSystem
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import tech.sourced.engine.rule._
import tech.sourced.engine.udf.ConcatArrayUDF

import scala.collection.JavaConversions.asScalaBuffer

/**
  * Engine is the main entry point to all usage of the source{d} spark-engine.
  * It has methods to configure all possible configurable options as well as
  * the available methods to start analysing repositories of code.
  *
  * {{{
  * import tech.sourced.engine._
  *
  * val engine = Engine(sparkSession, "/path/to/repositories")
  * }}}
  *
  * NOTE: Keep in mind that you will need to register the UDFs in the session
  * manually if you choose to instantiate this class directly instead of using
  * the companion object.
  *
  * {{{
  * import tech.sourced.engine.{Engine, SessionFunctions}
  *
  * engine = new Engine(sparkSession)
  * sparkSession.registerUDFs()
  * }}}
  *
  * The only method available as of now is getRepositories, which will generate
  * a DataFrame of repositories, which is the very first thing you need to
  * analyse repositories of code.
  *
  * @constructor creates a Engine instance with the given Spark session.
  * @param session Spark session to be used
  */
class Engine(val session: SparkSession,
             repositoriesPath: String,
             repositoriesFormat: String) extends Logging {

  UserMetricsSystem.initialize(session.sparkContext, "Engine")

  this.setRepositoriesPath(repositoriesPath)
  this.setRepositoriesFormat(repositoriesFormat)
  session.registerUDFs()
  session.experimental.extraOptimizations = Seq(
    AddSourceToAttributes,
    SquashGitRelationsJoin,
    SquashMetadataRelationsJoin
  )
  registerViews()

  /**
    * Register the initial views with the DefaultSource.
    */
  private def registerViews(): Unit = {
    Sources.orderedSources.foreach(table => {
      session.read.format(DefaultSourceName)
        .option(DefaultSource.TableNameKey, table)
        .load(session.sqlContext.getConf(RepositoriesPathKey))
        .createOrReplaceTempView(table)
    })
  }

  /**
    * Registers in the current session the views of the MetadataSource so the data is obtained
    * from the metadata database instead of reading the repositories with the DefaultSource.
    *
    * @param dbPath path to the folder that contains the database.
    * @param dbName name of the database file (engine_metadata.db) by default.
    * @return the same instance of the engine
    */
  def fromMetadata(dbPath: String, dbName: String = MetadataSource.DefaultDbName): Engine = {
    Seq(RepositoriesTable, ReferencesTable, CommitsTable, TreeEntriesTable).foreach(table => {
      session.read.format(MetadataSourceName)
        .option(DefaultSource.TableNameKey, table)
        .option(MetadataSource.DbPathKey, dbPath)
        .option(MetadataSource.DbNameKey, dbName)
        .load()
        .createOrReplaceTempView(table)
    })
    this
  }

  /**
    * Registers in the current session the views of the DefaultSource so the data is obtained
    * by reading the repositories instead of reading from the MetadataSource. This has no effect
    * if [[Engine#fromMetadata]] has not been called before.
    *
    * @return the same instance of the engine
    */
  def fromRepositories(): Engine = {
    registerViews()
    this
  }

  /**
    * Returns a DataFrame with the data about the repositories found at
    * the specified repositories path in the form of siva files.
    * To call this method you need to have set before the repositories path,
    * you can do so by calling setRepositoriesPath or, preferably, instantiating
    * the Engine using the companion object.
    *
    * {{{
    * val reposDf = engine.getRepositories
    * }}}
    *
    * @return DataFrame
    */
  def getRepositories: DataFrame = getDataSource("repositories", session)

  /**
    * Retrieves the blobs of a list of repositories, reference names and commit hashes.
    * So the result will be a [[org.apache.spark.sql.DataFrame]] of all the blobs in
    * the given commits that are in the given references that belong to the given
    * repositories.
    *
    * {{{
    * val blobsDf = engine.getBlobs(repoIds, refNames, hashes)
    * }}}
    *
    * Calling this function with no arguments is the same as:
    *
    * {{{
    * engine.getRepositories.getReferences.getCommits.getTreeEntries.getBlobs
    * }}}
    *
    * @param repositoryIds  List of the repository ids to filter by (optional)
    * @param referenceNames List of reference names to filter by (optional)
    * @param commitHashes   List of commit hashes to filter by (optional)
    * @return [[org.apache.spark.sql.DataFrame]] with blobs of the given commits, refs and repos.
    */
  def getBlobs(repositoryIds: Seq[String] = Seq(),
               referenceNames: Seq[String] = Seq(),
               commitHashes: Seq[String] = Seq()): DataFrame = {
    val df = getRepositories

    var reposDf = df
    if (repositoryIds.nonEmpty) {
      reposDf = reposDf.filter(reposDf("id").isin(repositoryIds: _*))
    }

    var refsDf = reposDf.getReferences
    if (referenceNames.nonEmpty) {
      refsDf = refsDf.filter(refsDf("name").isin(referenceNames: _*))
    }

    var commitsDf = refsDf.getCommits
    if (commitHashes.nonEmpty) {
      commitsDf = commitsDf.getAllReferenceCommits.filter(commitsDf("hash").isin(commitHashes: _*))
    }

    commitsDf.getTreeEntries.getBlobs
  }

  /**
    * This method is only offered for easier usage from Python.
    */
  private[engine] def getBlobs(repositoryIds: java.util.List[String],
                               referenceNames: java.util.List[String],
                               commitHashes: java.util.List[String]): DataFrame =
    getBlobs(
      asScalaBuffer(repositoryIds),
      asScalaBuffer(referenceNames),
      asScalaBuffer(commitHashes)
    )

  /**
    * Sets the path where the siva files of the repositories are stored.
    * Although this can actually be called the proper way to use Engine is
    * to instantiate it using the Engine companion object, which already
    * asks for the path in its apply method. If you already instantiated the
    * API instance using the Engine companion object you don't need to call
    * this unless you want to change the repositories path.
    * Note that setting this will affect the session, so any other uses of the
    * session outside the Engine instance will also have that config set.
    *
    * {{{
    * engine.setRepositoriesPath("/path/to/repositories")
    * }}}
    *
    * @param path of the repositories.
    * @return instance of the engine itself
    */
  def setRepositoriesPath(path: String): Engine = {
    session.conf.set(RepositoriesPathKey, path)
    this
  }

  /**
    * Sets the format of the stored repositories on the specified path.
    *
    * Actual compatible formats are:
    *
    * - siva: to read siva files
    * - bare: to read bare repositories
    * - standard: to read standard git repositories (with workspace)
    *
    * @param format of the repositories.
    * @return instance of the engine itself
    */
  def setRepositoriesFormat(format: String): Engine = {
    session.conf.set(RepositoriesFormatKey, format)
    this
  }

  /**
    * Configures the Engine so it won't cleanup the unpacked siva files after
    * it's done with them to avoid having to unpack them afterwards.
    *
    * {{{
    * // disable cleanup
    * engine.skipCleanup(true)
    *
    * // enable cleanup again
    * engine.skipCleanup(false)
    * }}}
    *
    * @param skip whether to skip cleanup or not
    * @return instance of the engine itself
    */
  def skipCleanup(skip: Boolean): Engine = {
    session.conf.set(SkipCleanupKey, skip)
    this
  }

  /**
    * Configures the Engine so it will skip all read errors occurred while
    * reading siva files or repositories.
    *
    * {{{
    * engine.skipReadErrors(true)
    * }}}
    *
    * @param skip whether to skip read errors or not
    * @return instance of the engine
    */
  def skipReadErrors(skip: Boolean): Engine = {
    session.conf.set(SkipReadErrorsKey, skip)
    this
  }

  /**
    * Saves all the metadata in a SQLite database on the given path as "engine_metadata.db".
    * If the database already exists, it will be overwritten. The given path must exist and
    * must be a directory, otherwise it will throw a [[SparkException]].
    * Saved tables are repositories, references, commits and tree_entries. Blobs are not saved.
    *
    * @param path   where database with the metadata will be stored.
    * @param dbName name of the database file
    * @throws SparkException when the given path is not a folder or does not exist.
    */
  def saveMetadata(path: String, dbName: String = MetadataSource.DefaultDbName): Unit = {
    val folder = Paths.get(path)
    if (!folder.toFile.exists() || !folder.toFile.isDirectory) {
      throw new SparkException("folder given to saveMetadata is not a directory " +
        "or does not exist")
    }

    val dbFile = folder.resolve(dbName)
    if (dbFile.toFile.exists) {
      log.warn(s"metadata file '$dbFile' already exists, it will be deleted")
      dbFile.toFile.delete()
    }

    val properties = new Properties()
    properties.put("driver", "org.sqlite.JDBC")

    val repositoriesDf = getDataSource(RepositoriesTable, session)
    val referencesDf = repositoriesDf.getReferences
    val commitsDf = referencesDf.getAllReferenceCommits
    val treeEntriesDf = commitsDf.getTreeEntries

    import MetadataDataFrameCompat._

    Seq(
      (RepositoriesTable, repositoriesDf
        .withStringArrayColumnAsString("urls")
        .withBooleanColumnAsInt("is_fork")),
      (ReferencesTable, referencesDf.withBooleanColumnAsInt("is_remote")),
      (CommitsTable, commitsDf
        .drop("reference_name", "repository_id", "index")
        .withStringArrayColumnAsString("parents")
        .distinct()),
      (RepositoryHasCommitsTable, commitsDf
        .select("hash", "reference_name", "repository_id", "index")),
      (TreeEntriesTable, treeEntriesDf
        .drop("reference_name", "repository_id").distinct())
    ) foreach {
      case (table, df) =>
        Tables(table).create(dbFile.toString, df.schema)
        df.repartition(session.currentActiveExecutors())
          .write
          .mode(SaveMode.Append)
          .jdbc(s"jdbc:sqlite:$dbFile", Tables.prefix(table), properties)
    }
  }

}

/**
  * Factory for [[tech.sourced.engine.Engine]] instances.
  */
object Engine {
  /**
    * Creates a new Engine instance with the given Spark session and
    * configures the repositories path for that session.
    *
    * {{{
    * import tech.sourced.engine._
    *
    * val engine = Engine(sparkSession, "/path/to/repositories")
    * }}}
    *
    * @param session            spark session to use
    * @param repositoriesPath   the path to the repositories
    * @param repositoriesFormat format of the repositories inside the provided path.
    *                           It can be siva, bare or standard.
    * @return Engine instance
    */
  def apply(session: SparkSession, repositoriesPath: String, repositoriesFormat: String): Engine = {
    new Engine(session, repositoriesPath, repositoriesFormat)
  }
}

/**
  * Contains the Convert implicit class that gives DataFrame some methods to
  * deal with compatibility between the regular DefaultSource dataframe and the
  * MetadataSource one.
  */
private object MetadataDataFrameCompat {

  implicit class Convert(df: DataFrame) {
    /**
      * Returns a new DataFrame with the given boolean column converted to
      * an int column, being 0 the value for false and 1 for true.
      *
      * @param column column name
      * @return new DataFrame
      */
    def withBooleanColumnAsInt(column: String): DataFrame =
      df.withColumn(column, when(df(column) === false, 0)
        .otherwise(when(df(column) === true, 1).otherwise(null)))

    /**
      * Returns a new DataFrame with the given string array column converted to
      * a column with the content of the array joined by "|".
      *
      * @param column column name
      * @return new dataframe
      */
    def withStringArrayColumnAsString(column: String): DataFrame =
      df.withColumn(column, ConcatArrayUDF(df.sparkSession)(df(column), lit("|")))
  }

}
