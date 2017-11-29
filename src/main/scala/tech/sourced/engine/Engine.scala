package tech.sourced.engine

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkException
import org.apache.spark.sql.{DataFrame, SparkSession}

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
class Engine(session: SparkSession) {

  session.registerUDFs()
  session.experimental.extraOptimizations = Seq(AddSourceToAttributes, SquashGitRelationJoin)

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
    commitsDf = if (commitHashes.nonEmpty) {
      commitsDf.filter(commitsDf("hash").isin(commitHashes: _*))
    } else {
      commitsDf.getFirstReferenceCommit
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
    * @param path of the siva files.
    * @return instance of the engine itself
    */
  def setRepositoriesPath(path: String): Engine = {
    if (!FileSystem.get(session.sparkContext.hadoopConfiguration).exists(new Path(path))) {
      throw new SparkException(s"the given repositories path ($path) does not exist, " +
        s"so siva files can't be read from there")
    }

    session.conf.set(repositoriesPathKey, path)
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
    session.conf.set(skipCleanupKey, skip)
    this
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
    * @param session          spark session to use
    * @param repositoriesPath the path to the repositories' siva files
    * @return Engine instance
    */
  def apply(session: SparkSession, repositoriesPath: String): Engine = {
    new Engine(session)
      .setRepositoriesPath(repositoriesPath)
  }
}
