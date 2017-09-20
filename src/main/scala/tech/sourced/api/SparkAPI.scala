package tech.sourced.api

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * SparkAPI is the main entry point to all usage of the source{d} spark-api.
  * It has methods to configure all possible configurable options as well as
  * the available methods to start analysing repositories of code.
  *
  * {{{
  * import tech.sourced.api._
  *
  * val api = SparkAPI(sparkSession, "/path/to/repositories")
  * }}}
  *
  * NOTE: Keep in mind that you will need to register the UDFs in the session
  * manually if you choose to instantiate this class directly instead of using
  * the companion object.
  *
  * {{{
  * import tech.sourced.api.{SparkAPI, SessionFunctions}
  *
  * api = new SparkAPI(sparkSession)
  * sparkSession.registerUDFs()
  * }}}
  *
  * The only method available as of now is getRepositories, which will generate
  * a DataFrame of repositories, which is the very first thing you need to
  * analyse repositories of code.
  *
  * @constructor creates a SparkAPI instance with the given Spark session.
  * @param session Spark session to be used
  */
class SparkAPI(session: SparkSession) {

  /**
    * Returns a DataFrame with the data about the repositories found at
    * the specified repositories path in the form of siva files.
    * To call this method you need to have set before the repositories path,
    * you can do so by calling setRepositoriesPath or, preferably, instantiating
    * the SparkAPI using the companion object.
    *
    * {{{
    * val reposDf = api.getRepositories
    * }}}
    *
    * @return DataFrame
    */
  def getRepositories(): DataFrame = getDataSource("repositories", session)

  /**
    * Sets the path where the siva files of the repositories are stored.
    * Although this can actually be called the proper way to use SparkAPI is
    * to instantiate it using the SparkAPI companion object, which already
    * asks for the path in its apply method. If you already instantiated the
    * API instance using the SparkAPI companion object you don't need to call
    * this unless you want to change the repositories path.
    * Note that setting this will affect the session, so any other uses of the
    * session outside the SparkAPI instance will also have that config set.
    *
    * {{{
    * api.setRepositoriesPath("/path/to/repositories")
    * }}}
    *
    * @param path of the siva files.
    * @return instance of the api itself
    */
  def setRepositoriesPath(path: String): SparkAPI = {
    session.sqlContext.setConf(repositoriesPathKey, path)
    this
  }

  /**
    * Configures the endpoint used to connect to the bblfsh GRPC server.
    * Note that setting this will affect the session, so any other uses of the
    * session outside the SparkAPI instance will also have that config set.
    *
    * {{{
    * api.setBblfshGRPCEndpoint("my.grpc.server", 9432)
    * }}}
    *
    * @param host of the GRPC server (0.0.0.0 by default)
    * @param port of the GRPC server (9432 by default)
    * @return instance of the api itself
    */
  def setBblfshGRPCEndpoint(host: String, port: Int): SparkAPI = {
    session.sparkContext.getConf.set(bblfshHostKey, host)
    session.sparkContext.getConf.set(bblfsPortKey, port.toString())
    this
  }

}

/**
  * Factory for [[tech.sourced.api.SparkAPI]] instances.
  */
object SparkAPI {
  /**
    * Creates a new SparkAPI instance with the given Spark session and
    * configures the repositories path for that session.
    *
    * {{{
    * import tech.sourced.api._
    *
    * val api = SparkAPI(sparkSession, "/path/to/repositories")
    * }}}
    *
    * @param session spark session to use
    * @param repositoriesPath the path to the repositories' siva files
    * @return SparkAPI instance
    */
  def apply(session: SparkSession, repositoriesPath: String) = {
    session.registerUDFs()
    new SparkAPI(session)
      .setRepositoriesPath(repositoriesPath)
  }
}