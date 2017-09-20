package tech.sourced.api

import org.apache.spark.sql.{DataFrame, SparkSession}

class SparkAPI(session: SparkSession) {

  def getRepositories(): DataFrame = getDataSource("repositories", session)

  def setRepositoriesPath(path: String): SparkAPI = {
    session.sqlContext.setConf(repositoriesPathKey, path)
    this
  }

}

object SparkAPI {
  def apply(session: SparkSession, repositoriesPath: String) = {
    session.registerUDFs()
    new SparkAPI(session)
      .setRepositoriesPath(repositoriesPath)
  }
}