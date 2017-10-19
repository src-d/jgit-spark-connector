package tech.sourced.engine.util

import org.apache.spark.SparkContext
import org.bblfsh.client.BblfshClient

object Bblfsh {

  case class Config(host: String, port: Int) extends Serializable

  /**
    * Key used for the option to specify the host of the bblfsh grpc service.
    */
  val hostKey = "spark.tech.sourced.bblfsh.grpc.host"

  /**
    * Key used for the option to specify the port of the bblfsh grpc service.
    */
  val portKey = "spark.tech.sourced.bblfsh.grpc.port"

  private val defaultPort = 9432
  private val defaultHost = "0.0.0.0"

  /**
    * Returns the configuration for babelfish.
    *
    * @param ctx spark context
    * @return bblfsh configuration
    */
  def getConfig(ctx: SparkContext): Config = {
    val port = ctx.getConf.getInt(portKey, defaultPort)
    val host = ctx.getConf.get(hostKey, defaultHost)
    Config(host, port)
  }

  /**
    * Returns a client for a given bblfsh configuration.
    *
    * @param config configuration for bblfsh
    * @return client
    */
  def getClient(config: Config): BblfshClient = BblfshClient(config.host, config.port)

}
