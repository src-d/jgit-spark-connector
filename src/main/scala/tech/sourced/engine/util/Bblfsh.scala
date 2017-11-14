package tech.sourced.engine.util

import java.nio.charset.StandardCharsets

import gopkg.in.bblfsh.sdk.v1.protocol.generated.Status
import gopkg.in.bblfsh.sdk.v1.uast.generated.Node
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.bblfsh.client.BblfshClient

object Bblfsh extends Logging {

  case class Config(host: String, port: Int)

  /**
    *  Languages whose UAST will not be retrieved.
    */
  val excludedLangs = Set("markdown", "text")

  /**
    * Key used for the option to specify the host of the bblfsh grpc service.
    */
  val hostKey = "spark.tech.sourced.bblfsh.grpc.host"

  /**
    * Key used for the option to specify the port of the bblfsh grpc service.
    */
  val portKey = "spark.tech.sourced.bblfsh.grpc.port"

  private val defaultPort = "9432"
  private val defaultHost = "0.0.0.0"

  private var config: Config = _
  private var client: BblfshClient = _

  /**
    * Set the host and port that will be used to connect to the bblfsh server,
    * retrieving these parameter from the SparkSession configuration.
    *
    * @param session
    */
  def setConfig(session: SparkSession): Unit = {
    val port = session.conf.get(portKey, defaultPort).toInt
    val host = session.conf.get(hostKey, defaultHost)
    config = Config(host, port)
  }

  /**
    * Returns the configuration for babelfish.
    *
    * @return bblfsh Configuration
    */
  def getConfig(): Config = {
    if (config == null) {
      Config(defaultHost, defaultPort.toInt)
    } else {
      config
    }
  }

  private def getClient(): BblfshClient = synchronized {
    if (client == null) {
      val Config(host, port) = getConfig()
      client = BblfshClient(host, port)
    }

    client
  }

  /**
    * Extracts the UAST using bblfsh.
    *
    * @param path    File path
    * @param content File content
    * @param lang    File language
    * @return List of uast nodes binary-encoded as a byte array
    */
  def extractUAST(path: String,
                  content: Array[Byte],
                  lang: String): Seq[Array[Byte]] = {

    //FIXME(bzz): not everything is UTF-8 encoded :/

    // if lang == null, it hasn't been classified yet
    // so rely on bblfsh to guess this file's language
    if (lang != null && excludedLangs.contains(lang.toLowerCase())) {
      Seq()
    } else {
      val client = getClient()
      val contentStr = new String(content, StandardCharsets.UTF_8)
      val parsed = client.parse(path, content = contentStr, lang = lang)
      if (parsed.status == Status.OK) {
        Seq(parsed.uast.get.toByteArray)
      } else {
        logWarning(s"${parsed.status} $path: ${parsed.errors.mkString("; ")}")
        Seq()
      }
    }
  }

  /**
    * Filter an UAST node using the given query
    *
    * @param node
    * @param query
    * @return
    */
  def filter(node: Node, query: String): List[Node] = {
    getClient().filter(node, query)
  }

}
