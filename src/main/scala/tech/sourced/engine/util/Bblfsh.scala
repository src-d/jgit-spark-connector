package tech.sourced.engine.util

import java.nio.charset.StandardCharsets

import gopkg.in.bblfsh.sdk.v1.protocol.generated.Status
import gopkg.in.bblfsh.sdk.v1.uast.generated.Node
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.bblfsh.client.BblfshClient
import tech.sourced.engine.util.Bblfsh.client

object Bblfsh extends Logging {

  case class Config(host: String, port: Int)

  /** Key used for the option to specify the host of the bblfsh grpc service. */
  val hostKey = "spark.tech.sourced.bblfsh.grpc.host"

  /** Key used for the option to specify the port of the bblfsh grpc service. */
  val portKey = "spark.tech.sourced.bblfsh.grpc.port"

  /** Default bblfsh host. */
  val defaultHost = "0.0.0.0"

  /** Default bblfsh port. */
  val defaultPort = 9432

  var supportedLanguages: Set[String] = Set()

  private var config: Config = _
  private var client: BblfshClient = _

  /**
    * Returns the configuration for bblfsh.
    *
    * @param session Spark session
    * @return bblfsh configuration
    */
  def getConfig(session: SparkSession): Config = {
    if (config == null) {
      val host = session.conf.get(hostKey, Bblfsh.defaultHost)
      val port = session.conf.get(portKey, Bblfsh.defaultPort.toString).toInt
      config = Config(host, port)
    }

    config
  }

  private def getClient(config: Config): BblfshClient = synchronized {
    if (client == null) {
      client = BblfshClient(config.host, config.port)
    }

    client
  }

  private def getSupportedLanguages(config: Config): Set[String] = synchronized {
    if (supportedLanguages.isEmpty) {
      val client = getClient(config)
      supportedLanguages = client.supportedLanguages()
        .languages.map(m => m.language)
        .toSet
    }

    supportedLanguages
  }

  private def shouldExtractLanguage(config: Config, lang: String): Boolean = {
    val supportedLanguages = getSupportedLanguages(config)
    supportedLanguages.contains(lang.toLowerCase())
  }

  /**
    * Extracts the UAST using bblfsh.
    *
    * @param path    File path
    * @param content File content
    * @param lang    File language
    * @param config bblfsh configuration
    * @return List of uast nodes binary-encoded as a byte array
    */
  def extractUAST(path: String,
                  content: Array[Byte],
                  lang: String,
                  config: Config): Seq[Array[Byte]] = {
    //FIXME(bzz): not everything is UTF-8 encoded :/
    // if lang == null, it hasn't been classified yet
    // so rely on bblfsh to guess this file's language
    if (lang != null && !shouldExtractLanguage(config, lang)) {
      Seq()
    } else {
      val client = getClient(config)
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
    * Filter an UAST node using the given query.
    *
    * @param node An UAST node
    * @param query XPath expression
    * @param config bblfsh configuration
    * @return UAST list of filtered nodes
    */
  def filter(node: Node, query: String, config: Config): List[Node] = {
    getClient(config).filter(node, query)
  }

}
