package tech.sourced.engine.udf

import java.nio.charset.StandardCharsets

import gopkg.in.bblfsh.sdk.v1.protocol.generated.Status
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import org.bblfsh.client.BblfshClient
import tech.sourced.engine.util.Bblfsh

/**
  * User defined function to extract UASTs from a row.
  */
object ExtractUASTsUDF extends CustomUDF {

  val name = "extractUASTs"

  def function(session: SparkSession): UserDefinedFunction = {
    val config = Bblfsh.getConfig(session.sparkContext)
    val configB = session.sparkContext.broadcast(config)
    udf[Seq[Array[Byte]], String, Array[Byte]]((path, content) =>
      extractUASTs(path, content, configB.value))
  }

  def functionWithLang(session: SparkSession): UserDefinedFunction = {
    val config = Bblfsh.getConfig(session.sparkContext)
    val configB = session.sparkContext.broadcast(config)
    udf[Seq[Array[Byte]], String, Array[Byte], String]((path, content, lang) =>
      extractUASTsWithLang(path, content, lang, configB.value))
  }

  /** Languages whose UAST will not be retrieved. */
  val excludedLangs = Set("markdown", "text")

  /** Default bblfsh GRPC host. */
  var bblfshHost = "0.0.0.0"

  /** Default bblfsh GRPC port. */
  var bblfshPort = 9432

  /**
    * Extracts the UAST of the file with the given path and content and returns
    * a list of uast nodes binary-encoded as a byte array
    *
    * @param path    File path
    * @param content File content
    * @param config  bblfsh config
    * @return List of uast nodes binary-encoded as a byte array
    */
  def extractUASTs(path: String, content: Array[Byte], config: Bblfsh.Config): Seq[Array[Byte]] = {
    extractUAST(path, content, "", config)
  }

  /**
    * Extracts the UAST of the file with the given path, language and content and returns
    * a list of uast nodes binary-encoded as a byte array
    *
    * @param path    File path
    * @param content File content
    * @param lang    File language
    * @param config  Bblfsh config
    * @return List of uast nodes binary-encoded as a byte array
    */
  def extractUASTsWithLang(path: String,
                           content: Array[Byte],
                           lang: String, config:
                           Bblfsh.Config): Seq[Array[Byte]] = {
    extractUAST(path, content, lang, config)
  }

  /**
    * Extracts the UAST of the file with the given path, language and content and returns
    * a list of uast nodes binary-encoded as a byte array.
    *
    * @param path    File path
    * @param content File content
    * @param lang    File language
    * @param config  Bblfsh config
    * @return List of uast nodes binary-encoded as a byte array
    */
  private def extractUAST(path: String,
                          content: Array[Byte],
                          lang: String,
                          config: Bblfsh.Config): Seq[Array[Byte]] =
    if (null == content || content.isEmpty) {
      Seq()
    } else if (lang != null && excludedLangs.contains(lang.toLowerCase)) {
      Seq()
    } else {
      val bblfshClient = Bblfsh.getClient(config)
      extractUsingBblfsh(bblfshClient, path, content, lang)
    }

  /**
    * Extracts the UAST using bblfsh.
    *
    * @param bblfshClient bblfsh client to use
    * @param path         File path
    * @param content      File content
    * @param lang         File language
    * @return List of uast nodes binary-encoded as a byte array
    */
  def extractUsingBblfsh(bblfshClient: BblfshClient,
                         path: String,
                         content: Array[Byte],
                         lang: String): Seq[Array[Byte]] = {
    //FIXME(bzz): not everything is UTF-8 encoded :/
    val contentStr = new String(content, StandardCharsets.UTF_8)
    val parsed = bblfshClient.parse(path, content = contentStr, lang = lang)
    if (parsed.status == Status.OK) {
      Seq(parsed.uast.get.toByteArray)
    } else {
      Seq()
    }
  }

}
