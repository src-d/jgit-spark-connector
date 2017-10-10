package tech.sourced.api.udf

import java.nio.charset.StandardCharsets

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import org.bblfsh.client.BblfshClient
import tech.sourced.api.util.Bblfsh

/**
  * User defined function to extract UASTs from a row.
  */
object ExtractUASTsUDF extends CustomUDF {

  val name = "extractUASTs"

  def function(session: SparkSession): UserDefinedFunction = {
    val config = Bblfsh.getConfig(session.sparkContext)
    val configB = session.sparkContext.broadcast(config)
    udf[Array[Byte], String, Array[Byte]]((path, content) =>
      extractUASTs(path, content, configB.value))
  }

  def functionWithLang(session: SparkSession): UserDefinedFunction = {
    val config = Bblfsh.getConfig(session.sparkContext)
    val configB = session.sparkContext.broadcast(config)
    udf[Array[Byte], String, Array[Byte], String]((path, content, lang) =>
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
    * a byte array with the resultant UAST.
    *
    * @param path    File path
    * @param content File content
    * @param config  bblfsh config
    * @return Byte array with the UAST
    */
  def extractUASTs(path: String, content: Array[Byte], config: Bblfsh.Config): Array[Byte] = {
    extractUAST(path, content, "", config)
  }

  /**
    * Extracts the UAST of the file with the given path, language and content and returns
    * a byte array with the resultant UAST.
    *
    * @param path    File path
    * @param content File content
    * @param lang    File language
    * @param config  Bblfsh config
    * @return Byte array with the UAST
    */
  def extractUASTsWithLang(path: String,
                           content: Array[Byte],
                           lang: String, config:
                           Bblfsh.Config): Array[Byte] = {
    extractUAST(path, content, lang, config)
  }

  /**
    * Extracts the UAST of the file with the given path, language and content and returns
    * a byte array with the resultant UAST.
    *
    * @param path    File path
    * @param content File content
    * @param lang    File language
    * @param config  Bblfsh config
    * @return Byte array with the UAST
    */
  private def extractUAST(path: String,
                          content: Array[Byte],
                          lang: String,
                          config: Bblfsh.Config): Array[Byte] =
    if (null == content || content.isEmpty) {
      Array.emptyByteArray
    } else if (lang != null && excludedLangs.contains(lang.toLowerCase)) {
      Array.emptyByteArray
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
    * @return Array of bytes with the UAST
    */
  def extractUsingBblfsh(bblfshClient: BblfshClient,
                         path: String,
                         content: Array[Byte],
                         lang: String): Array[Byte] = {
    //FIXME(bzz): not everything is UTF-8 encoded :/
    val contentStr = new String(content, StandardCharsets.UTF_8)
    val parsed = bblfshClient.parse(path, content = contentStr, lang = lang)
    if (parsed.errors.isEmpty) {
      parsed.uast.get.toByteArray
    } else {
      Array.emptyByteArray
    }
  }

}
