package tech.sourced.api.udf

import java.nio.charset.StandardCharsets

import org.apache.spark.sql.functions.udf
import org.bblfsh.client.BblfshClient

object ExtractUASTsUDF extends CustomUDF {
  val name = "extractUASTs"
  val function = udf[Array[Byte], String, Array[Byte]](extractUASTs)
  val functionMoreArgs = udf[Array[Byte], String, Array[Byte], String](extractUASTsWithLang)

  val excludedLangs = Set("markdown", "text")

  var bblfshHost = "0.0.0.0"
  var bblfshPort = 9432

  def extractUASTs(path: String, content: Array[Byte]): Array[Byte] = {
    extractUAST(path, content, "")
  }

  def extractUASTsWithLang(path: String, content: Array[Byte], lang: String): Array[Byte] = {
    extractUAST(path, content, lang)
  }

  def extractUAST(path: String, content: Array[Byte], lang: String): Array[Byte] =
    if (null == content || content.isEmpty) {
      Array.emptyByteArray
    } else if (lang != null && excludedLangs.contains(lang.toLowerCase)) {
      Array.emptyByteArray
    } else {
      val bblfshClient = BblfshClient(bblfshHost, bblfshPort)
      extractUsingBblfsh(bblfshClient, path, content, lang)
    }

  def extractUsingBblfsh(bblfshClient: BblfshClient, path: String, content: Array[Byte], lang: String): Array[Byte] = {
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
