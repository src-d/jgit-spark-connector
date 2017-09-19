package tech.sourced.api.udf

import java.nio.charset.StandardCharsets

import org.apache.spark.sql.functions.udf
import org.bblfsh.client.BblfshClient

object extractUASTsUDF extends CustomUDF {
  val name = "extractUASTs"
  val function = udf[Array[Byte], String, Array[Byte], String](extractUAST)

  val excludedLangs = Set("markdown", "text", "")
  val bblfshHost = "0.0.0.0"
  val bblfshPort = 9432

  def extractUAST(path: String, content: Array[Byte], lang: String = ""): Array[Byte] = {
    if (excludedLangs.contains(lang)) {
      return Array.emptyByteArray
    }
    val bblfshClient = BblfshClient(bblfshHost, bblfshPort)
    extractUsingBblfsh(bblfshClient, path, content, lang)
  }

  def extractUsingBblfsh(bblfshClient: BblfshClient, path: String, content: Array[Byte], lang: String): Array[Byte] = {
    //FIXME(bzz): not everything is UTF-8 encoded :/
    val parsed = bblfshClient.parse(path, lang, new String(content, StandardCharsets.UTF_8))
    if (parsed.errors.isEmpty) {
      parsed.uast.get.toByteArray
    } else {
      Array.emptyByteArray
    }
  }

}
