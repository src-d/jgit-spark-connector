package tech.sourced.api.udf

import java.nio.charset.StandardCharsets

import org.bblfsh.client.BblfshClient

object BblfshUDF {

  val bblfshHost = "0.0.0.0"
  val bblfshPort = 9432

  def extractUAST: (String, Array[Byte]) => Array[Byte] = { (path, content) =>
    val bblfshClient = BblfshClient(bblfshHost, bblfshPort)
    extractUsingBblfsh(bblfshClient, path, content)
  }

  def extractUsingBblfsh(bblfshClient: BblfshClient, path: String, content: Array[Byte]): Array[Byte] = {
    //FIXME(bzz): not everything is UTF-8 encoded :/
    val parsed = bblfshClient.parse(path, new String(content, StandardCharsets.UTF_8))
    if (parsed.errors.isEmpty) {
      parsed.uast.get.toByteArray
    } else {
      Array.emptyByteArray
    }
  }


}
