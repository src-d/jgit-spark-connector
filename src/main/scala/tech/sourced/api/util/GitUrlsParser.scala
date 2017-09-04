package tech.sourced.api.util

import java.net.{URI, URISyntaxException}

import scala.util.matching.Regex

object GitUrlsParser {
  val isGit: Regex = """(.+)\\@(.+):(.+)\\.git""".r

  def getIdFromUrls(urls: Array[String]): String = {
    urls.flatMap({
      case isGit(_, host, path, _*) => Some(host + path)
      case s => try {
        val u: URI = new URI(s)
        Some(u.getHost + u.getPath)
      } catch {
        case _: URISyntaxException => None
      }
    }).distinct.min
  }

}
