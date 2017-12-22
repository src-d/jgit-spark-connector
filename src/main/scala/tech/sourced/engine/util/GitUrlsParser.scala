package tech.sourced.engine.util

import java.net.{URI, URISyntaxException}

object GitUrlsParser {
  private val isGit = """(.+)\@(.+):(.+)\.git""".r

  /**
    * Retrieves the URL that will act as identifier in a list of URLs
    * for a repository.
    *
    * @param urls array of urls
    * @return processed id
    */
  def getIdFromUrls(urls: Array[String]): String = {
    urls.flatMap({
      case isGit(_, host, path, _*) =>
        Some(s"$host/$path")
      case s => try {
        val u: URI = new URI(s)
        Some(u.getHost + u.getPath)
      } catch {
        case _: URISyntaxException => None
      }
    }).distinct.min
  }

}
