package tech.sourced.api.iterator

import org.apache.spark.sql.Row
import org.eclipse.jgit.lib.{Repository, StoredConfig}
import tech.sourced.api.util.GitUrlsParser

import scala.collection.JavaConverters.collectionAsScalaIterableConverter

// TODO implement filter logic
abstract class RootedRepoIterator[T](requiredColumns: Array[String], repo: Repository) extends Iterator[Row] {

  private var internalIterator: Iterator[T] = _

  protected def loadIterator(): Iterator[T]

  protected def mapColumns(obj: T): Map[String, () => Any]

  override def hasNext: Boolean = {
    if (internalIterator == null) {
      internalIterator = loadIterator()
    }

    internalIterator.hasNext
  }

  override def next(): Row = {
    val mappedValues: Map[String, () => Any] = mapColumns(internalIterator.next())
    Row(requiredColumns.map(c => mappedValues(c)()): _*)
  }

  protected def getRepositoryId(uuid: String): Option[String] = {
    // TODO maybe a cache here could improve performance
    val c: StoredConfig = repo.getConfig
    c.getSubsections("remote").asScala.find(i => i == uuid) match {
      case None => None
      case Some(i) => Some(GitUrlsParser.getIdFromUrls(c.getStringList("remote", i, "url")))
    }
  }

  protected def getRepositoryUUID(id: String): Option[String] = {
    // TODO maybe a cache here could improve performance
    val c: StoredConfig = repo.getConfig
    c.getSubsections("remote").asScala.find(uuid => {
      val actualId: String =
        GitUrlsParser.getIdFromUrls(c.getStringList("remote", uuid, "url"))

      actualId == id
    })
  }
}
