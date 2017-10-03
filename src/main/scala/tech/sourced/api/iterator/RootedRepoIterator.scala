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

  private val repoConfig = repo.getConfig

  private val remotes = repoConfig.getSubsections("remote").asScala

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
    remotes.find(i => i == uuid) match {
      case None => None
      case Some(i) => Some(GitUrlsParser.getIdFromUrls(repoConfig.getStringList("remote", i, "url")))
    }
  }

  protected def getRepositoryUUID(id: String): Option[String] = {
    remotes.find(uuid => {
      val actualId: String =
        GitUrlsParser.getIdFromUrls(repoConfig.getStringList("remote", uuid, "url"))

      actualId == id
    })
  }

  protected def parseRef(ref: String): (String, String) = {
    val split: Array[String] = ref.split("/")
    val uuid: String = split.last
    val repoId: String = this.getRepositoryId(uuid).getOrElse(throw new IllegalArgumentException(s"cannot parse ref $ref"))
    val refName: String = split.init.mkString("/")

    (repoId, refName)
  }
}
