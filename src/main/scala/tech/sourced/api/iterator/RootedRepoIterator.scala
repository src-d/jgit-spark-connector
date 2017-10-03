package tech.sourced.api.iterator

import org.apache.spark.sql.Row
import org.eclipse.jgit.lib.{Repository, StoredConfig}
import tech.sourced.api.util.GitUrlsParser

import scala.collection.JavaConverters.collectionAsScalaIterableConverter

/**
  * Implements an iterator that iterates over rooted repositories, which are repositories
  * that are grouped by common roots (commits without parents).
  * It's meant to be extended by each specific iterator.
  * TODO: implement filter logic
  *
  * @param requiredColumns required columns for the returned row
  * @param repo            Git repository
  * @tparam T type of the internal iterator
  */
abstract class RootedRepoIterator[T](requiredColumns: Array[String],
                                     repo: Repository) extends Iterator[Row] {

  /**
    * Internal iterator of the specific type.
    */
  private var internalIterator: Iterator[T] = _

  /**
    * Loads the internal iterator.
    *
    * @return loaded internal iterator
    */
  protected def loadIterator(): Iterator[T]

  /**
    * Given an object of type T returns a map from column keys to a function that will return
    * the value of the given column.
    *
    * @param obj object to get the columns from
    * @return map from columns to value providers
    */
  protected def mapColumns(obj: T): Map[String, () => Any]

  private val repoConfig = repo.getConfig

  private val remotes = repoConfig.getSubsections("remote").asScala

  /**
    * @inheritdoc
    */
  override def hasNext: Boolean = {
    if (internalIterator == null) {
      internalIterator = loadIterator()
    }

    internalIterator.hasNext
  }

  /**
    * @inheritdoc
    */
  override def next(): Row = {
    val mappedValues: Map[String, () => Any] = mapColumns(internalIterator.next())
    Row(requiredColumns.map(c => mappedValues(c)()): _*)
  }

  /**
    * Returns the repository ID given its UUID.
    *
    * @param uuid the UUID of the repository
    * @return the ID of the repository, None if it can't be found
    */
  protected def getRepositoryId(uuid: String): Option[String] = {
    remotes.find(i => i == uuid) match {
      case None => None
      case Some(i) => Some(GitUrlsParser.getIdFromUrls(
        repoConfig.getStringList("remote", i, "url")
      ))
    }
  }

  /**
    * Returns the repository UUID given its ID.
    *
    * @param id id of the repository
    * @return the UUID of the repository, None if it can't be found
    */
  protected def getRepositoryUUID(id: String): Option[String] = {
    remotes.find(uuid => {
      val actualId: String =
        GitUrlsParser.getIdFromUrls(repoConfig.getStringList("remote", uuid, "url"))

      actualId == id
    })
  }

  /**
    * Parses a reference name to get from it the actual name and the repository ID to which
    * it belongs.
    *
    * @param ref Reference name
    * @return Tuple containing the repository ID and the reference name
    */
  protected def parseRef(ref: String): (String, String) = {
    val split: Array[String] = ref.split("/")
    val uuid: String = split.last
    val repoId: String = this.getRepositoryId(uuid)
      .getOrElse(throw new IllegalArgumentException(s"cannot parse ref $ref"))
    val refName: String = split.init.mkString("/")

    (repoId, refName)
  }
}
