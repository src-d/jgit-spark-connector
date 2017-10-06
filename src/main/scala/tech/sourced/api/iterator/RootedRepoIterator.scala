package tech.sourced.api.iterator

import org.apache.spark.sql.Row
import org.eclipse.jgit.lib.{Repository, StoredConfig}
import tech.sourced.api.util.{CompiledFilter, GitUrlsParser}

import scala.collection.JavaConverters.collectionAsScalaIterableConverter

/**
  * Common functionality shared by all git relation iterators. A RootedRepoIterator
  * is an iterator that generates rows of a certain data type extracted from a rooted
  * repository.
  * Multiple RootedRepoIterators can be chained to use the result of the previous iterator
  * in order to have a better performance and compute less data.
  *
  * @param finalColumns final columns that must be in the resultant row
  * @param repo         repository to get the data from
  * @param prevIter     previous iterator, if the iterator is chained
  * @param filters      filters for the iterator
  * @tparam T type of data returned by the internal iterator
  */
abstract class RootedRepoIterator[T](finalColumns: Array[String],
                                     repo: Repository,
                                     prevIter: RootedRepoIterator[_],
                                     filters: Seq[CompiledFilter]) extends Iterator[Row] {

  /** Raw values of the row. */
  type RawRow = Map[String, () => Any]

  /** Instance of the internal iterator. */
  private var iter: Iterator[T] = _

  /** The current row of the prevIter, null always if there is no prevIter. */
  private var currentRow: RawRow = _

  /**
    * Returns the internal iterator that will return the data used to construct the final row.
    *
    * @param filters filters for the iterator
    * @return internal iterator
    */
  protected def loadIterator(filters: Seq[CompiledFilter]): Iterator[T]

  /**
    * Loads the next internal iterator.
    *
    * @return internal iterator
    */
  private def loadIterator = loadIterator(getFilters(currentRow))

  /**
    * Returns the filters to be given to loadIterator when it's called. It's meant
    * to be overridden for iterators that need to pass extra filters depending on the
    * current row of the previous iterator.
    *
    * @return
    */
  protected def getFilters(currentRow: RawRow): Seq[CompiledFilter] = filters

  /**
    * Given the object returned by the internal iterator, this method must transform
    * that object into a RawRow.
    *
    * @param obj object returned by the internal iterator
    * @return raw row
    */
  protected def mapColumns(obj: T): RawRow

  private val repoConfig = repo.getConfig

  private val remotes = repoConfig.getSubsections("remote").asScala

  /**
    * @inheritdoc
    */
  override def hasNext: Boolean = {
    if (prevIter == null) {
      if (iter == null) {
        iter = loadIterator
      }

      iter.hasNext
    } else if (iter == null) {
      if (prevIter != null && !prevIter.hasNext) {
        return false
      }

      currentRow = prevIter.nextRaw
      iter = loadIterator
      iter.hasNext
    } else if (!iter.hasNext) {
      if (prevIter != null && !prevIter.hasNext) {
        return false
      }

      currentRow = prevIter.nextRaw
      iter = loadIterator
      iter.hasNext
    } else {
      true
    }
  }

  override def next: Row = {
    val mappedValues = if (currentRow != null) {
      currentRow ++ mapColumns(iter.next)
    } else {
      mapColumns(iter.next)
    }

    val values = finalColumns.map(c => mappedValues(c)())
    Row(values: _*)
  }


  def nextRaw: RawRow = {
    val row = mapColumns(iter.next())
    if (currentRow != null) {
      currentRow ++ row
    } else {
      row
    }
  }
}

object RootedRepo {

  /**
    * Returns the ID of a repository given its UUID.
    *
    * @param repo repository
    * @param uuid repository UUID
    * @return repository ID
    */
  private[iterator] def getRepositoryId(repo: Repository, uuid: String): Option[String] = {
    // TODO: maybe a cache here could improve performance
    val c: StoredConfig = repo.getConfig
    c.getSubsections("remote").asScala.find(i => i == uuid) match {
      case None => None
      case Some(i) => Some(GitUrlsParser.getIdFromUrls(
        c.getStringList("remote", i, "url")
      ))
    }
  }

  /**
    * Returns the UUID of a repository given its ID.
    *
    * @param repo repository
    * @param id   repository id
    * @return UUID of the repo
    */
  private[iterator] def getRepositoryUUID(repo: Repository, id: String): Option[String] = {
    // TODO: maybe a cache here could improve performance
    val c: StoredConfig = repo.getConfig
    c.getSubsections("remote").asScala.find(uuid => {
      val actualId: String =
        GitUrlsParser.getIdFromUrls(c.getStringList("remote", uuid, "url"))

      actualId == id
    })
  }

  /**
    * Parses a reference name and returns a tuple with the repository id and the reference name.
    *
    * @param repo repository
    * @param ref  reference name
    * @return tuple with repository id and reference name
    */
  private[iterator] def parseRef(repo: Repository, ref: String): (String, String) = {
    val split: Array[String] = ref.split("/")
    val uuid: String = split.last
    val repoId: String = getRepositoryId(repo, uuid).get
    val refName: String = split.init.mkString("/")

    (repoId, refName)
  }

}
