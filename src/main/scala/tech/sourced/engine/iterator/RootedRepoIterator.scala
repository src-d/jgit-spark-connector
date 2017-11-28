package tech.sourced.engine.iterator

import java.util.UUID

import org.apache.spark.sql.Row
import org.eclipse.jgit.lib.{Repository, StoredConfig}
import tech.sourced.engine.util.{CompiledFilter, GitUrlsParser}

import scala.annotation.tailrec
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
  private var prevIterCurrentRow: RawRow = _

  /** The current row of the internal iterator. */
  private[iterator] var currentRow: T = _

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
  private def loadIterator: Iterator[T] = loadIterator(filters)

  /**
    * Given the object returned by the internal iterator, this method must transform
    * that object into a RawRow.
    *
    * @param obj object returned by the internal iterator
    * @return raw row
    */
  protected def mapColumns(obj: T): RawRow

  //final private def isEmpty: Boolean = !hasNext

  /**
    * @inheritdoc
    */
  @tailrec
  final override def hasNext: Boolean = {
    // If there is no previous iter just load the iterator the first pass
    // and use hasNext of iter all the times. We return here to get rid of
    // this logic and assume from this point on that prevIter is not null
    if (prevIter == null) {
      if (iter == null) {
        iter = loadIterator
      }

      return iter.hasNext
    }

    // If the iter is not loaded, do so, but only if there are actually more
    // rows in the prev iter. If there are, just load the iter and preload
    // the prevIterCurrentRow.
    if (iter == null) {
      if (prevIter.isEmpty) {
        return false
      }

      prevIterCurrentRow = prevIter.nextRaw
      iter = loadIterator
    }

    // if iter is empty, we need to check if there are more rows in the prev iter
    // if not, just finish. If there are, preload the next raw row of the prev iter
    // and load the iterator again for the prev iter current row
    iter.hasNext || {
      if (prevIter.isEmpty) {
        return false
      }

      prevIterCurrentRow = prevIter.nextRaw
      iter = loadIterator

      // recursively check if it has more items, maybe there are no results for
      // this prevIter row but there are for the next
      hasNext
    }
  }

  override def next: Row = {
    currentRow = iter.next
    val mappedValues = if (prevIterCurrentRow != null) {
      prevIterCurrentRow ++ mapColumns(currentRow)
    } else {
      mapColumns(currentRow)
    }

    val values = finalColumns.map(c => mappedValues(c)())
    Row(values: _*)
  }


  def nextRaw: RawRow = {
    currentRow = iter.next
    val row = mapColumns(currentRow)
    if (prevIterCurrentRow != null) {
      prevIterCurrentRow ++ row
    } else {
      row
    }
  }
}

object RootedRepo {

  /**
    * Returns the ID of a repository given its remote name.
    *
    * @param repo       repository
    * @param remoteName remote name
    * @return repository ID
    */
  private[iterator] def getRepositoryId(repo: Repository, remoteName: String): Option[String] = {
    // TODO: maybe a cache here could improve performance
    val c: StoredConfig = repo.getConfig
    c.getSubsections("remote").asScala.find(_ == remoteName) match {
      case None => None
      case Some(name) => Some(GitUrlsParser.getIdFromUrls(
        c.getStringList("remote", name, "url")
      ))
    }
  }

  /**
    * Returns the remote name of a repository with the given ID.
    *
    * @param repo repository
    * @param id   repository id
    * @return remote name
    */
  private[iterator] def getRepositoryRemote(repo: Repository, id: String): Option[String] = {
    // TODO: maybe a cache here could improve performance
    val c: StoredConfig = repo.getConfig
    c.getSubsections("remote").asScala.find(remoteName => {
      val actualId: String =
        GitUrlsParser.getIdFromUrls(c.getStringList("remote", remoteName, "url"))

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

    // if it's a siva file, the last part will be the uuid of the repository, which
    // is the name of the remote associated to that particular repository
    getRepositoryId(repo, uuid) match {
      case Some(repoId) =>
        val refName: String = split.init.mkString("/")

        (repoId, refName)

      // If no uuid matches, it means this is not a siva file, so we should find this
      // using the whole reference name
      case None =>
        val c: StoredConfig = repo.getConfig
        val refRemote = repo.getRemoteName(ref)
        val repoId = c.getSubsections("remote").asScala
          .find(_ == refRemote)
          .map(r => GitUrlsParser.getIdFromUrls(c.getStringList("remote", r, "url")))
          .orNull

        if (repoId == null) {
          // if branch is local, use the repo path as directory
          // since there's no way to tell to which remote it belongs (probably none)
          val repoPath = if (repo.getDirectory.toPath.getFileName.toString == ".git") {
            // non-bare repositories will have the .git directory as their directory
            // so we'll use the parent
            repo.getDirectory.toPath.getParent
          } else {
            repo.getDirectory.toPath
          }

          ("file://" + repoPath, ref)
        } else {
          (repoId, ref)
        }
    }
  }

}
