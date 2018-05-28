package tech.sourced.engine.iterator

import java.sql.Timestamp

import org.apache.spark.internal.Logging
import org.eclipse.jgit.api.Git
import org.eclipse.jgit.errors.{
  IncorrectObjectTypeException,
  MissingObjectException,
  RevWalkException
}
import org.eclipse.jgit.lib.{ObjectId, Ref, Repository}
import org.eclipse.jgit.revwalk.RevCommit
import tech.sourced.engine.util.{CompiledFilter, Filters}

import scala.annotation.tailrec
import scala.collection.JavaConverters.iterableAsScalaIterableConverter

/**
  * Iterator that will return rows of commits in a repository.
  *
  * @param finalColumns final columns that must be in the resultant row
  * @param repo         repository to get the data from
  * @param prevIter     previous iterator, if the iterator is chained
  * @param filters      filters for the iterator
  */
class CommitIterator(finalColumns: Array[String],
                     repo: Repository,
                     prevIter: ReferenceIterator,
                     filters: Seq[CompiledFilter],
                     skipReadErrors: Boolean)
  extends ChainableIterator[ReferenceWithCommit](
    finalColumns,
    prevIter,
    filters,
    repo,
    skipReadErrors
  ) {

  /** @inheritdoc */
  override protected def loadIterator(filters: Seq[CompiledFilter]): Iterator[ReferenceWithCommit] =
    CommitIterator.loadIterator(
      repo,
      Option(prevIter).map(_.currentRow),
      Filters(filters)
    )

  /** @inheritdoc*/
  override protected def mapColumns(obj: ReferenceWithCommit): RawRow = {
    val (repoId, refName) = RootedRepo.parseRef(repo, obj.ref.getName)

    val c: RevCommit = obj.commit
    Map[String, Any](
      "repository_id" -> repoId,
      "reference_name" -> refName,
      "index" -> obj.index,
      "hash" -> ObjectId.toString(c.getId),
      "message" -> c.getFullMessage,
      "parents" -> c.getParents.map(p => ObjectId.toString(p.getId)),
      "parents_count" -> c.getParentCount,

      "author_email" -> c.getAuthorIdent.getEmailAddress,
      "author_name" -> c.getAuthorIdent.getName,
      "author_date" -> new Timestamp(c.getAuthorIdent.getWhen.getTime),

      "committer_email" -> c.getCommitterIdent.getEmailAddress,
      "committer_name" -> c.getCommitterIdent.getName,
      "committer_date" -> new Timestamp(c.getCommitterIdent.getWhen.getTime)
    )
  }

}

case class ReferenceWithCommit(ref: Ref, commit: RevCommit, index: Int)

object CommitIterator {

  /**
    * Returns an iterator of references with commit.
    *
    * @param repo    repository to get the data from
    * @param filters filters to skip some rows. "hash" and "index" fields are supported
    *                at iterator level. That means, any "hash" filter passed to this iterator
    *                will make it only retrieve commits with the given hashes. Same for "index".
    * @return the iterator
    */
  def loadIterator(repo: Repository,
                   ref: Option[Ref],
                   filters: Filters,
                   hashKey: String = "hash"): Iterator[ReferenceWithCommit] = {
    val refs = ref match {
      case Some(r) =>
        val (_, refName) = RootedRepo.parseRef(repo, r.getName)
        if (!filters.hasFilters("reference_name")
          || filters.matches(Seq("reference_name"), refName)) {
          Seq(r).toIterator
        } else {
          Seq().toIterator
        }
      case None => ReferenceIterator.loadIterator(
        repo,
        None,
        filters,
        refNameKey = "reference_name"
      )
    }

    val isChained = ref.isDefined
    val hashKeys = Seq("hash", hashKey)
    var iter: Iterator[ReferenceWithCommit] = new RefWithCommitIterator(
      repo,
      refs,
      if (isChained && !filters.hasFilters("index")) 1 else 0
    )

    if (filters.hasFilters(hashKeys: _*)) {
      iter = iter.filter(c => filters.matches(hashKeys, c.commit.getId.getName))
    }

    if (filters.hasFilters("index")) {
      iter = iter.filter(c => filters.matches(Seq("index"), c.index))
    }

    iter
  }

}

/**
  * Iterator that will return references with their commit and the commit index in the reference.
  *
  * @param repo       repository to get the data from
  * @param refs       iterator of references
  * @param maxResults max results to return
  */
private[iterator] class RefWithCommitIterator(repo: Repository,
                                              refs: Iterator[Ref],
                                              maxResults: Int = 0
                                             ) extends Iterator[ReferenceWithCommit] with Logging {

  private var actualRef: Ref = _
  private var commits: Iterator[RevCommit] = _
  private var index: Int = 0
  private var nextResult: ReferenceWithCommit = _
  private var consumed: Int = 0

  /** @inheritdoc*/
  final override def hasNext: Boolean = {
    // do not advance the iterator until the next result has been consumed
    if (nextResult != null) {
      return true
    }

    while ((commits == null || !commits.hasNext) && refs.hasNext) {
      actualRef = refs.next()
      index = 0
      consumed = 0
      commits = Git.wrap(repo).log()
        .add(Option(actualRef.getPeeledObjectId).getOrElse(actualRef.getObjectId))
        .call().asScala.toIterator
    }


    if (maxResults > 0 && consumed == maxResults) {
      false
    } else if (refs.hasNext || (commits != null && commits.hasNext)) {
      nextResult = ReferenceWithCommit(actualRef, commits.next(), index)
      index += 1
      true
    } else {
      false
    }
  }

  /**
    * Returns the next [[ReferenceWithCommit]]. It should never be called after
    * hasNext has returned "false", since this can lead to incorrect results.
    * It can only be called after a call to hasNext. Repeatedly calling this method
    * will result in an error.
    *
    * @return a reference with commit
    */
  override def next(): ReferenceWithCommit = {
    val result = nextResult
    nextResult = null
    consumed += 1
    result
  }
}
