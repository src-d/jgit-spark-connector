package tech.sourced.engine.iterator

import java.sql.Timestamp

import org.eclipse.jgit.api.Git
import org.eclipse.jgit.errors.IncorrectObjectTypeException
import org.eclipse.jgit.lib.{ObjectId, Ref, Repository}
import org.eclipse.jgit.revwalk.RevCommit
import org.eclipse.jgit.treewalk.TreeWalk
import tech.sourced.engine.util.{CompiledFilter, Filter}

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
                     filters: Seq[CompiledFilter])
  extends RootedRepoIterator[ReferenceWithCommit](finalColumns, repo, prevIter, filters) {

  /** @inheritdoc */
  override protected def loadIterator(filters: Seq[CompiledFilter]): Iterator[ReferenceWithCommit] =
    CommitIterator.loadIterator(
      repo,
      Option(prevIter).map(_.currentRow),
      filters.flatMap(_.matchingCases)
    )

  /** @inheritdoc*/
  override protected def mapColumns(obj: ReferenceWithCommit): Map[String, () => Any] = {
    val (repoId, refName) = RootedRepo.parseRef(repo, obj.ref.getName)

    val c: RevCommit = obj.commit
    Map[String, () => Any](
      "repository_id" -> (() => repoId),
      "reference_name" -> (() => refName),
      "index" -> (() => obj.index),
      "hash" -> (() => ObjectId.toString(c.getId)),
      "message" -> (() => c.getFullMessage),
      "parents" -> (() => c.getParents.map(p => ObjectId.toString(p.getId))),
      "parents_count" -> (() => c.getParentCount),

      "author_email" -> (() => c.getAuthorIdent.getEmailAddress),
      "author_name" -> (() => c.getAuthorIdent.getName),
      "author_date" -> (() => new Timestamp(c.getAuthorIdent.getWhen.getTime)),

      "committer_email" -> (() => c.getCommitterIdent.getEmailAddress),
      "committer_name" -> (() => c.getCommitterIdent.getName),
      "committer_date" -> (() => new Timestamp(c.getCommitterIdent.getWhen.getTime))
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
                   filters: Seq[Filter.Match],
                   hashKey: String = "hash"): Iterator[ReferenceWithCommit] = {
    val refs = ref match {
      case Some(r) =>
        val filterRefs = filters.flatMap {
          case ("reference_name", references) => references.map(_.toString)
          case _ => Seq()
        }

        val (_, refName) = RootedRepo.parseRef(repo, r.getName)
        if (filterRefs.isEmpty || filterRefs.contains(refName)) {
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

    val indexes = filters.flatMap {
      case ("index", idx) => idx.map(_.asInstanceOf[Int])
      case _ => Seq()
    }

    val hashes = filters.flatMap {
      case (k, h) if k == hashKey => h.map(_.toString)
      case ("hash", h) => h.map(_.toString)
      case _ => Seq()
    }

    var iter: Iterator[ReferenceWithCommit] = new RefWithCommitIterator(repo, refs)
    if (hashes.nonEmpty) {
      iter = iter.filter(c => hashes.contains(c.commit.getId.getName))
    }

    if (indexes.nonEmpty) {
      iter = iter.filter(c => indexes.contains(c.index))
    }

    iter
  }

}

/**
  * Iterator that will return references with their commit and the commit index in the reference.
  *
  * @param repo repository to get the data from
  * @param refs iterator of references
  */
class RefWithCommitIterator(repo: Repository,
                            refs: Iterator[Ref]) extends Iterator[ReferenceWithCommit] {

  private var actualRef: Ref = _
  private var commits: Iterator[RevCommit] = _
  private var index: Int = 0

  /** @inheritdoc*/
  override def hasNext: Boolean = {
    while ((commits == null || !commits.hasNext) && refs.hasNext) {
      actualRef = refs.next()
      index = 0
      commits =
        try {
          Git.wrap(repo).log()
            .add(Option(actualRef.getPeeledObjectId).getOrElse(actualRef.getObjectId))
            .call().asScala.toIterator
        } catch {
          case _: IncorrectObjectTypeException => null
          // TODO: This reference is pointing to a non commit object, log this
        }
    }

    refs.hasNext || (commits != null && commits.hasNext)
  }

  /** @inheritdoc*/
  override def next(): ReferenceWithCommit = {
    val result: ReferenceWithCommit = ReferenceWithCommit(actualRef, commits.next(), index)
    index += 1
    result
  }

}
