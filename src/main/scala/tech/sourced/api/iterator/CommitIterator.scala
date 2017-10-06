package tech.sourced.api.iterator

import java.sql.Timestamp

import org.eclipse.jgit.api.Git
import org.eclipse.jgit.errors.IncorrectObjectTypeException
import org.eclipse.jgit.lib.{ObjectId, Ref, Repository}
import org.eclipse.jgit.revwalk.RevCommit
import org.eclipse.jgit.treewalk.TreeWalk
import tech.sourced.api.util.{Attr, CompiledFilter, EqualExpr, Filter}

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

  /** @inheritdoc*/
  override def getFilters(currentRow: RawRow): Seq[CompiledFilter] = {
    if (currentRow != null) {
      val id = currentRow("repository_id")().asInstanceOf[String]
      val refName = currentRow("name")().asInstanceOf[String]
      filters ++ Seq(
        EqualExpr(Attr("repository_id", "commits"), id),
        EqualExpr(Attr("reference_name", "commits"), refName)
      )
    } else {
      filters
    }
  }

  type T = Iterator[ReferenceWithCommit]

  /** @inheritdoc */
  override protected def loadIterator(filters: Seq[CompiledFilter]): T =
    CommitIterator.loadIterator(repo, filters.flatMap(_.matchingCases))

  /** @inheritdoc*/
  override protected def mapColumns(obj: ReferenceWithCommit): Map[String, () => Any] = {
    val (repoId, refName) = RootedRepo.parseRef(repo, obj.ref.getName)

    val c: RevCommit = obj.commit
    lazy val files: Map[String, String] = this.getFiles(obj.commit)

    Map[String, () => Any](
      "repository_id" -> (() => repoId),
      "reference_name" -> (() => refName),
      "index" -> (() => obj.index),
      "hash" -> (() => ObjectId.toString(c.getId)),
      "message" -> (() => c.getFullMessage),
      "parents" -> (() => c.getParents.map(p => ObjectId.toString(p.getId))),
      "tree" -> (() => files),
      "blobs" -> (() => files.values.toArray),
      "parents_count" -> (() => c.getParentCount),

      "author_email" -> (() => c.getAuthorIdent.getEmailAddress),
      "author_name" -> (() => c.getAuthorIdent.getName),
      "author_date" -> (() => new Timestamp(c.getAuthorIdent.getWhen.getTime)),

      "committer_email" -> (() => c.getCommitterIdent.getEmailAddress),
      "committer_name" -> (() => c.getCommitterIdent.getName),
      "committer_date" -> (() => new Timestamp(c.getCommitterIdent.getWhen.getTime))
    )
  }

  /**
    * Retrieves the files for a commit.
    *
    * @param c commit
    * @return map of files
    */
  private def getFiles(c: RevCommit): Map[String, String] = {
    val treeWalk: TreeWalk = new TreeWalk(repo)
    val nth: Int = treeWalk.addTree(c.getTree.getId)
    treeWalk.setRecursive(false)

    Stream.continually(treeWalk)
      .takeWhile(_.next()).map(tree => {
      if (tree.isSubtree) {
        tree.enterSubtree()
      }
      tree
    }).filter(!_.isSubtree)
      .map(
        walker => new String(walker.getRawPath) -> ObjectId.toString(walker.getObjectId(nth))).toMap
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
                   filters: Seq[Filter.Match]): Iterator[ReferenceWithCommit] = {
    val refs = ReferenceIterator.loadIterator(repo, filters, "repository_id", "reference_name")
    val hashes = filters.flatMap {
      case ("hash", h) => h.map(_.toString)
      case _ => Seq()
    }

    val indexes = filters.flatMap {
      case ("index", idx) => idx.map(_.asInstanceOf[Int])
      case _ => Seq()
    }

    val refsL = refs.toList

    new RefWithCommitIterator(repo, refsL.toIterator)
      .filter(c => hashes.isEmpty || hashes.contains(c.commit.getId.getName))
      .filter(c => indexes.isEmpty || indexes.contains(c.index))
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
