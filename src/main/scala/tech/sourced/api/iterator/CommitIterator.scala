package tech.sourced.api.iterator

import java.sql.Timestamp

import org.eclipse.jgit.api.Git
import org.eclipse.jgit.errors.IncorrectObjectTypeException
import org.eclipse.jgit.lib.{ObjectId, Ref, Repository}
import org.eclipse.jgit.revwalk.RevCommit
import org.eclipse.jgit.treewalk.TreeWalk

import scala.collection.JavaConverters.iterableAsScalaIterableConverter

class CommitIterator(requiredColumns: Array[String], repo: Repository)
  extends RootedRepoIterator[ReferenceWithCommit](requiredColumns, repo) {
  override protected def loadIterator(): Iterator[ReferenceWithCommit] =
  // TODO this can be improved sending filters to loadIterator,
  // because we don't need to get all the references in all the cases
    new Iterator[ReferenceWithCommit] {
      var refs: Iterator[Ref] = _
      var actualRef: Ref = _
      var commits: Iterator[RevCommit] = _
      var index: Int = 0

      override def hasNext: Boolean = {
        if (refs == null) {
          refs = repo.getAllRefs.values().asScala.toIterator
        }

        while ((commits == null || !commits.hasNext) && refs.hasNext) {
          actualRef = refs.next()
          index = 0
          commits = CommitIterator.refCommits(repo, actualRef)
        }

        refs.hasNext || (commits != null && commits.hasNext)
      }

      override def next(): ReferenceWithCommit = {
        val result: ReferenceWithCommit = ReferenceWithCommit(actualRef, commits.next(), index)
        index += 1
        result
      }
    }


  override protected def mapColumns(obj: ReferenceWithCommit): Map[String, () => Any] = {
    val (repoId, refName) = parseRef(obj.ref.getName)

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
    })
      .filter(!_.isSubtree)
      .map(
        walker => new String(walker.getRawPath) -> ObjectId.toString(walker.getObjectId(nth))) toMap
  }
}


case class ReferenceWithCommit(ref: Ref, commit: RevCommit, index: Int)

object CommitIterator {
  def refCommits(repo: Repository, refs: Ref*): Iterator[RevCommit] =
    try {
      var log = Git.wrap(repo).log()
      refs.foreach(ref => {
        log = log.add(Option(ref.getPeeledObjectId).getOrElse(ref.getObjectId))
      })
      log.call().asScala.toIterator
    } catch {
      case _: IncorrectObjectTypeException => null
      // TODO log this
      // This reference is pointing to a non commit object
    }
}