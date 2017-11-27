package tech.sourced.engine.iterator

import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, ArrayData}
import org.apache.spark.unsafe.types.UTF8String
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
      Option(prevIter) match {
        case Some(it) => Option(it.currentRow)
        case None => None
      },
      filters.flatMap(_.matchingCases)
    )

  /** @inheritdoc*/
  override protected def mapColumns(obj: ReferenceWithCommit): Map[String, () => Any] = {
    val (repoId, refName) = RootedRepo.parseRef(repo, obj.ref.getName)

    val c: RevCommit = obj.commit
    lazy val files: Map[String, String] = this.getFiles(obj.commit)

    Map[String, () => Any](
      "repository_id" -> (() => UTF8String.fromString(repoId)),
      "reference_name" -> (() => UTF8String.fromString(refName)),
      "index" -> (() => obj.index),
      "hash" -> (() => UTF8String.fromString(ObjectId.toString(c.getId))),
      "message" -> (() => UTF8String.fromString(c.getFullMessage)),
      "parents" -> (() => ArrayData.toArrayData(
        c.getParents.map(p => UTF8String.fromString(ObjectId.toString(p.getId)))
      )),
      "tree" -> (() => {
        val tree = files.map {
          case (k, v) => (UTF8String.fromString(k), UTF8String.fromString(v))
        }
        ArrayBasedMapData(
          tree.keys.toArray,
          tree.values.toArray
        )
      }
        ),
      "blobs" -> (() => ArrayData.toArrayData(files.values.toArray.map(UTF8String.fromString))),
      "parents_count" -> (() => c.getParentCount),

      "author_email" -> (() => UTF8String.fromString(c.getAuthorIdent.getEmailAddress)),
      "author_name" -> (() => UTF8String.fromString(c.getAuthorIdent.getName)),
      "author_date" -> (() => c.getAuthorIdent.getWhen.getTime),

      "committer_email" -> (() => UTF8String.fromString(c.getCommitterIdent.getEmailAddress)),
      "committer_name" -> (() => UTF8String.fromString(c.getCommitterIdent.getName)),
      "committer_date" -> (() => c.getCommitterIdent.getWhen.getTime)
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
