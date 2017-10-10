package tech.sourced.api.iterator

import org.apache.spark.internal.Logging
import org.eclipse.jgit.diff.RawText
import org.eclipse.jgit.lib.{ObjectId, ObjectReader, Repository}
import org.eclipse.jgit.treewalk.TreeWalk
import org.slf4j.Logger
import tech.sourced.api.util.CompiledFilter

import scala.collection.mutable

/**
  * Iterator that will return rows of files in a repository.
  *
  * @param finalColumns final columns that must be in the resultant row
  * @param repo         repository to get the data from
  * @param prevIter     previous iterator, if the iterator is chained
  * @param filters      filters for the iterator
  */
class BlobIterator(finalColumns: Array[String],
                   repo: Repository,
                   prevIter: CommitIterator,
                   filters: Seq[CompiledFilter])
  extends RootedRepoIterator[CommitTree](finalColumns, repo, prevIter, filters) with Logging {


  private val computed = mutable.HashSet[String]()

  /** @inheritdoc */
  override protected def loadIterator(filters: Seq[CompiledFilter]): Iterator[CommitTree] = {
    val commitIter = Option(prevIter) match {
      case Some(it) =>
        val commitId = it.currentRow.commit.getId.getName
        if (computed.contains(commitId)) {
          return Seq().toIterator
        }

        computed.add(commitId)
        Seq(it.currentRow).toIterator
      case None => CommitIterator.loadIterator(repo, None, filters.flatMap(_.matchingCases))
    }

    commitIter.flatMap(c => {
      val commitId = c.commit.getId
      if (repo.hasObject(commitId)) {
        JGitBlobIterator(getCommitTree(commitId), log)
      } else {
        Seq()
      }
    })
  }

  /** @inheritdoc */
  override protected def mapColumns(commitTree: CommitTree): Map[String, () => Any] = {
    val content = BlobIterator.readFile(
      commitTree.tree.getObjectId(0),
      commitTree.tree.getObjectReader
    )
    val isBinary = RawText.isBinary(content)
    Map[String, () => Any](
      "file_hash" -> (() => commitTree.tree.getObjectId(0).name),
      "content" -> (() => if (isBinary) Array.emptyByteArray else content),
      "commit_hash" -> (() => commitTree.commit.name),
      "is_binary" -> (() => isBinary),
      "path" -> (() => commitTree.tree.getPathString)
    )
  }

  /**
    * Returns the commit and its tree for the given commit ID.
    *
    * @param commitId commit ID
    * @return commit with tree
    */
  private def getCommitTree(commitId: ObjectId) = {
    val revCommit = repo.parseCommit(commitId)
    val treeWalk = new TreeWalk(repo)
    treeWalk.setRecursive(true)
    treeWalk.addTree(revCommit.getTree)
    CommitTree(commitId, treeWalk)
  }

}

/**
  * Contains a commit and its tree.
  *
  * @param commit ObjectId of the commit
  * @param tree   the tree
  */
case class CommitTree(commit: ObjectId, tree: TreeWalk)

object BlobIterator {
  /** Max bytes to read for the content of a file. */
  val readMaxBytes: Int = 20 * 1024 * 1024

  /**
    * Read max N bytes of the given blob
    *
    * @param objId  ID of the object to read
    * @param reader Object reader to use
    * @param max    maximum number of bytes to read in memory
    * @return Bytearray with the contents of the file
    */
  def readFile(objId: ObjectId, reader: ObjectReader, max: Integer = readMaxBytes): Array[Byte] = {
    val obj = reader.open(objId)
    val data = if (obj.isLarge) {
      val buf = Array.ofDim[Byte](max)
      val is = obj.openStream()
      is.read(buf)
      is.close()
      buf
    } else {
      obj.getBytes
    }
    reader.close()
    data
  }
}

/**
  * Iterates a Tree from a given commit, skipping missing blobs.
  * Must not produce un-reachable objects, as client has no way of dealing with it.
  *
  * @see [[BlobIterator#mapColumns]]
  */
class JGitBlobIterator[T <: CommitTree](commitTree: T, log: Logger) extends Iterator[T] {
  var wasAlreadyMoved = false

  /** @inheritdoc */
  override def hasNext: Boolean = {
    if (wasAlreadyMoved) {
      return true
    }
    val hasNext = try {
      moveIteratorSkippingMissingObj
    } catch {
      case e: Exception =>
        log.error(s"Failed to iterate tree - due to ${e.getClass.getSimpleName}", e)
        false
    }
    wasAlreadyMoved = true
    if (!hasNext) {
      commitTree.tree.close()
    }
    hasNext
  }

  /** @inheritdoc */
  override def next(): T = {
    if (!wasAlreadyMoved) {
      moveIteratorSkippingMissingObj
    }
    wasAlreadyMoved = false
    commitTree
  }

  /**
    * Moves the iterator but skips non-existing objects.
    *
    * @return whether it contains more rows or not
    */
  private def moveIteratorSkippingMissingObj: Boolean = {
    val hasNext = commitTree.tree.next()
    if (!hasNext) {
      return false
    }

    if (commitTree.tree.getObjectReader.has(commitTree.tree.getObjectId(0))) {
      true
    } else { // tree hasNext, but blob obj is missing
      log.debug(s"Skip non-existing ${commitTree.tree.getObjectId(0).name()} ")
      moveIteratorSkippingMissingObj
    }
  }

}

object JGitBlobIterator {
  /**
    * Creates a new JGitBlobIterator from a commit tree and a logger.
    *
    * @constructor
    * @param commitTree commit tree
    * @param log        logger
    * @return a JGit blob iterator
    */
  def apply(commitTree: CommitTree, log: Logger): JGitBlobIterator[CommitTree] =
    new JGitBlobIterator(
      commitTree,
      log
    )

}
