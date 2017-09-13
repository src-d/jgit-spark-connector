package tech.sourced.api.iterator

import org.apache.log4j.Logger
import org.eclipse.jgit.api.Git
import org.eclipse.jgit.diff.RawText
import org.eclipse.jgit.lib.{ObjectId, ObjectReader, Repository}
import org.eclipse.jgit.revwalk.RevWalk
import org.eclipse.jgit.treewalk.TreeWalk
import tech.sourced.api.util.CompiledFilter

import scala.collection.JavaConverters._


/**
  * Blob iterator: returns all blobs from the filtered commits
  *
  * @param requiredColumns
  * @param repo
  * @param filters
  */
class BlobIterator(requiredColumns: Array[String], repo: Repository, filters: Array[CompiledFilter])
  extends RootedRepoIterator[CommitTree](requiredColumns, repo) {

  val log = Logger.getLogger(this.getClass.getSimpleName)

  override protected def loadIterator(): Iterator[CommitTree] = {
    val filtered = filters.toIterator.flatMap { filter =>
      filter.matchingCases.getOrElse("hash", Seq()).flatMap { hash =>
        val commitId = ObjectId.fromString(hash.asInstanceOf[String])
        if (repo.hasObject(commitId)) {
          JGitBlobIterator(getTreeWalk(commitId), log)
        } else {
          Seq()
        }
      }
    }

    if (filtered.hasNext) {
      filtered
    } else {
      val refs = new Git(repo).branchList().call().asScala.filter(!_.isSymbolic)
      log.warn(s"Iterating all ${refs.size} refs")
      refs.toIterator.flatMap { ref =>
        log.warn(s" $ref")
        JGitBlobIterator(getTreeWalk(ref.getObjectId), log)
      }
    }
  }

  override protected def mapColumns(commitTree: CommitTree): Map[String, () => Any] = {
    val content = BlobIterator.readFile(commitTree.tree.getObjectId(0), commitTree.tree.getObjectReader)
    Map[String, () => Any](
      "file_hash" -> (() => commitTree.tree.getObjectId(0).name),
      "content" -> (() => content),
      "commit_hash" -> (() => commitTree.commit.name),
      "is_binary" -> (() => RawText.isBinary(content)),
      "path" -> (() => commitTree.tree.getPathString)
    )
  }

  private def getTreeWalk(commitId: ObjectId) = {
    val revWalk = new RevWalk(repo)
    val revCommit = revWalk.parseCommit(commitId)
    revWalk.close()

    val treeWalk = new TreeWalk(repo)
    treeWalk.setRecursive(true)
    treeWalk.addTree(revCommit.getTree)
    CommitTree(commitId, treeWalk)
  }
}

case class CommitTree(commit: ObjectId, tree: TreeWalk)

object BlobIterator {
  val readMaxBytes = 20 * 1024 * 1024

  /**
    * Read max N bytes of the given blob
    *
    * @param objId
    * @param reader
    * @param max maximum number of bytes to read in memory
    * @return
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

  override def hasNext: Boolean = {
    if (wasAlreadyMoved) {
      return true
    }
    val hasNext = try {
      moveIteratorSkippingMissingObj
    } catch {
      case e: Exception => log.error(s"Failed to iterate tree - due to ${e.getClass.getSimpleName}", e)
        false
    }
    wasAlreadyMoved = true
    if (!hasNext) {
      commitTree.tree.close()
    }
    hasNext
  }

  override def next(): T = {
    if (!wasAlreadyMoved) {
      moveIteratorSkippingMissingObj
    }
    wasAlreadyMoved = false
    commitTree
  }

  private def moveIteratorSkippingMissingObj: Boolean = {
    val hasNext = commitTree.tree.next()
    if (!hasNext) {
      return false
    }

    if (commitTree.tree.getObjectReader().has(commitTree.tree.getObjectId(0))) {
      true
    } else { // tree hasNext, but blob obj is missing
      moveIteratorSkippingMissingObj
    }
  }

}

object JGitBlobIterator {
  def apply(commitTree: CommitTree, log: Logger) = new JGitBlobIterator(commitTree, log)
}


