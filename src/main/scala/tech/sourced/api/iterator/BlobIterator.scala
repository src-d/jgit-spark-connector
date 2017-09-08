package tech.sourced.api.iterator

import org.apache.log4j.Logger
import org.eclipse.jgit.diff.RawText
import org.eclipse.jgit.lib.{ObjectId, ObjectReader, Repository}
import org.eclipse.jgit.revwalk.RevWalk
import org.eclipse.jgit.treewalk.TreeWalk
import tech.sourced.api.util.CompiledFilter

/**
  * Blob iterator: returns all blobs from the filtered commits
  *
  * @param requiredColumns
  * @param repo
  * @param filters
  */
class BlobIterator(requiredColumns: Array[String], repo: Repository, filters: Array[CompiledFilter])
  extends RootedRepoIterator[TreeWalk](requiredColumns, repo) {

  val log = Logger.getLogger(this.getClass.getSimpleName)
  val readMaxBytes = 20 * 1024 * 1024

  override protected def loadIterator(): Iterator[TreeWalk] = {
    filters.flatMap { filter =>
      filter.matchingCases.getOrElse("hash", Seq()).flatMap { hash =>
        val commitId = ObjectId.fromString(hash.asInstanceOf[String])
        if (repo.hasObject(commitId)) {
          val revWalk = new RevWalk(repo)
          val revCommit = revWalk.parseCommit(commitId)
          revWalk.close()

          val treeWalk = new TreeWalk(repo)
          treeWalk.setRecursive(true)
          treeWalk.addTree(revCommit.getTree)

          JGitBlobIterator(treeWalk, log)
        } else {
          Seq()
        }
      }
    }
  }.toIterator

  override protected def mapColumns(tree: TreeWalk): Map[String, () => Any] = {
    val content = readFile(tree.getObjectId(0), tree.getObjectReader)
    Map[String, () => Any](
      "file_hash" -> (() => tree.getObjectId(0)),
      "content" -> (() => content),
      "is_binary" -> (() => RawText.isBinary(content)),
      "path" -> (() => tree.getPathString)
    )
  }

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


class JGitBlobIterator[T <: TreeWalk](treeWalk: T, log: Logger) extends Iterator[T] {
  var wasAlreadyMoved = false

  override def hasNext: Boolean = {
    if (wasAlreadyMoved) {
      return true
    }

    val hasNext = try {
      treeWalk.next()
    } catch {
      case e: Exception => log.error(s"Failed to iterate tree - due to ${e.getClass.getSimpleName}", e)
        false
    }
    wasAlreadyMoved = true
    if (!hasNext) {
      treeWalk.close()
    }
    hasNext
  }

  override def next(): T = {
    if (!wasAlreadyMoved) {
      treeWalk.next()
    }
    wasAlreadyMoved = false
    treeWalk
  }
}

object JGitBlobIterator {
  def apply(tree: TreeWalk, log: Logger) = new JGitBlobIterator(tree, log)
}


