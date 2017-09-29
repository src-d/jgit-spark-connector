package tech.sourced.api.iterator

import org.apache.spark.internal.Logging
import org.eclipse.jgit.diff.RawText
import org.eclipse.jgit.lib.{ObjectId, ObjectReader, Ref, Repository}
import org.eclipse.jgit.treewalk.TreeWalk
import org.slf4j.Logger
import tech.sourced.api.util.CompiledFilter

import scala.collection.JavaConverters._
import scala.collection.mutable


/**
  * Blob iterator: returns all blobs from the filtered commits.
  *
  * @param requiredColumns
  * @param repo
  * @param filters
  */
class BlobIterator(requiredColumns: Array[String], repo: Repository, filters: Array[CompiledFilter])
  extends RootedRepoIterator[CommitTree](requiredColumns, repo) with Logging {

  override protected def loadIterator(): Iterator[CommitTree] = {
    val repositories = mutable.Buffer[String]()
    val references = mutable.Buffer[String]()
    val hashes = mutable.Buffer[String]()

    filters.flatMap(_.matchingCases)
      .foreach(filter => filter match {
        case ("repository_id", repos) =>
          repositories ++= repos.map(_.asInstanceOf[String])
        case ("reference_name", refs) =>
          references ++= refs.map(_.asInstanceOf[String])
        case ("commit_hash", commitHashes) =>
          hashes ++= commitHashes.map(_.asInstanceOf[String])
        case anyOtherFilter =>
          log.debug(s"BlobIterator does not support filter $anyOtherFilter")
      })

    // Fast path, if there are no repositories or references to filter by
    // just return the files from the given hashes, which is the fastest
    // option of all.
    if (!hashes.isEmpty && repositories.isEmpty && references.isEmpty) {
      filesFromHashes(hashes)
    } else {
      var repos = repo.getConfig.getSubsections("remote").asScala
        .map(getRepositoryId(_).get)

      if (!repositories.isEmpty) {
        repos = repos.filter(repositories.contains(_))
      }

      var refs = repo.getAllRefs.asScala.values.toIterator
        .filter(ref => {
          val (repoId, refName) = parseRef(ref.getName)
          repos.contains(repoId)
        })

      // If there are references to filter, just get the hash of the commit they point to
      // and treat it as a hash filter.
      if (!references.isEmpty) {
        refs = refs.filter(ref => {
          val (_, refName) = parseRef(ref.getName)
          val contained = references.contains(refName)
          if (contained) {
            hashes += Option(ref.getPeeledObjectId).getOrElse(ref.getObjectId).name
          }
          contained
        })
      }

      if (refs.isEmpty) {
        Seq().toIterator
      } else {
        hashes.distinct
        var commits = CommitIterator.refCommits(repo, refs.toList.distinct: _*)
        if (!hashes.isEmpty) {
          commits = commits.filter(c => hashes.contains(c.getId.name))
        }

        commits.flatMap(c => JGitBlobIterator(getTreeWalk(c.getId), log))
      }
    }
  }

  private def filesFromHashes(hashes: Seq[String]): Iterator[CommitTree] = {
    hashes.toIterator.flatMap(hash => {
      val id = ObjectId.fromString(hash)
      if (repo.hasObject(id)) {
        JGitBlobIterator(getTreeWalk(id), log)
      } else {
        Seq()
      }
    })
  }

  override protected def mapColumns(commitTree: CommitTree): Map[String, () => Any] = {
    log.debug(s"Reading blob:${commitTree.tree.getObjectId(0).name()} of tree:${commitTree.tree.getPathString} from commit:${commitTree.commit.name()}")
    val content = BlobIterator.readFile(commitTree.tree.getObjectId(0), commitTree.tree.getObjectReader)
    val isBinary = RawText.isBinary(content)
    Map[String, () => Any](
      "repository_id" -> (() => null),
      "reference_name" -> (() => null),
      "file_hash" -> (() => commitTree.tree.getObjectId(0).name),
      "content" -> (() => if (isBinary) Array.emptyByteArray else content),
      "commit_hash" -> (() => commitTree.commit.name),
      "is_binary" -> (() => isBinary),
      "path" -> (() => commitTree.tree.getPathString)
    )
  }

  private def getTreeWalk(commitId: ObjectId): CommitTree = {
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
      log.debug(s"Skip non-existing ${commitTree.tree.getObjectId(0).name()} ")
      moveIteratorSkippingMissingObj
    }
  }

}

object JGitBlobIterator {
  def apply(commitTree: CommitTree, log: Logger) = new JGitBlobIterator(commitTree, log)
}


