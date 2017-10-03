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
  * Iterator that generates rows containing files of commits.
  *
  * @param requiredColumns required columns for the returned row
  * @param repo            Git repository
  * @param filters         list of filters to apply in the iterator
  */
class BlobIterator(requiredColumns: Array[String], repo: Repository, filters: Array[CompiledFilter])
  extends RootedRepoIterator[CommitTree](requiredColumns, repo) with Logging {

  /** @inheritdoc*/
  override protected def loadIterator(): Iterator[CommitTree] = {
    val f = getFilterMap()

    // Fast path, if there are no repositories or references to filter by
    // just return the files from the given hashes, which is the fastest
    // option of all.
    if (f.contains("commit_hash")
      && !f.contains("repository_id")
      && !f.contains("reference_name")) {
      filesFromHashes(f("commit_hash"))
    } else {
      var repos = repo.getConfig.getSubsections("remote").asScala
        .map(getRepositoryId(_).get)

      if (f.contains("repository_id")) {
        val repoIds = f("repository_id")
        repos = repos.filter(repoIds.contains(_))
      }

      var refs = repo.getAllRefs.asScala.values.toIterator
        .filter(ref => {
          val (repoId, _) = parseRef(ref.getName)
          repos.contains(repoId)
        })

      // If there are references to filter, just get the hash of the commit they point to
      // and treat it as a hash filter.
      if (f.contains("reference_name")) {
        val references = f("reference_name")
        refs = refs.filter(ref => {
          val (_, refName) = parseRef(ref.getName)
          val contained = references.contains(refName)
          if (contained) {
            val hash = Option(ref.getPeeledObjectId).getOrElse(ref.getObjectId).name
            f.get("commit_hash") match {
              case Some(buf) =>
                buf += hash
              case None => f("commit_hash") = mutable.Buffer(hash)
            }
          }
          contained
        })
      }

      if (refs.isEmpty) {
        Seq().toIterator
      } else {
        val hashes = f.get("commit_hash") match {
          case Some(hashes) => hashes.distinct
          case None => Seq()
        }

        filesFromRefsAndHashes(refs, hashes)
      }
    }
  }

  /**
    * Returns a map from filter key to the buffer containing the filter values.
    */
  private def getFilterMap(): mutable.HashMap[String, mutable.Buffer[String]] = {
    val filterMap = mutable.HashMap[String, mutable.Buffer[String]]()

    filters.flatMap(_.matchingCases)
      .foreach(filter => filter match {
        case (k@("repository_id" | "reference_name" | "commit_hash"), values) =>
          filterMap(k) = mutable.Buffer(values.map(_.asInstanceOf[String]): _*)
        case anyOtherFilter =>
          log.debug(s"BlobIterator does not support filter $anyOtherFilter")
      })

    filterMap
  }

  /**
    * Returns an iterator of [[CommitTree]] with the files of the given references and
    * hashes.
    *
    * @param refs   reference iterator
    * @param hashes hash sequence
    * @return CommitTree iterator
    */
  private def filesFromRefsAndHashes(refs: Iterator[Ref], hashes: Seq[String]) =
    refs.flatMap(ref => CommitIterator.refCommits(repo, ref).map(c => (ref, c)))
      .filter(row => {
        val (_, c) = row
        hashes.isEmpty || hashes.contains(c.getId.name)
      })
      .flatMap(row => {
        val (ref, commit) = row
        val (repoId, refName) = parseRef(ref.getName)

        JGitBlobIterator(getCommitTree(repoId, refName, commit.getId), log)
      })

  /**
    * Returns an iterator of [[CommitTree]] with the files of the given commit hashes.
    *
    * @param hashes commit hashes
    * @return iterator of commit trees
    */
  private def filesFromHashes(hashes: Seq[String]): Iterator[CommitTree] = {
    hashes.toIterator.flatMap(hash => {
      val id = ObjectId.fromString(hash)
      if (repo.hasObject(id)) {
        JGitBlobIterator(getCommitTree(id), log)
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
      "repository_id" -> (() => commitTree.repoId),
      "reference_name" -> (() => commitTree.refName),
      "file_hash" -> (() => commitTree.tree.getObjectId(0).name),
      "content" -> (() => if (isBinary) Array.emptyByteArray else content),
      "commit_hash" -> (() => commitTree.commit.name),
      "is_binary" -> (() => isBinary),
      "path" -> (() => commitTree.tree.getPathString)
    )
  }

  /**
    * Returns a commit tree for a commit in the given reference and repository.
    *
    * @param repoId   Repository id
    * @param refName  Reference name
    * @param commitId Commit id
    * @return Commit tree of the commit
    */
  private def getCommitTree(repoId: String, refName: String, commitId: ObjectId): CommitTree = {
    val revCommit = repo.parseCommit(commitId)
    val treeWalk = new TreeWalk(repo)
    treeWalk.setRecursive(true)
    treeWalk.addTree(revCommit.getTree)
    CommitTree(repoId, refName, commitId, treeWalk)
  }

  /**
    * Gets a commit tree only with the ID of the commit.
    *
    * @param commitId commit id
    * @return Commit tree
    */
  private def getCommitTree(commitId: ObjectId): CommitTree = getCommitTree(null, null, commitId)
}

/**
  * Contains a commit and its tree.
  *
  * @param repoId  repository id
  * @param refName reference name
  * @param commit  ObjectId of the commit
  * @param tree    the tree
  */
case class CommitTree(repoId: String, refName: String, commit: ObjectId, tree: TreeWalk)

object BlobIterator {
  /** Max bytes to read for the content of a file. */
  val readMaxBytes = 20 * 1024 * 1024

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

  /** @inheritdoc*/
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

  /** @inheritdoc*/
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

    if (commitTree.tree.getObjectReader().has(commitTree.tree.getObjectId(0))) {
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


