package tech.sourced

import scala.collection.JavaConverters._
import tech.sourced.api._
import org.apache.spark.rdd.RDD
import org.eclipse.jgit.lib.{FileMode, Ref, Repository}
import org.eclipse.jgit.revwalk.{RevCommit, RevWalk}
import org.eclipse.jgit.treewalk.TreeWalk

package object spark {

  def loadAllFiles(repositories: GitRepositories): RDD[DenormalizedBlob] = {
    loadAllFiles(repositories, (_: Seq[Ref], _: Ref) => true)
  }

  def loadAllFiles(repositories: GitRepositories, filterReferences: (Seq[Ref], Ref) => Boolean): RDD[DenormalizedBlob] = {
    repositories.flatMapGit { git =>
      val repositoryDescriptor = RepositoryDescriptor(null, null , None, isMainRepository = true) // TODO
      val repository = git.getRepository
      val refs = repository.getAllRefs.values().asScala.toSeq
      for {
        ref <- refs
        if filterReferences(refs, ref)
        resolvedReference = ResolvedReference(ref.getName, ref.getObjectId.getName)
        revCommit = getRevCommit(repository, ref)
        commit = null // TODO
        treeWalk = getTreeWalk(repository, revCommit)
        (file, blob) <- traverseTreeWalk(treeWalk)
      } yield DenormalizedBlob(repositoryDescriptor, resolvedReference, commit, file, blob)
    }
  }

  private[this] def getRevCommit(repository: Repository, ref: Ref): RevCommit = {
    val objectId = ref.getObjectId
    val revWalk = new RevWalk(repository)
    val revCommit = revWalk.parseCommit(objectId)
    revWalk.close()
    revCommit
  }

  private[this] def getTreeWalk(repository: Repository, revCommit: RevCommit): TreeWalk = {
    val treeWalk = new TreeWalk(repository)
    treeWalk.setRecursive(true)
    treeWalk.addTree(revCommit.getTree)
    treeWalk
  }

  private[this] def traverseTreeWalk(treeWalk: TreeWalk): Stream[(File, Blob)] = {
    val fileStream: Stream[Option[(Boolean, (File, Blob))]] = Stream.continually(
        if (treeWalk.next()) {
          val fm = treeWalk.getFileMode
          if (fm == FileMode.REGULAR_FILE || fm == FileMode.EXECUTABLE_FILE) {
            val path = treeWalk.getPathString
            val reader = treeWalk.getObjectReader
            val objId = treeWalk.getObjectId(0)
            val obj = reader.open(objId)
            val bytes = obj.getBytes

            val file = File(
              path = path
            )

            val blob = Blob(
              hash = objId.getName,
              content = bytes
            )

            Some(true -> (file, blob))
          } else {
            None
          }
        } else {
          Some(false -> (null, null))
        }
    )

    fileStream.flatten.takeWhile(_._1).map(_._2)
  }

}
