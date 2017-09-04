package tech.sourced.api.iterator

import org.eclipse.jgit.lib.{ObjectId, Ref, Repository}

import scala.collection.JavaConverters._

class ReferenceIterator(requiredColumns: Array[String], repo: Repository)
  extends RootedRepoIterator[Ref](requiredColumns, repo) {

  override protected def loadIterator(): Iterator[Ref] =
    repo.getAllRefs.asScala.values.toIterator

  override protected def mapColumns(ref: Ref): Map[String, () => Any] = {
    val (repoId, refName) = parseRef(ref.getName)
    Map[String, () => Any](
      "repository_id" -> (() => {
        repoId
      }),
      "name" -> (() => {
        refName
      }),
      "hash" -> (() => {
        ObjectId.toString(ref.getObjectId)
      })
    )
  }
}
