package tech.sourced.api.iterator

import org.eclipse.jgit.lib.{ObjectId, Ref, Repository}

import scala.collection.JavaConverters._

/**
  * Iterator that generates rows of references.
  *
  * @param requiredColumns required columns for the returned row
  * @param repo            Git repository
  */
class ReferenceIterator(requiredColumns: Array[String], repo: Repository)
  extends RootedRepoIterator[Ref](requiredColumns, repo) {

  /**
    * @inheritdoc
    */
  override protected def loadIterator(): Iterator[Ref] =
    repo.getAllRefs.asScala.values.toIterator

  /**
    * @inheritdoc
    */
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
