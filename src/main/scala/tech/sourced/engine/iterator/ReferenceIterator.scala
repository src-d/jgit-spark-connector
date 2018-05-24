package tech.sourced.engine.iterator

import org.eclipse.jgit.lib.{ObjectId, Ref, Repository}
import tech.sourced.engine.util.{CompiledFilter, Filters}

import scala.collection.JavaConverters._

/**
  * Iterator that will return rows of references in a repository.
  *
  * @param finalColumns final columns that must be in the resultant row
  * @param repo         repository to get the data from
  * @param prevIter     previous iterator, if the iterator is chained
  * @param filters      filters for the iterator
  */
class ReferenceIterator(finalColumns: Array[String],
                        repo: Repository,
                        prevIter: RepositoryIterator,
                        filters: Seq[CompiledFilter],
                        skipReadErrors: Boolean)
  extends ChainableIterator[Ref](
    finalColumns,
    prevIter,
    filters,
    repo,
    skipReadErrors
  ) {

  /** @inheritdoc */
  protected def loadIterator(filters: Seq[CompiledFilter]): Iterator[Ref] =
    ReferenceIterator.loadIterator(
      repo,
      Option(prevIter).map(_.currentRow),
      Filters(filters)
    )

  /** @inheritdoc */
  override protected def mapColumns(ref: Ref): RawRow = {
    val (repoId, refName) = RootedRepo.parseRef(repo, ref.getName)
    Map[String, Any](
      "repository_id" -> repoId,
      "name" -> refName,
      "hash" -> ObjectId.toString(Option(ref.getPeeledObjectId).getOrElse(ref.getObjectId)),
      "is_remote" -> RootedRepo.isRemote(repo, ref.getName)
    )
  }
}

object ReferenceIterator {

  /**
    * Returns an iterator of references.
    *
    * @param repo       repository to get the data from
    * @param filters    filters to skip some rows. The only supported filters at the iterator
    *                   level are by repository id and by reference name. The keys of said filters
    *                   are controlled by the parameters `repoKey` and `refNameKey`.
    * @param repoKey    name of the repository id filter key
    * @param refNameKey name of the reference name filter key
    * @return the iterator
    */
  def loadIterator(repo: Repository,
                   repoId: Option[String],
                   filters: Filters,
                   repoKey: String = "repository_id",
                   refNameKey: String = "name"): Iterator[Ref] = {
    val repoKeys = Seq(repoKey)
    val repoIds: Array[String] = repoId match {
      case Some(id) =>
        if (!filters.hasFilters(repoKeys: _*) || filters.matches(repoKeys, id)) {
          Array(id)
        } else {
          Array()
        }
      case None =>
        RepositoryIterator.loadIterator(repo, filters, repoKey).toArray
    }

    val refNameKeys = Seq("name", refNameKey)
    val hasRefFilters = filters.hasFilters(refNameKeys: _*)
    val out = repo.getAllRefs.asScala.values.filter(ref => {
      val (repoId, refName) = RootedRepo.parseRef(repo, ref.getName)
      (repoIds.isEmpty || repoIds.contains(repoId)) &&
        (!hasRefFilters || filters.matches(refNameKeys, refName))
    })

    out.toIterator
  }

}
