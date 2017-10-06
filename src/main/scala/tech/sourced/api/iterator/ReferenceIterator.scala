package tech.sourced.api.iterator

import org.eclipse.jgit.lib.{ObjectId, Ref, Repository}
import tech.sourced.api.util.{Attr, CompiledFilter, EqualExpr, Filter}

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
                        filters: Seq[CompiledFilter])
  extends RootedRepoIterator[Ref](finalColumns, repo, prevIter, filters) {

  /** @inheritdoc*/
  override def getFilters(currentRow: RawRow): Seq[CompiledFilter] = {
    if (currentRow != null) {
      val id = currentRow("id")().asInstanceOf[String]
      filters ++ Seq(EqualExpr(Attr("repository_id", "references"), id))
    } else {
      filters
    }
  }

  /** @inheritdoc*/
  protected def loadIterator(filters: Seq[CompiledFilter]): Iterator[Ref] =
    ReferenceIterator.loadIterator(repo, filters.flatMap(_.matchingCases))

  /** @inheritdoc*/
  override protected def mapColumns(ref: Ref): Map[String, () => Any] = {
    val (repoId, refName) = RootedRepo.parseRef(repo, ref.getName)
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
                   filters: Seq[Filter.Match],
                   repoKey: String = "repository_id",
                   refNameKey: String = "name"): Iterator[Ref] = {
    val referenceNames = filters.flatMap {
      case (k, refNames) if k == refNameKey => refNames.map(_.toString)
      case _ => Seq()
    }

    val repoIds = RepositoryIterator.loadIterator(repo, filters, "repository_id").toArray

    repo.getAllRefs.asScala.values.toIterator.filter(ref => {
      val (repoId, refName) = RootedRepo.parseRef(repo, ref.getName)
      (repoIds.isEmpty || repoIds.contains(repoId)) &&
        (referenceNames.isEmpty || referenceNames.contains(refName))
    })
  }

}
