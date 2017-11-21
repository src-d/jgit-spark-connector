package tech.sourced.engine.iterator

import org.eclipse.jgit.lib.Repository
import tech.sourced.engine.util.{CompiledFilter, Filters}

import scala.collection.JavaConverters.collectionAsScalaIterableConverter

/**
  * Iterator that will return rows of repositories in a repository.
  *
  * @param finalColumns final columns that must be in the resultant row
  * @param repo         repository to get the data from
  * @param filters      filters for the iterator
  */
class RepositoryIterator(finalColumns: Array[String],
                         repo: Repository,
                         filters: Seq[CompiledFilter])
  extends RootedRepoIterator[String](finalColumns, repo, null, filters) {

  // since this iterator does not override getFilters method of RootedRepository
  // we can cache here the matching cases, because they are not going to change.
  private val matchingFilters = filters.flatMap(_.matchingCases)

  /** @inheritdoc*/
  override protected def loadIterator(filters: Seq[CompiledFilter]): Iterator[String] =
    RepositoryIterator.loadIterator(repo, matchingFilters)


  /** @inheritdoc*/
  override protected def mapColumns(id: String): Map[String, () => Any] = {
    val c = repo.getConfig
    val uuid = RootedRepo.getRepositoryUUID(repo, id).get
    val urls = c.getStringList("remote", uuid, "url")
    val isFork = c.getBoolean("remote", uuid, "isfork", false)

    Map[String, () => Any](
      "id" -> (() => id),
      "urls" -> (() => urls),
      "is_fork" -> (() => isFork)
    )
  }

}

object RepositoryIterator {

  /**
    * Returns an iterator of references.
    *
    * @param repo    repository to get the data from
    * @param filters filters to skip some rows. The only supported filters at the iterator
    *                level are by repository id. The key of said filters
    *                are controlled by the parameter `repoKey`.
    * @param repoKey name of the repository id filter key
    * @return the iterator
    */
  def loadIterator(repo: Repository,
                   filters: Seq[Filters.Match],
                   repoKey: String = "id"): Iterator[String] = {
    val ids = filters.flatMap {
      case (k, repoIds) if k == repoKey => repoIds.map(_.toString)
      case _ => Seq()
    }

    var iter = repo.getConfig.getSubsections("remote").asScala.toIterator
      .map(RootedRepo.getRepositoryId(repo, _).get)

    if (ids.nonEmpty) {
      iter = iter.filter(ids.contains(_))
    }

    val l = iter.toList
    iter = l.toIterator

    iter
  }

}
