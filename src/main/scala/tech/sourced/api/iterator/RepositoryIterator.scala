package tech.sourced.api.iterator

import org.eclipse.jgit.lib.{Repository, StoredConfig}
import tech.sourced.api.util.{CompiledFilter, Filter}

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


  /**
    * @inheritdoc
    */
  override protected def mapColumns(uuid: String): Map[String, () => Any] = {
    val c: StoredConfig = repo.getConfig
    val urls: Array[String] = c.getStringList("remote", uuid, "url")
    val isFork: Boolean = c.getBoolean("remote", uuid, "isfork", false)

    Map[String, () => Any](
      "id" -> (() => {
        RootedRepo.getRepositoryId(repo, uuid).get
      }),
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
                   filters: Seq[Filter.Match],
                   repoKey: String = "id"): Iterator[String] = {
    val ids = filters.flatMap {
      case (k, repoIds) if k == repoKey => repoIds.map(_.toString)
      case _ => Seq()
    }

    repo.getConfig.getSubsections("remote").asScala.toIterator
      .filter(ids.isEmpty || ids.contains(_))
  }

}
