package tech.sourced.engine.iterator

import org.eclipse.jgit.lib.Repository
import tech.sourced.engine.util.{CompiledFilter, Filters}

/**
  * Iterator that will return rows of repositories in a repository.
  *
  * @param repositoryPath path of the given repository
  * @param finalColumns   final columns that must be in the resultant row
  * @param repo           repository to get the data from
  * @param filters        filters for the iterator
  */
class RepositoryIterator(repositoryPath: String,
                         finalColumns: Array[String],
                         repo: Repository,
                         filters: Seq[CompiledFilter],
                         skipReadErrors: Boolean)
  extends ChainableIterator[String](
    finalColumns,
    null,
    filters,
    repo,
    skipReadErrors
  ) {

  // since this iterator does not override getFilters method of RootedRepository
  // we can cache here the matching cases, because they are not going to change.
  private val matchingFilters = Filters(filters)

  /** @inheritdoc*/
  override protected def loadIterator(filters: Seq[CompiledFilter]): Iterator[String] =
    RepositoryIterator.loadIterator(repo, matchingFilters)

  /** @inheritdoc*/
  override protected def mapColumns(id: String): RawRow = {
    val c = repo.getConfig
    val remote = RootedRepo.getRepositoryRemote(repo, id)
    val urls = remote.map(r => c.getStringList("remote", r, "url"))
      .orElse(Some(Array[String]())).get
    val isFork = remote.map(r => c.getBoolean("remote", r, "isfork", false))
      .orElse(Some(false)).get

    Map[String, Any](
      "id" -> id,
      "urls" -> urls,
      "is_fork" -> isFork,
      "repository_path" -> repositoryPath
    )
  }
}

object RepositoryIterator {

  import scala.collection.JavaConverters._

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
                   filters: Filters,
                   repoKey: String = "id"): Iterator[String] = {
    // If there's any non-remote reference, it will show up here, thus
    // making the local repository appear. If we only take into account
    // the remotes the result will be different from the one returned by
    // the reference iterator.
    // This makes us process this twice in a chained reference iterator
    // scenario, even though the result would be correct without this,
    // but it's needed for correctness when the table is asked independently.
    val refRepos = repo.getAllRefs.asScala.keys
      .map(ref => RootedRepo.parseRef(repo, ref)._1)

    val repos = repo.getConfig.getSubsections("remote").asScala.toIterator
      .map(RootedRepo.getRepositoryId(repo, _).get) ++ refRepos

    val iter = repos.toList.distinct.toIterator

    val filterKeys = Seq("id", repoKey)
    if (filters.hasFilters(filterKeys: _*)) {
      iter.filter(filters.matches(filterKeys, _))
    } else {
      iter
    }
  }

}
