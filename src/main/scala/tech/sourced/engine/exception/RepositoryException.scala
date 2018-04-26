package tech.sourced.engine.exception

import org.eclipse.jgit.lib.Repository

import scala.collection.JavaConverters.iterableAsScalaIterableConverter

/**
  * Exception to add repository debug information to any
  * uncontrolled exception. It does not add a stacktrace level.
  *
  * @param repo Repository that was beeing iterated
  * @param cause Original exception
  */
class RepositoryException(repo: Repository, cause: Throwable)
    extends Exception(
      s"Repository error with data: ${RepositoryException.repoInfo(repo)}",
      cause,
      true,
      false
    ) {}

object RepositoryException {

  def apply(repo: Repository, cause: Throwable): RepositoryException = {
    new RepositoryException(repo, cause)
  }

  /**
    * Returns a string with a debug description of the repository
    * @param repo Repository to describe
    * @return
    */
  def repoInfo(repo: Repository): String = {
    val repoPath = try {
      repo.toString
    } catch {
      case _: Throwable => "Unknown repository path"
    }

    try {
      val c = repo.getConfig
      val remotes = c.getSubsections("remote").asScala
      val urls = remotes.flatMap(r => c.getStringList("remote", r, "url"))

      if (urls.isEmpty) {
        repoPath
      } else {
        s"$repoPath; urls ${urls.toSet.mkString(", ")}"
      }
    } catch {
      case e: Throwable =>
        s"Exception in RepositoryException.repoInfo for $repoPath: ${e.getMessage}"
    }
  }

}
