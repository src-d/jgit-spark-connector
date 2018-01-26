package tech.sourced.engine.iterator

import org.eclipse.jgit.lib.{Repository, StoredConfig}
import tech.sourced.engine.util.GitUrlsParser
import scala.collection.JavaConverters.collectionAsScalaIterableConverter

object RootedRepo {

  /**
    * Returns the ID of a repository given its remote name.
    *
    * @param repo       repository
    * @param remoteName remote name
    * @return repository ID
    */
  private[iterator] def getRepositoryId(repo: Repository, remoteName: String): Option[String] = {
    // TODO: maybe a cache here could improve performance
    val c: StoredConfig = repo.getConfig
    c.getSubsections("remote").asScala.find(_ == remoteName) match {
      case None => None
      case Some(name) => Some(GitUrlsParser.getIdFromUrls(
        c.getStringList("remote", name, "url")
      ))
    }
  }

  /**
    * Returns the remote name of a repository with the given ID.
    *
    * @param repo repository
    * @param id   repository id
    * @return remote name
    */
  private[iterator] def getRepositoryRemote(repo: Repository, id: String): Option[String] = {
    // TODO: maybe a cache here could improve performance
    val c: StoredConfig = repo.getConfig
    c.getSubsections("remote").asScala.find(remoteName => {
      val actualId: String =
        GitUrlsParser.getIdFromUrls(c.getStringList("remote", remoteName, "url"))

      actualId == id
    })
  }

  /**
    * Parses a reference name and returns a tuple with the repository id and the reference name.
    *
    * @param repo repository
    * @param ref  reference name
    * @return tuple with repository id and reference name
    */
  private[iterator] def parseRef(repo: Repository, ref: String): (String, String) = {
    val split: Array[String] = ref.split("/")
    val uuid: String = split.last

    // if it's a siva file, the last part will be the uuid of the repository, which
    // is the name of the remote associated to that particular repository
    getRepositoryId(repo, uuid) match {
      case Some(repoId) =>
        val refName: String = split.init.mkString("/")

        (repoId, refName)

      // If no uuid matches, it means this is not a siva file, so we should find this
      // using the whole reference name
      case None =>
        val c: StoredConfig = repo.getConfig
        val refRemote = repo.getRemoteName(ref)
        val repoId = c.getSubsections("remote").asScala
          .find(_ == refRemote)
          .map(r => GitUrlsParser.getIdFromUrls(c.getStringList("remote", r, "url")))
          .orNull

        if (repoId == null) {
          // if branch is local, use the repo path as directory
          // since there's no way to tell to which remote it belongs (probably none)
          val repoPath = if (repo.getDirectory.toPath.getFileName.toString == ".git") {
            // non-bare repositories will have the .git directory as their directory
            // so we'll use the parent
            repo.getDirectory.toPath.getParent
          } else {
            repo.getDirectory.toPath
          }

          ("file://" + repoPath, ref)
        } else {
          (repoId, ref.replace(s"refs/remotes/$refRemote", "refs/heads"))
        }
    }
  }

  private[iterator] def isRemote(repo: Repository, ref: String): Boolean = {
    val split: Array[String] = ref.split("/")
    val uuid: String = split.last

    // if it's a siva file, the last part will be the uuid of the repository, which
    // is the name of the remote associated to that particular repository
    getRepositoryId(repo, uuid) match {
      case Some(_) =>
        true // is a siva file

      // If no uuid matches, it means this is not a siva file, so we should find this
      // using the whole reference name
      case None =>
        Option(repo.getRemoteName(ref)).isDefined
    }
  }

}
