package tech.sourced.api.iterator

import org.eclipse.jgit.lib.{Repository, StoredConfig}

import scala.collection.JavaConverters.collectionAsScalaIterableConverter

/**
  * Iterator that generates rows of repositories.
  *
  * @param requiredColumns required columns for the returned row
  * @param repo            Git repository
  */
class RepositoryIterator(requiredColumns: Array[String], repo: Repository)
  extends RootedRepoIterator[String](requiredColumns: Array[String], repo: Repository) {
  /**
    * @inheritdoc
    */
  override protected def loadIterator(): Iterator[String] =
    repo.getConfig.getSubsections("remote").asScala.toIterator

  /**
    * @inheritdoc
    */
  override protected def mapColumns(uuid: String): Map[String, () => Any] = {
    val c: StoredConfig = repo.getConfig
    val urls: Array[String] = c.getStringList("remote", uuid, "url")
    val isFork: Boolean = c.getBoolean("remote", uuid, "isfork", false)

    Map[String, () => Any](
      "id" -> (() => {
        this.getRepositoryId(uuid).get
      }),
      "urls" -> (() => urls),
      "is_fork" -> (() => isFork)
    )
  }
}
