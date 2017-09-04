package tech.sourced.api.iterator

import org.eclipse.jgit.lib.{Repository, StoredConfig}

import scala.collection.JavaConverters.collectionAsScalaIterableConverter

class RepositoryIterator(requiredColumns: Array[String], repo: Repository)
  extends RootedRepoIterator[String](requiredColumns: Array[String], repo: Repository) {
  override protected def loadIterator(): Iterator[String] =
    repo.getConfig.getSubsections("remote").asScala.toIterator

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
