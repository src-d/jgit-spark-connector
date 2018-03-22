package tech.sourced.engine.provider

import java.io.File

import org.eclipse.jgit.internal.storage.file.FileRepository
import org.eclipse.jgit.storage.file.FileBasedConfig

/**
  * [[FileRepository]] implementation for read-only repositories.
  *
  * Some operations are performance optimized for this case. If the underlying repository changes,
  * usage of this repository implementation might lead to unexpected results.
  *
  * @param gitDir Path to the git directory.
  */
private[provider] class ReadOnlyFileRepository(gitDir: File) extends FileRepository(gitDir) {

  /** @inheritdoc */
  override lazy val getConfig: FileBasedConfig = {
    //XXX: repoConfig is initialized in FileRepository's constructor.
    //     Here we always return it without checking for changes in the underlying
    //     filesystem. This prevents checking for last modification date of configuration
    //     files on every operation.
    val accessor = classOf[FileRepository].getDeclaredField("repoConfig")
    accessor.setAccessible(true)
    accessor.get(this).asInstanceOf[FileBasedConfig]
  }

}
