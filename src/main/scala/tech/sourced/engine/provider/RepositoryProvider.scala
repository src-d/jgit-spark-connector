package tech.sourced.engine.provider

import java.io.File
import java.nio.file.Paths
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.input.PortableDataStream
import org.apache.spark.internal.Logging
import org.eclipse.jgit.lib.{Repository, RepositoryBuilder}
import tech.sourced.engine.util.MD5Gen
import tech.sourced.siva.SivaReader

import scala.collection.JavaConverters._
import scala.collection.concurrent

/**
  * Generates repositories from siva files at the given local path and keeps a reference count
  * on them to know when they have to be cleaned up. All user-facing API methods are thread-safe.
  *
  * @param localPath   Local path where siva files are.
  * @param skipCleanup Skip deleting files after they reference count of a repository gets to 0.
  */
class RepositoryProvider(val localPath: String, val skipCleanup: Boolean = false) extends Logging {

  /**
    * Map to keep track of all the repository instances open.
    */
  private val repositories: concurrent.Map[String, Repository] =
    new ConcurrentHashMap[String, Repository]().asScala

  /**
    * Map to keep track of the reference count of all repositories.
    */
  private val repoRefCounts: concurrent.Map[String, AtomicInteger] =
    new ConcurrentHashMap[String, AtomicInteger]().asScala

  /**
    * Thread-safe method to get a repository given an HDFS configuration and its path.
    * The count of the given repository is incremented by one.
    *
    * @param conf HDFS configuration
    * @param path Repository path
    * @return Repository
    */
  def get(conf: Configuration, path: String): Repository = synchronized {
    this.incrCounter(path)
    repositories.get(path) match {
      case Some(repo) => {
        repo.incrementOpen()
        repo
      }
      case None => {
        val repo = genRepository(conf, path, localPath)
        repositories.put(path, repo)
        repo
      }
    }
  }

  /**
    * Increments the reference count of the repository at the given path by one.
    *
    * @param path Repository path
    */
  private def incrCounter(path: String): Unit = {
    repoRefCounts.get(path) match {
      case Some(counter) => counter.incrementAndGet()
      case None => repoRefCounts.put(path, new AtomicInteger(1))
    }
  }

  /**
    * Returns a repository corresponding to the given [[PortableDataStream]].
    *
    * @param pds Portable Data Stream
    * @return Repository
    */
  def get(pds: PortableDataStream): Repository =
    this.get(pds.getConfiguration, pds.getPath())

  /**
    * Closes a repository with the given path. All the repositories are ref-counted
    * and when they reach 0 they are removed, unless [[RepositoryProvider#skipCleanup]] is
    * active.
    * This method is thread-safe.
    *
    * @param path Repository path
    */
  def close(path: String): Unit = synchronized {
    repositories.get(path).foreach(r => {
      log.debug(s"Close $path")
      r.close()

      val counter = repoRefCounts.getOrElse(path, new AtomicInteger(1))
      if (counter.decrementAndGet() <= 0) {
        if (!skipCleanup) {
          log.debug(s"Deleting unpacked files for $path at ${r.getDirectory}")
          FileUtils.deleteQuietly(r.getDirectory)
        }
        repositories.remove(path)
        repoRefCounts.remove(path)
      }
    })
  }

  /**
    * Generates a repository with the given configuration and paths.
    *
    * @param conf      HDFS configuration
    * @param path      Remote repository path
    * @param localPath local path where rooted repositories are downloaded from the remote FS
    * @return Repository
    */
  private[provider] def genRepository(conf: Configuration,
                                      path: String,
                                      localPath: String): Repository = {
    val remotePath = new Path(path)

    val localUnpackedPath =
      new Path(localPath,
        new Path(RepositoryProvider.temporalLocalFolder,
          new Path(MD5Gen.str(path), remotePath.getName)
        )
      )

    val fs = remotePath.getFileSystem(conf)

    val (localSivaPath, isLocalSivaFile) = if (!path.startsWith("file:")) {
      // if `remotePath` does not exist locally, copy it
      val localSivaPath = new Path(
        localPath,
        new Path(RepositoryProvider.temporalSivaFolder, remotePath.getName)
      )

      if (!fs.exists(localSivaPath) && !fs.exists(localUnpackedPath)) {
        // Copy siva file to local fs
        log.debug(s"Copy $remotePath to $localSivaPath")
        fs.copyToLocalFile(remotePath, localSivaPath)
      }

      (localSivaPath, false)
    } else {
      // if `remotePath` already exists locally, don't copy anything
      // just use the given siva file
      (new Path(path.substring(5)), true)
    }

    if (!fs.exists(localUnpackedPath)) {
      // unpack siva file
      log.debug(s"Unpack siva file $localSivaPath to $localUnpackedPath")
      val sr = new SivaReader(new File(localSivaPath.toString))
      val index = sr.getIndex.getFilteredIndex.getEntries.asScala
      index.foreach(ie => {
        val e = sr.getEntry(ie)
        val outPath = Paths.get(localUnpackedPath.toString, ie.getName)

        FileUtils.copyInputStreamToFile(e, new File(outPath.toString))
      })

      sr.close()
    }

    // After copy create a repository instance using the local path
    val repo = new RepositoryBuilder().setGitDir(new File(localUnpackedPath.toString)).build()

    // delete siva file
    if (!skipCleanup && !isLocalSivaFile) {
      log.debug(s"Delete $localSivaPath")
      FileUtils.deleteQuietly(Paths.get(localSivaPath.toString).toFile)
    }

    repo
  }

}

object RepositoryProvider {
  /** Singleton repository provider. */
  var provider: RepositoryProvider = _

  /**
    * Returns a new [[RepositoryProvider]] or creates one and returns it if it's not
    * already created.
    *
    * @constructor
    * @param localPath   local path where rooted repositories are downloaded from the remote FS
    * @param skipCleanup skip cleanup after some operations
    * @return a new repository provider or an already existing one if there is one
    */
  def apply(localPath: String, skipCleanup: Boolean = false): RepositoryProvider = {
    if (provider == null) {
      provider = new RepositoryProvider(localPath, skipCleanup = skipCleanup)
    }

    provider
  }

  val temporalLocalFolder = "processing-repositories"
  val temporalSivaFolder = "siva-files"
}
