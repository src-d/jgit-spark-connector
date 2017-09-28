package tech.sourced.api.provider

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
import tech.sourced.api.util.MD5Gen
import tech.sourced.siva.SivaReader

import scala.collection.JavaConverters._
import scala.collection.concurrent

class RepositoryProvider(val localPath: String, val skipCleanup: Boolean = false) extends Logging {

  private val repositories: concurrent.Map[String, Repository] =
    new ConcurrentHashMap[String, Repository]().asScala

  private val repoRefCounts: concurrent.Map[String, AtomicInteger] =
    new ConcurrentHashMap[String, AtomicInteger]().asScala

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

  private def incrCounter(path: String): Unit = {
    repoRefCounts.get(path) match {
      case Some(counter) => counter.incrementAndGet()
      case None => repoRefCounts.put(path, new AtomicInteger(1))
    }
  }

  def get(pds: PortableDataStream): Repository =
    this.get(pds.getConfiguration, pds.getPath())

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

  private[provider] def genRepository(conf: Configuration, path: String, localPath: String): Repository = {
    val remotePath = new Path(path)

    val localCompletePath =
      new Path(localPath,
        new Path(RepositoryProvider.temporalLocalFolder,
          new Path(MD5Gen.str(path), remotePath.getName)
        )
      )

    val localSivaPath = new Path(localPath, new Path(RepositoryProvider.temporalSivaFolder, remotePath.getName))
    val fs = FileSystem.get(conf)

    if (!fs.exists(localSivaPath) && !fs.exists(localCompletePath)) {
      // Copy siva file to local fs
      log.debug(s"Copy $remotePath to $localSivaPath")
      fs.copyToLocalFile(remotePath, localSivaPath)
    }

    if (!fs.exists(localCompletePath)) {
      // unpack siva file
      log.debug(s"Unpack siva file $localSivaPath to $localCompletePath")
      val sr = new SivaReader(new File(localSivaPath.toString))
      val index = sr.getIndex.getFilteredIndex.getEntries.asScala
      index.foreach(ie => {
        val e = sr.getEntry(ie)
        val outPath = Paths.get(localCompletePath.toString, ie.getName)

        FileUtils.copyInputStreamToFile(e, new File(outPath.toString))
      })
    }

    // After copy create a repository instance using the local path
    val repo = new RepositoryBuilder().setGitDir(new File(localCompletePath.toString)).build()

    // delete siva file
    if (!skipCleanup) {
      log.debug(s"Delete $localSivaPath")
      FileUtils.deleteQuietly(Paths.get(localSivaPath.toString).toFile)
    }

    repo
  }

}

object RepositoryProvider {
  var provider: RepositoryProvider = _

  def apply(localPath: String, skipCleanup: Boolean = false): RepositoryProvider = {
    if (provider == null) {
      provider = new RepositoryProvider(localPath, skipCleanup=skipCleanup)
    }

    if (provider.localPath != localPath) {
      throw new RuntimeException(s"actual provider instance is not intended " +
        s"to be used with the localPath provided: $localPath")
    }

    provider
  }

  val temporalLocalFolder = "processing-repositories"
  val temporalSivaFolder = "siva-files"
}
