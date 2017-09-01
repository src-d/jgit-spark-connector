package tech.sourced.api.provider

import java.io.File
import java.nio.file.Paths

import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.input.PortableDataStream
import org.eclipse.jgit.lib.{Repository, RepositoryBuilder}
import tech.sourced.api.util.MD5Gen
import tech.sourced.siva.SivaReader

import scala.collection.JavaConverters._
import scala.collection.mutable

class RepositoryProvider(localPath: String) {
  val repositories: mutable.Map[String, Repository] = mutable.Map()

  def get(conf: Configuration, path: String): Repository = {
    repositories.get(path) match {
      case Some(r) =>
        r.incrementOpen()
        r
      case None => RepositoryProvider.genRepository(conf, path, localPath)
    }
  }

  def get(pds: PortableDataStream): Repository =
    this.get(pds.getConfiguration, pds.getPath())

  def close(path: String): Unit = {
    repositories.get(path) match {
      case Some(r) =>
        r.close()
      // TODO maybe others are using this repository instance
      // FileUtils.deleteQuietly(r.getDirectory)
      case None =>
    }
  }
}

object RepositoryProvider {
  var provider: RepositoryProvider = _

  def apply(localPath: String): RepositoryProvider = {
    if (provider == null) {
      provider = new RepositoryProvider(localPath)
    }

    provider
  }

  val temporalLocalFolder = "processing-repositories"
  val temporalSivaFolder = "siva-files"

  private def genRepository(conf: Configuration, path: String, localPath: String): Repository = {
    val remotePath = new Path(path)

    val localCompletePath =
      new Path(localPath,
        new Path(temporalLocalFolder,
          new Path(MD5Gen.str(path), remotePath.getName)
        )
      )

    val localSivaPath = new Path(localPath, new Path(temporalSivaFolder, remotePath.getName))

    // Copy siva file to local fs
    FileSystem.get(conf)
      .copyToLocalFile(remotePath, localSivaPath)

    // unpack siva file
    val sr = new SivaReader(new File(localSivaPath.toString))
    val index = sr.getIndex.getFilteredIndex.getEntries.asScala
    index.foreach(ie => {
      val e = sr.getEntry(ie)
      val outPath = Paths.get(localCompletePath.toString, ie.getName)

      FileUtils.copyInputStreamToFile(e, new File(outPath.toString))
    })

    // After copy create a repository instance using the local path
    val repo = new RepositoryBuilder().setGitDir(new File(localCompletePath.toString)).build()

    // delete siva file
    FileUtils.deleteQuietly(Paths.get(localSivaPath.toString).toFile)

    repo
  }
}