package tech.sourced.engine.provider

import java.io.File
import java.nio.file.Paths

import org.apache.commons.io.FileUtils
import org.apache.commons.pool2.impl.{DefaultPooledObject, GenericKeyedObjectPool}
import org.apache.commons.pool2.{BaseKeyedPooledObjectFactory, PooledObject}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.eclipse.jgit.lib.{Repository, RepositoryBuilder}
import tech.sourced.engine.Sources
import tech.sourced.engine.util.MD5Gen
import tech.sourced.siva.SivaReader

import scala.collection.JavaConverters._

/**
  * Generates repositories from siva files at the given local path and keeps a reference count
  * on them to know when they have to be cleaned up. All user-facing API methods are thread-safe.
  *
  * @param localPath   Local path where siva files are.
  * @param skipCleanup Skip deleting files after they reference count of a repository gets to 0.
  * @param maxTotal    Maximum number of repositories to keep in the pool.
  */
class RepositoryProvider(val localPath: String,
                         val skipCleanup: Boolean = false,
                         maxTotal: Int = 64)
  extends Logging {
  private val repositoryObjectFactory =
    new RepositoryObjectFactory(localPath, skipCleanup)
  private val repositoryPool =
    new GenericKeyedObjectPool[RepositoryKey, Repository](repositoryObjectFactory)
  private val numTables = Sources.orderedSources.length

  // The minimum number of total instances must be at least equal to the number of tables that
  // can be processed at the same time.
  private val total = if (maxTotal <= numTables) {
    numTables
  } else {
    maxTotal
  }

  repositoryPool.setMaxTotalPerKey(numTables)
  repositoryPool.setMaxIdlePerKey(numTables)
  repositoryPool.setMaxTotal(total)
  repositoryPool.setBlockWhenExhausted(true)

  /**
    * Thread-safe method to get a repository given a repository key.
    *
    * @param key Repository key
    * @return Repository
    */
  def get(key: RepositoryKey): Repository = {
    logDebug(s"Getting new repository instance. active/idle count: " +
      s"${repositoryPool.getNumActive()}/ " +
      s"${repositoryPool.getNumIdle()}")
    repositoryPool.borrowObject(key)
  }

  /**
    * Returns a repository corresponding to the given [[RepositorySource]].
    *
    * @param source Repository source
    * @return Repository
    */
  def get(source: RepositorySource): Repository = this.get(RepositoryProvider.keyForSource(source))

  /**
    * Closes a repository with the given path. All the repositories are ref-counted
    * and when they reach 0 they are removed, unless [[RepositoryProvider#skipCleanup]] is
    * active.
    * This method is thread-safe.
    *
    * @param source Repository source
    * @param repo   The previously obtained Repository instance
    */
  def close(source: RepositorySource, repo: Repository): Unit = {
    val key = RepositoryProvider.keyForSource(source)
    logDebug(s"Closing repository. active/idle count: " +
      s"${repositoryPool.getNumActive()}/ " +
      s"${repositoryPool.getNumIdle()}")

    if (repositoryPool.getNumActive(key) != 0) {
      repositoryPool.returnObject(key, repo)
    } else {
      logDebug(s"closing repository at ${repo.getDirectory};" +
        s" returning object to the pool failed because it was already returned")
    }

    if (repositoryPool.getNumActive == 0) {
      logDebug("No active elements on the pool, clearing all.")
      repositoryPool.clear()
    }
  }
}

class RepositoryObjectFactory(val localPath: String, val skipCleanup: Boolean)
  extends BaseKeyedPooledObjectFactory[RepositoryKey, Repository]
    with Logging {

  override def create(key: RepositoryKey): Repository = {
    val repo = key match {
      case RepositoryKey(conf, path, false, true) =>
        genSivaRepository(conf, path, localPath)
      case RepositoryKey(conf, path, true, false) =>
        genRepository(conf, path, localPath, isBare = true)
      case RepositoryKey(conf, path, _, _) =>
        genRepository(conf, path, localPath, isBare = false)
    }

    if (!skipCleanup) {
      val ctx = TaskContext.get()
      if (ctx != null) {
        // Make really sure that repositories are cleaned up if context is available.
        ctx.addTaskCompletionListener((_) => {
          cleanupRepo(key, repo)
        }).addTaskFailureListener((_, _) => {
          cleanupRepo(key, repo)
        })
      }
    }

    repo
  }

  override def wrap(value: Repository): PooledObject[Repository] =
    new DefaultPooledObject[Repository](value)

  override def destroyObject(key: RepositoryKey, p: PooledObject[Repository]): Unit = {
    cleanupRepo(key, p.getObject)
  }

  def cleanupRepo(key: RepositoryKey, r: Repository): Unit = {
    if (r.getDirectory.exists()) {
      logDebug(s"Cleaning up repository at ${r.getDirectory}")
      r.close()

      if (!skipCleanup && r.getDirectory.toString.startsWith(localPath)) {
        if (key.isSiva) {
          logDebug(s"Deleting unpacked files for ${key.path} at ${r.getDirectory.getParentFile}")
          FileUtils.deleteQuietly(r.getDirectory.getParentFile)
        } else {
          logDebug(s"Deleting unpacked files for ${key.path} at ${r.getDirectory}")
          FileUtils.deleteQuietly(r.getDirectory)
        }
      }
    } else {
      logDebug(s"Repository at ${r.getDirectory} was already cleaned up. Skipping")
    }
  }

  /**
    * Generates a repository from a repository that is not a siva file.
    *
    * @param conf      hadoop configuration
    * @param path      repository path
    * @param localPath local path
    * @param isBare    is it a bare repository?
    * @return generated repository
    */
  private[provider] def genRepository(conf: Configuration,
                                      path: String,
                                      localPath: String,
                                      isBare: Boolean): Repository = {
    val remotePath = new Path(path)
    val fs = remotePath.getFileSystem(conf)

    val (localRepoPath, isLocalPath) = if (!path.startsWith("file:")) {
      val localRepoPath = new Path(
        localPath,
        new Path(
          RepositoryProvider.temporalLocalFolder,
          new Path(MD5Gen.str(path), remotePath.getName)
        )
      )
      if (!fs.exists(localRepoPath)) {
        import RepositoryProvider.HadoopFsRecursiveCopier
        fs.copyToLocalDir(new Path(path), localRepoPath)
      }

      (localRepoPath, false)
    } else {
      (new Path(path.substring(5)), true)
    }

    val repo = new RepositoryBuilder().setGitDir(if (isBare) {
      new File(localRepoPath.toString)
    } else {
      new File(localRepoPath.toString, ".git")
    }).build()

    if (!skipCleanup && !isLocalPath) {
      log.debug(s"Delete $localRepoPath")
      FileUtils.deleteQuietly(Paths.get(localRepoPath.toString).toFile)
    }

    repo
  }


  /**
    * Generates a repository coming from a siva file with the given configuration and paths.
    *
    * @param conf      HDFS configuration
    * @param path      Remote repository path
    * @param localPath local path where rooted repositories are downloaded from the remote FS
    * @return Repository
    */
  private[provider] def genSivaRepository(conf: Configuration,
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

        // Name is an arbitrary UTF-8 string identifying a file in the archive. Note
        // that this might or might not be a POSIX-compliant path.
        //
        // Security note: Users should be careful when using name as a file path
        // (e.g. to extract an archive) since it can contain relative paths and be
        // vulnerable to Zip Slip (https://snyk.io/research/zip-slip-vulnerability)
        // or other potentially dangerous values such as absolute paths, network
        // drive addresses, etc.
        val outFile = new File(outPath.toString)
        val localFile = new File(localUnpackedPath.toString)
        if (!outFile.getCanonicalPath.startsWith(localFile.getCanonicalPath + File.separator)) {
          throw new RuntimeException(s"Entry is outside of the target dir: ${ie.getName}")
        }

        FileUtils.copyInputStreamToFile(e, outFile)
      })

      sr.close()
    }

    // After copy create a repository instance using the local path
    val gitDir = new File(localUnpackedPath.toString)
    val repo = new ReadOnlyFileRepository(gitDir)

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
    * @param maxTotal    Maximum number of repositories to keep in the pool
    * @return a new repository provider or an already existing one if there is one
    */
  def apply(localPath: String,
            skipCleanup: Boolean = false,
            maxTotal: Int = 64): RepositoryProvider = {
    if (provider == null || provider.localPath != localPath) {
      provider = new RepositoryProvider(localPath, skipCleanup = skipCleanup, maxTotal = maxTotal)
    }

    provider
  }

  /**
    * Returns the repository key for the given repository source.
    *
    * @param source repository source
    * @return key
    */
  def keyForSource(source: RepositorySource): RepositoryKey = source match {
    case SivaRepository(pds) => RepositoryKey(
      pds.getConfiguration,
      pds.getPath,
      isBare = false,
      isSiva = true)
    case BareRepository(root, pds) => RepositoryKey(
      pds.getConfiguration,
      root,
      isBare = true,
      isSiva = false
    )
    case GitRepository(root, pds) => RepositoryKey(
      pds.getConfiguration,
      root,
      isBare = false,
      isSiva = false
    )
  }


  /**
    * Little wrapper around Hadoop File System to copy all folder files to the local
    * file system.
    *
    * @param fs hadoop file system
    */
  implicit class HadoopFsRecursiveCopier(fs: FileSystem) {

    /**
      * Recursively copy a directory to a local directory
      *
      * @param src source directory path
      * @param dst destination directory path
      */
    def copyToLocalDir(src: Path, dst: Path): Unit = {
      val iter = fs.listFiles(src, true)
      while (iter.hasNext) {
        val f = iter.next
        val dstPath = new Path(dst.toString, f.getPath.toString.substring(src.toString.length))
        fs.copyToLocalFile(f.getPath, dstPath)
      }

    }

  }

  val temporalLocalFolder = "processing-repositories"
  val temporalSivaFolder = "siva-files"
}

protected case class RepositoryKey(conf: Configuration,
                                   path: String,
                                   isBare: Boolean,
                                   isSiva: Boolean)
