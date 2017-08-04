package tech.sourced.spark

import java.io.File
import java.nio.file.{Files, Paths}

import org.apache.spark.rdd.RDD
import org.eclipse.jgit.api.Git

import scala.reflect.ClassTag

object GitLoad {
  def clone(basePath: String, url: RDD[String]): GitRepositories =
    new GitCloner(basePath, url)
}

trait GitRepositories {
  def mapGit[T: ClassTag](f: Git => T): RDD[T]
  def flatMapGit[T: ClassTag](f: Git => TraversableOnce[T]): RDD[T]
}

private[spark] object GitCloner {

  private[spark] def clone(path: String, url: String): Git = {
    val pathDir = new File(path)
    pathDir.mkdirs()
    val gitDir = Files.createTempDirectory(pathDir.toPath, "gitdir")
    Git.cloneRepository()
        .setURI(url)
        .setBare(true)
        .setGitDir(gitDir.toFile)
        .call()
  }

  private[spark] def cloneAndMap[T: ClassTag](path: String, url: String, f: Git => T): T = {
    val git = GitCloner.clone(path, url)
    f(git)
    //TODO: Clean up Git directory after usage.
  }

}

private[spark] class GitCloner(var path: String, @transient var urls: RDD[String])
    extends GitRepositories with Serializable {

  def mapGit[T: ClassTag](f: Git => T): RDD[T] = {
    urls.map(GitCloner.cloneAndMap(path, _, f))
  }

  def flatMapGit[T: ClassTag](f: Git => TraversableOnce[T]): RDD[T] = {
    urls.flatMap(GitCloner.cloneAndMap(path, _, f))
  }

}
