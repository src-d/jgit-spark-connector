package tech.sourced.engine.provider

import java.util.concurrent.ConcurrentHashMap

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext
import org.apache.spark.input.PortableDataStream
import org.apache.spark.rdd.RDD

import scala.collection.convert.decorateAsScala._
import scala.collection.concurrent

/**
  * Provides an RDD of repositories in the following forms:
  * - siva files
  * - bare repositories
  * - regular git repositories
  *
  * @param sc Spark Context
  */
class RepositoryRDDProvider(sc: SparkContext) {
  private val rdd: concurrent.Map[String, RDD[RepositorySource]] =
    new ConcurrentHashMap[String, RDD[RepositorySource]]().asScala

  /**
    * Generates an RDD of repositories with their source at the given path.
    * Path may be remote or local.
    *
    * @param path Path where the repositories are stored.
    * @return RDD of repositories
    */
  def get(path: String): RDD[RepositorySource] =
    rdd.getOrElse(path, RepositoryRDDProvider.generateRDD(sc, path))
}

/**
  * Provides some utility methods for [[RepositoryRDDProvider]] class.
  * Acts as a singleton for getting an unique instance of [[RepositoryRDDProvider]]s, so the
  * recommended way of using said class is using this companion object.
  */
object RepositoryRDDProvider {
  /** The singleton Siva RDD provider. */
  var provider: RepositoryRDDProvider = _

  /**
    * Returns the provider instance and creates one if none has been created yet.
    *
    * @param sc Spark Context
    * @return RepositorySource RDD provider
    */
  def apply(sc: SparkContext): RepositoryRDDProvider = {
    Option(provider).getOrElse({
      provider = new RepositoryRDDProvider(sc)
      provider
    })
  }

  /**
    * Generates an RDD of [[RepositorySource]] with the repositories at the given path.
    * Allows bucketting of siva files, but not bucketting of repositories or bare repoitories.
    *
    * @param sc   Spark Context
    * @param path path to get the repositories from
    * @return generated RDD
    */
  private def generateRDD(sc: SparkContext, path: String): RDD[RepositorySource] = {
    sc.binaryFiles(s"$path/*")
      .map(_._2)
      .map(pds => {
        // returns a tuple of the root directory where it is contained, with a maximum depth
        // of 1 under the given path, the file name, and the portable data stream
        val path = pds.getPath
        val idx = path.indexOf('/', path.length + 1)
        if (idx < 0) {
          val p = new Path(path)
          (p.getParent.toString, p.getName, pds)
        } else {
          val (parent, file) = path.splitAt(idx)
          (parent, file, pds)
        }
      })
      .groupBy(_._1) // group by root directory
      .flatMap {
        case (dir, files) =>
          // files ending in .siva will be treated as an individual siva repository
          // If there are no siva files in a repository, HEAD file will be searched
          // if it's found, it will be treated as a bare repository. If it's not a
          // bare repository, .git directory will be searched and if found, it will
          // be treated as a regular git repository.
          val sivaFiles = files.filter(_._2.endsWith(".siva"))
          if (sivaFiles.nonEmpty) {
            sivaFiles.map(f => SivaRepository(f._3))
          } else if (files.exists(_._2 == "HEAD")) {
            Seq(BareRepository(files.head._1, files.head._3))
          } else if (files.nonEmpty) {
            val f = files.head
            val fs = FileSystem.get(f._3.getConfiguration)
            if (fs.exists(new Path(f._1, ".git"))) {
              Seq(GitRepository(f._1, f._3))
            } else {
              Seq()
            }
          } else {
            Seq()
          }
      }
  }
}

/**
  * RepositorySource is a repository that comes from a certain source.
  */
sealed trait RepositorySource extends Serializable {
  def pds: PortableDataStream
}

/**
  * Repository coming from a siva file.
  * @param pds portable data stream of the siva file
  */
case class SivaRepository(pds: PortableDataStream) extends RepositorySource

/**
  * Repository coming from a bare repository.
  * @param root root of the repository
  * @param pds portable data stream of any repository file (should only be used to
  *            retrieve the HDFS config)
  */
case class BareRepository(root: String, pds: PortableDataStream) extends RepositorySource

/**
  * Repository coming from a regular repository with a .git directory.
  * @param root root of the repository
  * @param pds portable data stream of any repository file (should only be used to
  *            retrieve the HDFS config)
  */
case class GitRepository(root: String, pds: PortableDataStream) extends RepositorySource
