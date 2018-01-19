package tech.sourced.engine.provider

import java.util.concurrent.ConcurrentHashMap

import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext
import org.apache.spark.input.PortableDataStream
import org.apache.spark.rdd.RDD

import scala.collection.concurrent
import scala.collection.convert.decorateAsScala._

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
    * @param path               Path where the repositories are stored.
    * @param repositoriesFormat Format of the repositories that are inside the provided path
    * @return RDD of repositories
    */
  def get(path: String, repositoriesFormat: String): RDD[RepositorySource] =
    rdd.getOrElse(path, RepositoryRDDProvider.generateRDD(sc, path, repositoriesFormat))
}

/**
  * Provides some utility methods for [[RepositoryRDDProvider]] class.
  * Acts as a singleton for getting an unique instance of [[RepositoryRDDProvider]]s, so the
  * recommended way of using said class is using this companion object.
  */
object RepositoryRDDProvider {
  val SivaFormat: String = "siva"
  val BareFormat: String = "bare"
  val StandardFormat: String = "standard"

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
    * Allows bucketing of siva files and raw repositories.
    *
    * @param sc                 Spark Context
    * @param path               path to get the repositories from
    * @param repositoriesFormat format of the repositories inside the provided path
    * @return generated RDD
    */
  private def generateRDD(sc: SparkContext,
                          path: String,
                          repositoriesFormat: String): RDD[RepositorySource] = {
    repositoriesFormat match {
      case SivaFormat =>
        sc.binaryFiles(s"$path/*").flatMap(b => if (b._1.endsWith(".siva")) {
          Some(SivaRepository(b._2))
        } else {
          None
        })
      case StandardFormat | BareFormat =>
        sc.binaryFiles(s"$path/**/*").map {
          case (path: String, pds: PortableDataStream) =>
            // returns a tuple of the root directory where it is contained, with a maximum depth
            // of 1 under the given path, the file name, and the portable data stream
            val idx = path.indexOf('/', path.length + 1)
            if (idx < 0) {
              val p = new Path(path)
              (p.getParent.toString, (p.getName, pds))
            } else {
              val (parent, file) = path.splitAt(idx)
              (parent, (file, pds))
            }
        }.groupByKey()
          .map {
            case (dir, files) =>
              if (repositoriesFormat == StandardFormat) {
                GitRepository(dir, files.head._2)
              } else {
                BareRepository(dir, files.head._2)
              }
          }
      case other => throw new RuntimeException(s"Repository format $other is not supported")
    }
  }

}

/**
  * RepositorySource is a repository that comes from a certain source.
  */
sealed trait RepositorySource extends Serializable {
  /**
    * Returns the portable data stream of one of the repository files. In the case
    * of siva files, of the siva file itself.
    *
    * @return portable data stream
    */
  def pds: PortableDataStream

  /**
    * Returns the path to the root of the repository. In the case of siva files, the
    * path to the siva file itself.
    *
    * @return path to the repository root
    */
  def root: String
}

/**
  * Repository coming from a siva file.
  *
  * @param pds portable data stream of the siva file
  */
case class SivaRepository(pds: PortableDataStream) extends RepositorySource {
  def root: String = pds.getPath
}

/**
  * Repository coming from a bare repository.
  *
  * @param root root of the repository
  * @param pds  portable data stream of any repository file (should only be used to
  *             retrieve the HDFS config)
  */
case class BareRepository(root: String, pds: PortableDataStream) extends RepositorySource

/**
  * Repository coming from a regular repository with a .git directory.
  *
  * @param root root of the repository
  * @param pds  portable data stream of any repository file (should only be used to
  *             retrieve the HDFS config)
  */
case class GitRepository(root: String, pds: PortableDataStream) extends RepositorySource
