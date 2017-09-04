package tech.sourced.api

import java.io.File
import java.sql.Timestamp

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.{Partition, SCWrapper, SparkContext, TaskContext}
import org.eclipse.jgit.api.Git
import org.eclipse.jgit.errors.MissingObjectException
import org.eclipse.jgit.lib.{ObjectId, RefDatabase, Repository}
import org.eclipse.jgit.revwalk.ObjectWalk
import org.eclipse.jgit.storage.file.FileRepositoryBuilder
import org.eclipse.jgit.treewalk.TreeWalk

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.control.Breaks._

case class GitPartition() extends Partition {
  override def index = 0
}

class RepositoryRDD(sc: SparkContext, conf: Broadcast[SCWrapper], folders: Seq[String], localFs: String) extends RDD[Row](sc, Nil) {
  // TODO add repositories into the Partition
  override def compute(split: Partition, context: TaskContext): Iterator[Row] = new MetadataIterator(conf.value.value, folders, localFs) {
    override protected def fillCache(repo: Repository): Iterable[Row] =
      Row(this.actualRepositoryId, Array(), null) :: Nil
  }

  override protected def getPartitions: Array[Partition] = Array(GitPartition())

}

class ReferenceRDD(sc: SparkContext, conf: Broadcast[SCWrapper], folders: Seq[String], localPath: String) extends RDD[Row](sc, Nil) {
  // TODO add repositories into the Partition
  override def compute(split: Partition, context: TaskContext): Iterator[Row] =
    new MetadataIterator(conf.value.value, folders, localPath) {
      override protected def fillCache(repo: Repository): Iterable[Row] = {
        repo.getAllRefs.asScala.values
          .map(ref => Row(this.actualRepositoryId, ref.getName, ObjectId.toString(ref.getLeaf.getObjectId)))
      }
    }

  override protected def getPartitions: Array[Partition] = Array(GitPartition())
}

class CommitRDD(sc: SparkContext, conf: Broadcast[SCWrapper], folders: Seq[String], localPath: String) extends RDD[Row](sc, Nil) {
  // TODO add repositories into the Partition
  override def compute(split: Partition, context: TaskContext): Iterator[Row] =
    new MetadataIterator(conf.value.value, folders, localPath) {
      override protected def fillCache(repo: Repository): Iterable[Row] = {
        repo.getAllRefs.asScala.values.map(ref => Option(ref.getPeeledObjectId).getOrElse(ref.getObjectId)
        )
          .flatMap(refId => {
            try {
              Git.wrap(repo)
                .log()
                .add(refId).call().asScala.zipWithIndex
                .map(commitWithIndex => {
                  val commit = commitWithIndex._1
                  val index = commitWithIndex._2

                  val treeWalk = new TreeWalk(repo)

                  val nth = treeWalk.addTree(commit.getTree.getId)
                  treeWalk.setRecursive(false)

                  val files = Stream.continually(treeWalk)
                    .takeWhile(_.next()).map(tree => {
                    if (tree.isSubtree) {
                      tree.enterSubtree()
                    }
                    tree
                  })
                    .filter(!_.isSubtree)
                    .map(
                      walker => new String(walker.getRawPath) -> ObjectId.toString(walker.getObjectId(nth))) toMap

                  Row(
                    this.actualRepositoryId,
                    ObjectId.toString(refId),
                    index.toLong,
                    commit.getName,
                    commit.getFullMessage,

                    commit.getParents.map(_.name()),
                    files,
                    files.values.toSeq,
                    commit.getParentCount,

                    commit.getAuthorIdent.getEmailAddress,
                    commit.getAuthorIdent.getName,
                    new Timestamp(commit.getAuthorIdent.getWhen.getTime),

                    commit.getCommitterIdent.getEmailAddress,
                    commit.getCommitterIdent.getName,
                    new Timestamp(commit.getCommitterIdent.getWhen.getTime)
                  )
                })
            } catch {
              case e: MissingObjectException =>
                println(e)
                Nil
            }
          })
      }
    }

  override protected def getPartitions: Array[Partition] = Array(GitPartition())
}

class BlobRDD(sc: SparkContext, conf: Broadcast[SCWrapper], folders: Seq[String], localPath: String) extends RDD[Row](sc, Nil) {
  // TODO add repositories into the Partition
  override def compute(split: Partition, context: TaskContext): Iterator[Row] =
    new MetadataIterator(conf.value.value, folders, localPath) {
      override protected def fillCache(repo: Repository): Iterable[Row] = {
        val or = repo.newObjectReader()

        val walker = new ObjectWalk(repo)
        val refs = repo.getRefDatabase.getRefs(RefDatabase.ALL).values().asScala

        val treeWalk = new TreeWalk(repo)
        treeWalk.setRecursive(true)

        refs.foreach(ref => {
          val oid = Option(ref.getPeeledObjectId).getOrElse(ref.getObjectId)

          val commit = walker.parseCommit(oid)
          treeWalk.addTree(commit.getTree)
        })

        Stream.continually(treeWalk).takeWhile(_.next()).map(w => {
          val oid = w.getObjectId(0)
          val content = repo.open(oid).getBytes()
          val path = w.getPathString

          Row(ObjectId.toString(oid), content, path, null, null)
        })
      }
    }

  override protected def getPartitions: Array[Partition] = Array(GitPartition())
}

abstract class MetadataIterator(conf: Configuration, folders: Seq[String], localFs: String) extends Iterator[Row] {

  private var actualRepository: Option[Repository] = None

  protected var actualRepositoryId: String = _

  private var index: Int = 0

  // TODO move to an Iterator itself
  private val cache: mutable.Queue[Row] = mutable.Queue()

  protected def fillCache(repo: Repository): Iterable[Row]

  override def hasNext: Boolean = {
    if (folders.length == index && this.actualRepository.isEmpty) {
      return false
    }

    if (this.actualRepository.isEmpty) {
      while (!(folders.length == index)) {
        breakable {
          val remotePath = new Path(folders(this.index))

          this.index += 1

          val localPath = new Path(localFs, remotePath.getName)

          if (!FileSystem.newInstanceLocal(conf).exists(localPath)) {
            FileSystem.get(conf).copyToLocalFile(remotePath, localPath)
          }

          //TODO Create only one instance per repository per JVM to reuse it in all the Iterators.
          // TODO check if the repository is bare or not
          this.actualRepository = Some(new FileRepositoryBuilder().setGitDir(new File(localPath.toString + "/.git")).build())

          this.setRepositoryId

          this.cache ++= this.fillCache(this.actualRepository.get)

          if (this.cache.isEmpty) {
            break()
          }

          return true
        }
      }
    }

    if (this.cache.isEmpty) {
      false
    } else {
      true
    }
  }

  override def next(): Row = {
    val result = this.cache.dequeue()
    if (this.cache.isEmpty) {
      this.actualRepository = None
    }

    result
  }

  private def setRepositoryId = {
    //val config = this.actualRepository.get.getConfig
    //config.load()
    // TODO get all remotes

    // val url = new URL(config.getString("remote", "origin", "url"))

    //this.actualRepositoryId = url.getHost + url.getPath
    this.actualRepositoryId = this.actualRepository.get.getDirectory.toString
  }

}
