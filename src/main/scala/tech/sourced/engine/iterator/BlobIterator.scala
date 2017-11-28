package tech.sourced.engine.iterator

import org.apache.spark.internal.Logging
import org.eclipse.jgit.diff.RawText
import org.eclipse.jgit.lib.{ObjectId, ObjectReader, Repository}
import tech.sourced.engine.util.CompiledFilter

import scala.collection.mutable

/**
  * Iterator that will return rows of blobs in a repository.
  *
  * @param finalColumns final columns that must be in the resultant row
  * @param repo         repository to get the data from
  * @param prevIter     previous iterator, if the iterator is chained
  * @param filters      filters for the iterator
  */
class BlobIterator(finalColumns: Array[String],
                   repo: Repository,
                   prevIter: TreeEntryIterator,
                   filters: Seq[CompiledFilter])
  extends RootedRepoIterator[Blob](finalColumns, repo, prevIter, filters) with Logging {

  // stores the references to the blob, so we only have to read the blob once
  val blobCache: mutable.HashMap[ObjectId, Array[Byte]] = mutable.HashMap()

  /** @inheritdoc*/
  override protected def loadIterator(filters: Seq[CompiledFilter]): Iterator[Blob] = {
    val treeEntryIter = Option(prevIter) match {
      case Some(it) =>
        Seq(it.currentRow).toIterator
      case None => TreeEntryIterator.loadIterator(
        repo,
        None,
        filters.flatMap(_.matchingCases),
        blobIdKey = "blob_id"
      )
    }

    val blobIds = filters.flatMap(_.matchingCases).flatMap {
      case ("blob_id", ids) => ids.map(i => ObjectId.fromString(i.toString))
      case _ => Seq()
    }

    val iter = treeEntryIter.flatMap(entry => {
      if (repo.hasObject(entry.blob)) {
        Some(Blob(entry.blob, entry.commitHash, entry.ref, entry.repo))
      } else {
        None
      }
    })

    if (blobIds.nonEmpty) {
      iter.filter(b => blobIds.contains(b.id))
    } else {
      iter
    }
  }

  /** @inheritdoc*/
  override protected def mapColumns(blob: Blob): Map[String, () => Any] = {
    // Don't read the blob again if it's already in the blob cache
    val content = if (blobCache.contains(blob.id)) {
      blobCache.getOrElse(blob.id, Array.emptyByteArray)
    } else {
      val c = BlobIterator.readFile(
        blob.id,
        repo.newObjectReader()
      )
      blobCache.put(blob.id, c)
      c
    }
    val isBinary = RawText.isBinary(content)

    Map[String, () => Any](
      "commit_hash" -> (() => blob.commit.getName),
      "repository_id" -> (() => blob.repo),
      "reference_name" -> (() => blob.ref),
      "blob_id" -> (() => blob.id.getName),
      "content" -> (() => if (isBinary) Array.emptyByteArray else content),
      "is_binary" -> (() => isBinary)
    )
  }

}

case class Blob(id: ObjectId, commit: ObjectId, ref: String, repo: String)

object BlobIterator {
  /** Max bytes to read for the content of a file. */
  val readMaxBytes: Int = 20 * 1024 * 1024

  /**
    * Read max N bytes of the given blob
    *
    * @param objId  ID of the object to read
    * @param reader Object reader to use
    * @param max    maximum number of bytes to read in memory
    * @return Bytearray with the contents of the file
    */
  def readFile(objId: ObjectId, reader: ObjectReader, max: Integer = readMaxBytes): Array[Byte] = {
    val obj = reader.open(objId)
    val data = if (obj.isLarge) {
      val buf = Array.ofDim[Byte](max)
      val is = obj.openStream()
      is.read(buf)
      is.close()
      buf
    } else {
      obj.getBytes
    }
    reader.close()
    data
  }
}
