package tech.sourced.engine.iterator

import org.apache.spark.internal.Logging
import org.eclipse.jgit.diff.RawText
import org.eclipse.jgit.errors.MissingObjectException
import org.eclipse.jgit.lib.{ObjectId, Repository}
import tech.sourced.engine.exception.RepositoryException
import tech.sourced.engine.util.{CompiledFilter, Filters}

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
                   filters: Seq[CompiledFilter],
                   skipReadErrors: Boolean)
  extends ChainableIterator[Blob](
    finalColumns,
    Option(prevIter).orNull,
    filters,
    repo,
    skipReadErrors
  ) with Logging {

  /** @inheritdoc*/
  override protected def loadIterator(compiledFilters: Seq[CompiledFilter]): Iterator[Blob] = {
    val filters = Filters(compiledFilters)
    val treeEntryIter = Option(prevIter) match {
      case Some(it) =>
        Seq(it.currentRow).toIterator
      case None => GitTreeEntryIterator.loadIterator(
        repo,
        None,
        filters,
        blobIdKey = "blob_id"
      )
    }

    val iter = treeEntryIter.flatMap(entry => {
      if (repo.hasObject(entry.blob)) {
        Some(
          Blob(
            entry.blob,
            entry.commitHash,
            entry.ref,
            entry.repo,
            BlobIterator.readFile(
              entry.blob,
              repo
            )
          ))
      } else {
        None
      }
    })

    if (filters.hasFilters("blob_id")) {
      iter.filter(b => filters.matches(Seq("blob_id"), b.id.getName))
    } else {
      iter
    }
  }

  override protected def mapColumns(blob: Blob): RawRow = {
    val isBinary = RawText.isBinary(blob.content)

    Map[String, Any](
      "commit_hash" -> blob.commit.getName,
      "repository_id" -> blob.repo,
      "reference_name" -> blob.ref,
      "blob_id" -> blob.id.getName,
      "content" -> (if (isBinary) Array.emptyByteArray else blob.content),
      "is_binary" -> isBinary
    )
  }

}

case class Blob(id: ObjectId,
                commit: ObjectId,
                ref: String,
                repo: String,
                content: Array[Byte])

object BlobIterator extends Logging {
  /** Max bytes to read for the content of a file. */
  val readMaxBytes: Int = 20 * 1024 * 1024

  /**
    * Read max N bytes of the given blob
    *
    * @param objId ID of the object to read
    * @param repo  repository to get the data from
    * @param max   maximum number of bytes to read in memory
    * @return Bytearray with the contents of the file
    */
  def readFile(objId: ObjectId, repo: Repository, max: Integer = readMaxBytes): Array[Byte] = {
    val reader = repo.newObjectReader()
    val obj = try {
      reader.open(objId)
    } catch {
      case e: MissingObjectException =>
        log.warn(s"missing object", new RepositoryException(repo, e))
        null
    }

    if (obj != null) {
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
    } else {
      Array.emptyByteArray
    }
  }
}
