package tech.sourced.engine.iterator

import org.apache.spark.internal.Logging
import org.eclipse.jgit.errors.{
  CorruptObjectException,
  IncorrectObjectTypeException,
  MissingObjectException
}
import org.eclipse.jgit.lib.{ObjectId, Repository}
import org.eclipse.jgit.treewalk.TreeWalk
import tech.sourced.engine.exception.RepositoryException
import tech.sourced.engine.util.{CompiledFilter, Filters}

abstract class TreeEntryIterator(finalColumns: Array[String],
                                 prevIter: CommitIterator,
                                 filters: Seq[CompiledFilter],
                                 repo: Repository,
                                 skipReadErrors: Boolean)
  extends ChainableIterator[TreeEntry](
    finalColumns,
    prevIter,
    filters,
    repo,
    skipReadErrors
  ) {
}

/**
  * Iterator that will return rows of commits in a repository.
  *
  * @param finalColumns final columns that must be in the resultant row
  * @param repo         repository to get the data from
  * @param prevIter     previous iterator, if the iterator is chained
  * @param filters      filters for the iterator
  */
class GitTreeEntryIterator(finalColumns: Array[String],
                           repo: Repository,
                           prevIter: CommitIterator,
                           filters: Seq[CompiledFilter],
                           skipReadErrors: Boolean)
  extends TreeEntryIterator(finalColumns, prevIter, filters, repo, skipReadErrors) {

  /** @inheritdoc */
  override protected def loadIterator(filters: Seq[CompiledFilter]): Iterator[TreeEntry] =
    GitTreeEntryIterator.loadIterator(
      repo,
      Option(prevIter).map(_.currentRow),
      Filters(filters)
    )

  /** @inheritdoc*/
  override protected def mapColumns(obj: TreeEntry): RawRow = {
    Map[String, Any](
      "commit_hash" -> obj.commitHash.getName,
      "reference_name" -> obj.ref,
      "repository_id" -> obj.repo,
      "path" -> obj.path,
      "blob" -> obj.blob.getName
    )
  }
}

/**
  * Iterator that will return rows of tree entries in a repository from metadata.
  * Even if the iterator that is loaded is only of TreeEntry, mapColumns returns
  * a RawRow with all other fields from repositories, references and commits
  * tables.
  *
  * @param finalColumns final columns that must be in the resultant row
  * @param iter         iterator of tree entries as RawRows
  */
class MetadataTreeEntryIterator(finalColumns: Array[String],
                                iter: Iterator[Map[String, Any]])
  extends TreeEntryIterator(finalColumns, null, Seq(), null, false) {

  // iter is converted to Iterator[TreeEntry] and mapColumns must receive a TreeEntry
  // in order to not lose all other columns that come from previous tables.
  // We can do this because we now that before an iteration of iter.map comes a call
  // to mapColumns, even though it is a hack, but it's the only way to fit this iterator
  // as a subclass of something of which GitTreeEntryIterator is also a subclass.
  private var lastRow: Map[String, Any] = _

  /** @inheritdoc*/
  override def loadIterator(filters: Seq[CompiledFilter]): Iterator[TreeEntry] =
    iter.map(row => {
      lastRow = row
      TreeEntry(
        ObjectId.fromString(row("commit_hash").toString),
        row("path").toString,
        ObjectId.fromString(row("blob").toString),
        row("reference_name").toString,
        row("repository_id").toString
      )
    })

  /** @inheritdoc*/
  override protected def mapColumns(obj: TreeEntry): RawRow = lastRow

}

case class TreeEntry(commitHash: ObjectId, path: String, blob: ObjectId, ref: String, repo: String)

object GitTreeEntryIterator extends Logging {

  /**
    * Returns an iterator of references with commit.
    *
    * @param repo      repository to get the data from
    * @param commit    the commit to get the entries from, if any
    * @param filters   filters to skip some rows. "hash" and "index" fields are supported
    *                  at iterator level. That means, any "hash" filter passed to this iterator
    *                  will make it only retrieve commits with the given hashes. Same for "index".
    * @param blobIdKey key of the blob ids
    * @return the iterator
    */
  def loadIterator(repo: Repository,
                   commit: Option[ReferenceWithCommit],
                   filters: Filters,
                   blobIdKey: String = "blob"): Iterator[TreeEntry] = {
    val commits = commit match {
      case Some(c) =>
        if (!filters.hasFilters("commit_hash")
          || filters.matches(Seq("commit_hash"), c.commit.getId.getName)) {
          Seq(c).toIterator
        } else {
          Seq().toIterator
        }
      case None => CommitIterator.loadIterator(
        repo,
        None,
        filters,
        hashKey = "commit_hash"
      )
    }

    var iter: Iterator[TreeEntry] = commits.flatMap(c => getTreeEntries(repo, c))
    if (filters.hasFilters("path")) {
      iter = iter.filter(te => filters.matches(Seq("path"), te.path))
    }

    val blobKeys = Seq("blob", blobIdKey)
    if (filters.hasFilters(blobKeys: _*)) {
      iter = iter.filter(te => filters.matches(blobKeys, te.blob.getName))
    }

    iter
  }

  /**
    * Retrieves the tree entries for a commit.
    *
    * @param repo Repository
    * @param c    commit
    * @return iterator of tree entries
    */
  private def getTreeEntries(repo: Repository, c: ReferenceWithCommit): Iterator[TreeEntry] = {
    val treeWalk: TreeWalk = new TreeWalk(repo)
    val nth: Int = treeWalk.addTree(c.commit.getTree.getId)
    treeWalk.setRecursive(false)
    val commitId = c.commit.getId
    val (repoName, refName) = RootedRepo.parseRef(repo, c.ref.getName)

    Stream.continually(treeWalk)
      .takeWhile(_.next())
      .flatMap(tree => {
        try {
          if (tree.isSubtree) {
            tree.enterSubtree()
          }

          tree :: Nil
        } catch {
          case e: IncorrectObjectTypeException =>
            log.debug(s"incorrect object found", new RepositoryException(repo, e))
            Nil
          case e: MissingObjectException =>
            log.warn(s"missing object for", new RepositoryException(repo, e))
            Nil
          case e: CorruptObjectException =>
            log.warn(s"corrupt object", new RepositoryException(repo, e))
            Nil
        }
      })
      .filter(!_.isSubtree)
      .map(tree => TreeEntry(
        commitId,
        new String(tree.getRawPath),
        tree.getObjectId(nth),
        refName,
        repoName
      ))
      .toIterator
  }

}
