package tech.sourced.engine.iterator

import org.apache.spark.sql.Row
import tech.sourced.engine.util.CompiledFilter

import scala.annotation.tailrec

/**
  * Iterator that can have a previous iterator to output chained values.
  *
  * @param finalColumns final columns that must be in the resultant row
  * @param prevIter     previous iterator, if the iterator is chained
  * @param filters      filters for the iterator
  * @tparam T type of data returned by the internal iterator
  */
abstract class ChainableIterator[T](finalColumns: Array[String],
                                    prevIter: ChainableIterator[_],
                                    filters: Seq[CompiledFilter]) extends Iterator[Row] {

  /** Raw values of the row. */
  type RawRow = Map[String, Any]

  /** Instance of the internal iterator. */
  private var iter: Iterator[T] = _

  /** The current row of the prevIter, null always if there is no prevIter. */
  private var prevIterCurrentRow: RawRow = _

  /** The current row of the internal iterator. */
  private[iterator] var currentRow: T = _

  /**
    * Returns the internal iterator that will return the data used to construct the final row.
    *
    * @param filters filters for the iterator
    * @return internal iterator
    */
  protected def loadIterator(filters: Seq[CompiledFilter]): Iterator[T]

  /**
    * Loads the next internal iterator.
    *
    * @return internal iterator
    */
  private def loadIterator: Iterator[T] = loadIterator(filters)

  /**
    * Given the object returned by the internal iterator, this method must transform
    * that object into a RawRow.
    *
    * @param obj object returned by the internal iterator
    * @return raw row
    */
  protected def mapColumns(obj: T): RawRow

  //final private def isEmpty: Boolean = !hasNext

  /**
    * @inheritdoc
    */
  @tailrec
  final override def hasNext: Boolean = {
    // If there is no previous iter just load the iterator the first pass
    // and use hasNext of iter all the times. We return here to get rid of
    // this logic and assume from this point on that prevIter is not null
    if (prevIter == null) {
      if (iter == null) {
        iter = loadIterator
      }

      return iter.hasNext
    }

    // If the iter is not loaded, do so, but only if there are actually more
    // rows in the prev iter. If there are, just load the iter and preload
    // the prevIterCurrentRow.
    if (iter == null) {
      if (prevIter.isEmpty) {
        return false
      }

      prevIterCurrentRow = prevIter.nextRaw
      iter = loadIterator
    }

    // if iter is empty, we need to check if there are more rows in the prev iter
    // if not, just finish. If there are, preload the next raw row of the prev iter
    // and load the iterator again for the prev iter current row
    iter.hasNext || {
      if (prevIter.isEmpty) {
        return false
      }

      prevIterCurrentRow = prevIter.nextRaw
      iter = loadIterator

      // recursively check if it has more items, maybe there are no results for
      // this prevIter row but there are for the next
      hasNext
    }
  }

  override def next: Row = {
    currentRow = iter.next
    // FIXME: if there's a repeated column name, value
    // will be the last one added. This could be solved by
    // qualifying all column names with their source.
    val mappedValues = if (prevIterCurrentRow != null) {
      prevIterCurrentRow ++ mapColumns(currentRow)
    } else {
      mapColumns(currentRow)
    }

    val values = finalColumns.map(c => mappedValues(c))
    Row(values: _*)
  }


  def nextRaw: RawRow = {
    currentRow = iter.next
    val row = mapColumns(currentRow)
    if (prevIterCurrentRow != null) {
      prevIterCurrentRow ++ row
    } else {
      row
    }
  }
}
