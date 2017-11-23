package tech.sourced.engine.iterator

import org.apache.spark.{InterruptibleIterator, TaskContext}
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow

/**
  * Iterator that calls a cleanup function after the given iterator has
  * finished or an exception has been thrown.
  *
  * @param it      internal iterator
  * @param cleanup cleanup function
  * @tparam T type of the rows in the RootedRepoIterator
  */
class CleanupIterator[T](it: Iterator[InternalRow], cleanup: => Unit)
  extends InterruptibleIterator[InternalRow](TaskContext.get(), it) {

  /** @inheritdoc
    *
    * After catching an exception cleans up all the resources calling the cleanup function
    * and will rethrow such exception again.
    */
  override def hasNext: Boolean = {
    try {
      val hasNext = super.hasNext
      if (!hasNext) {
        val _ = cleanup
      }
      hasNext
    } catch {
      case e: Throwable =>
        val _ = cleanup
        throw e
    }
  }

  /** @inheritdoc*/
  override def next(): InternalRow = super.next()
}
