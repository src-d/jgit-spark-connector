package tech.sourced.api.iterator

import org.apache.spark.{InterruptibleIterator, TaskContext}
import org.apache.spark.sql.Row

class CleanupIterator[T](it: RootedRepoIterator[T], cleanup: => Unit)
  extends InterruptibleIterator[Row](TaskContext.get(), it) {

  override def hasNext: Boolean = {
    try {
      val hasNext = super.hasNext
      if (!hasNext) {
        val _ = cleanup
      }
      hasNext
    } catch {
      case e: Throwable => {
        val _ = cleanup
        throw e
      }
    }
  }

  override def next(): Row = super.next()
}
