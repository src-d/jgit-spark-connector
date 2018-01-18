package tech.sourced.engine.udf

import org.apache.spark.groupon.metrics.{NotInitializedException, SparkTimer, UserMetricsSystem}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.UserDefinedFunction

/**
  * Custom named user defined function.
  */
abstract class CustomUDF {
  /** timer intended to be used on UDF logic **/
  lazy protected val timer: SparkTimerUDFWrapper = new SparkTimerUDFWrapper(name)

  /** Name of the function. */
  val name: String

  /** Function to execute when this function is called. */
  def apply(session: SparkSession): UserDefinedFunction

  def apply(): UserDefinedFunction = this.apply(session = null)
}

sealed class SparkTimerUDFWrapper(name: String) extends Logging {
  lazy val timer: SparkTimer = init()

  private def init(): SparkTimer = {
    try {
      UserMetricsSystem.timer(name)
    } catch {
      case _: NotInitializedException => {
        logWarning("SparMetric not initialized on UDF")
        null
      }
    }

  }

  def time[T](f: => T): T =
    if (timer == null) {
      f
    } else {
      timer.time(f)
    }
}
