package tech.sourced.api.udf

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.UserDefinedFunction

/**
  * Custom named user defined function.
  */
abstract class CustomUDF {
  /** Name of the function. */
  val name: String

  /** Function to execute when this function is called. */
  def function(session: SparkSession): UserDefinedFunction
}
