package tech.sourced.api.udf

import org.apache.spark.sql.expressions.UserDefinedFunction

/**
  * Custom named user defined function.
  */
abstract class CustomUDF {
  /** Name of the function. */
  val name: String

  /** Function to execute when this function is called. */
  val function: UserDefinedFunction
}
