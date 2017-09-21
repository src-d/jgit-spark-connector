package tech.sourced.api.udf

import org.apache.spark.sql.expressions.UserDefinedFunction

abstract class CustomUDF {
  val name: String
  val function: UserDefinedFunction
}
