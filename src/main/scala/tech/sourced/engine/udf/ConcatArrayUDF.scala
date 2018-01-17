package tech.sourced.engine.udf

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf


/** User defined function to concat array elements with the given separator. */
case object ConcatArrayUDF extends CustomUDF {

  override val name = "concatArray"

  override def apply(session: SparkSession): UserDefinedFunction = {
    udf[String, Seq[String], String]((arr, sep) => arr.mkString(sep))
  }

}
