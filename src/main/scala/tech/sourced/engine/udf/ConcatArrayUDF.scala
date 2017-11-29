package tech.sourced.engine.udf

import gopkg.in.bblfsh.sdk.v1.uast.generated.Node
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.{Column, SparkSession}
import tech.sourced.engine.util.Bblfsh


/** User defined function to concat array elements with the given separator. */
case object ConcatArrayUDF extends CustomUDF {

  override val name = "concatArray"

  override def function(session: SparkSession): UserDefinedFunction = {
    val configB = session.sparkContext.broadcast(Bblfsh.getConfig(session))
    udf[String, Seq[String], String]((arr, sep) => arr.mkString(sep))
  }

  def apply(arr: Column, sep: Column)(implicit session: SparkSession): Column = {
    function(session)(arr, sep)
  }

}
