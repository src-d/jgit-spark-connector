package tech.sourced.engine.udf

import gopkg.in.bblfsh.sdk.v1.uast.generated.Node
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.{Column, SparkSession}

/** User defined function to extract tokens from an UAST. */
case object ExtractTokensUDF extends CustomUDF {

  override val name = "extractTokens"

  override def function(session: SparkSession = null): UserDefinedFunction =
    udf[Seq[String], Seq[Array[Byte]]](extractTokens)

  def apply(uast: Column): Column = {
    function()(uast)
  }

  private def extractTokens(nodes: Seq[Array[Byte]]): Seq[String] = {
    if (nodes == null) {
      Seq()
    } else {
      nodes.map(Node.parseFrom).map(_.token)
    }
  }

}
