package tech.sourced.engine.udf

import gopkg.in.bblfsh.sdk.v1.uast.generated.Node
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

/** User defined function to extract tokens from an UAST. */
case object ExtractTokensUDF extends CustomUDF {

  override val name = "extractTokens"

  override def apply(session: SparkSession): UserDefinedFunction =
    udf[Seq[String], Seq[Array[Byte]]](extractTokens)

  private def extractTokens(nodes: Seq[Array[Byte]]): Seq[String] = {
    timer.time({
      if (nodes == null) {
        Seq()
      } else {
        nodes.map(Node.parseFrom).map(_.token)
      }
    })
  }

}
