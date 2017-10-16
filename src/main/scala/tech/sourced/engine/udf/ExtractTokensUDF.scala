package tech.sourced.engine.udf

import gopkg.in.bblfsh.sdk.v1.uast.generated.Node
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

object ExtractTokensUDF extends CustomUDF {

  override val name = "extractTokens"

  override def function(session: SparkSession): UserDefinedFunction =
    udf[Seq[String], Seq[Array[Byte]]](extractTokens)

  def apply(): UserDefinedFunction = udf[Seq[String], Seq[Array[Byte]]](extractTokens)

  /**
    * Extracts the string token representation of the nodes.
    *
    * @param nodes nodes to extract tokens from
    * @return extracted tokens
    */
  def extractTokens(nodes: Seq[Array[Byte]]): Seq[String] = {
    if (nodes == null) {
      Seq()
    } else {
      nodes.map(Node.parseFrom).map(_.token)
    }
  }

}
