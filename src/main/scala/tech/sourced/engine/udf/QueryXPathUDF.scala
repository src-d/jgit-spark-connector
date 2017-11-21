package tech.sourced.engine.udf

import gopkg.in.bblfsh.sdk.v1.uast.generated.Node
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.{Column, SparkSession}
import tech.sourced.engine.util.Bblfsh


/** User defined function to perform XPath queries on UASTs. */
case object QueryXPathUDF extends CustomUDF {

  override val name = "queryXPath"

  override def function(session: SparkSession): UserDefinedFunction = {
    val configB = session.sparkContext.broadcast(Bblfsh.getConfig(session))
    udf[Seq[Array[Byte]], Seq[Array[Byte]], String]((nodes, query) =>
      queryXPath(nodes, query, configB.value))
  }

  def apply(uast: Column, query: Column)(implicit session: SparkSession): Column = {
    function(session)(uast, query)
  }

  private def queryXPath(nodes: Seq[Array[Byte]],
                         query: String,
                         config: Bblfsh.Config): Seq[Array[Byte]] = {
    if (nodes == null) {
      return null
    }

    nodes.map(Node.parseFrom).flatMap(n => {
      val result = Bblfsh.filter(n, query, config)
      if (result == null) {
        None
      } else {
        result.toIterator
      }
    }).map(_.toByteArray)
  }

}
