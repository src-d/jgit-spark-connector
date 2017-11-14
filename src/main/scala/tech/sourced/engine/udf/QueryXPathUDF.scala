package tech.sourced.engine.udf

import gopkg.in.bblfsh.sdk.v1.uast.generated.Node
import org.apache.spark.sql.Column
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import tech.sourced.engine.util.Bblfsh


/**
  * User defined function to perform XPath queries on UASTs.
  */
case object QueryXPathUDF extends CustomUDF {

  override val name = "queryXPath"

  override def function: UserDefinedFunction = {
    udf[Seq[Array[Byte]], Seq[Array[Byte]], String](queryXPath)
  }

  def apply(uast: Column, query: Column): Column = {
    function(uast, query)
  }

  private def queryXPath(nodes: Seq[Array[Byte]],
                         query: String): Seq[Array[Byte]] = {
    if (nodes == null) {
      return null
    }

    nodes.map(Node.parseFrom).flatMap(n => {
      val result = Bblfsh.filter(n, query)
      if (result == null) {
        None
      } else {
        result.toIterator
      }
    }).map(_.toByteArray)
  }

}
