package tech.sourced.api.udf

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import tech.sourced.api.util.Bblfsh
import gopkg.in.bblfsh.sdk.v1.uast.generated.Node

object QueryXPathUDF extends CustomUDF {

  override val name = "queryXPath"

  override def function(session: SparkSession): UserDefinedFunction = {
    val config = Bblfsh.getConfig(session.sparkContext)
    val configB = session.sparkContext.broadcast(config)
    udf[Seq[Array[Byte]], Seq[Array[Byte]], String]((content, query) =>
      queryXPath(content, query, configB.value))
  }

  def apply(session: SparkSession, query: String): UserDefinedFunction = {
    val config = Bblfsh.getConfig(session.sparkContext)
    val configB = session.sparkContext.broadcast(config)
    val queryB = session.sparkContext.broadcast(query)
    udf[Seq[Array[Byte]], Seq[Array[Byte]]](content =>
      queryXPath(content, queryB.value, configB.value))
  }

  /**
    * Query an UAST using XPath.
    *
    * @param nodes  list of UAST nodes
    * @param query  XPath query
    * @param config bblfsh configuration
    * @return list of nodes as byte arrays matching the given query
    */
  def queryXPath(nodes: Seq[Array[Byte]],
                 query: String,
                 config: Bblfsh.Config): Seq[Array[Byte]] = {
    val client = Bblfsh.getClient(config)
    nodes.map(Node.parseFrom)
      .flatMap(client.filter(_, query).toIterator)
      .map(_.toByteArray)
  }

}
