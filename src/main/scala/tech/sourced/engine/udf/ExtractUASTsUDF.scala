package tech.sourced.engine.udf

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import tech.sourced.engine.util.Bblfsh

trait ExtractUASTsUDF {

  def extractUASTs(path: String,
                   content: Array[Byte],
                   lang: String = null,
                   config: Bblfsh.Config): Seq[Array[Byte]] = {
    if (content == null || content.isEmpty) {
      Seq()
    } else {
      Bblfsh.extractUAST(path, content, lang, config)
    }
  }

}

/** Common entry point to use extraction UAST UDFs with or without language parameter. */
case object ExtractUASTsUDF extends CustomUDF with ExtractUASTsUDF {

  override val name = "extractUASTs"

  override def apply(session: SparkSession): UserDefinedFunction = {
    val configB = session.sparkContext.broadcast(Bblfsh.getConfig(session))
    udf[Seq[Array[Byte]], String, Array[Byte], String]((path, content, lang) =>
      extractUASTs(path, content, lang, configB.value))
  }

}
