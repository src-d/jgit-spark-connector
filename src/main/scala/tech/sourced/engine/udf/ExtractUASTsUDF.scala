package tech.sourced.engine.udf

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.{Column, SparkSession}
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

/** Common entry point to use extraction UAST UDFs with or without language. */
object ExtractUASTsUDF {

  def apply(path: Column,
            content: Column,
            lang: Column = null)(
             implicit session: SparkSession): Column = {
    if (lang == null) {
      ExtractUASTsWithoutLangUDF(path, content)
    } else {
      ExtractUASTsWithLangUDF(path, content, lang)
    }
  }

}

/** User defined function to extract UASTs with no provided language for files. */
case object ExtractUASTsWithoutLangUDF extends CustomUDF with ExtractUASTsUDF {

  override val name = "extractUASTs"

  override def function(session: SparkSession): UserDefinedFunction = {
    val configB = session.sparkContext.broadcast(Bblfsh.getConfig(session))
    udf[Seq[Array[Byte]], String, Array[Byte]]((path, content) =>
      extractUASTs(path, content, config = configB.value))
  }

  def apply(path: Column,
            content: Column)(
             implicit session: SparkSession): Column = {
    function(session)(path, content)
  }

}

/** User defined function to extract UASTs providing files' language. */
case object ExtractUASTsWithLangUDF extends CustomUDF with ExtractUASTsUDF {

  override val name = "extractUASTsWithLang"

  override def function(session: SparkSession): UserDefinedFunction = {
    val configB = session.sparkContext.broadcast(Bblfsh.getConfig(session))
    udf[Seq[Array[Byte]], String, Array[Byte], String]((path, content, lang) =>
      extractUASTs(path, content, lang, configB.value))
  }

  def apply(path: Column,
            content: Column,
            lang: Column)(
             implicit session: SparkSession): Column = {
    function(session)(path, content, lang)
  }

}
