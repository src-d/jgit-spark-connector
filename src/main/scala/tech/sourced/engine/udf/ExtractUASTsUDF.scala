package tech.sourced.engine.udf

import org.apache.spark.sql.Column
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import tech.sourced.engine.util.Bblfsh

trait ExtractUASTsUDF {

  def extractUASTs(path: String,
                   content: Array[Byte],
                   lang: String = null): Seq[Array[Byte]] = {
    if (content == null || content.isEmpty) {
      Seq()
    } else {
      Bblfsh.extractUAST(path, content, lang)
    }
  }

}

/**
  * Common entry point to use extraction UAST UDFs with or without language.
  */
object ExtractUASTsUDF {

  def apply(path: Column,
            content: Column,
            lang: Column = null): Column = {
    if (lang == null) {
      ExtractUASTsWithoutLangUDF(path, content)
    } else {
      ExtractUASTsWithLangUDF(path, content, lang)
    }
  }

}

/**
  * User defined function to extract UASTs with no provided language for files.
  */
case object ExtractUASTsWithoutLangUDF extends CustomUDF with ExtractUASTsUDF {

  override val name = "extractUASTs"

  override def function: UserDefinedFunction = {
    udf[Seq[Array[Byte]], String, Array[Byte]]((path, content) =>
      extractUASTs(path, content))
  }

  def apply(path: Column,
            content: Column): Column = {
    function(path, content)
  }

}

/**
  * User defined function to extract UASTs providing files' language.
  */
case object ExtractUASTsWithLangUDF extends CustomUDF with ExtractUASTsUDF {

  override val name = "extractUASTsWithLang"

  override def function: UserDefinedFunction = {
    udf[Seq[Array[Byte]], String, Array[Byte], String]((path, content, lang) =>
      extractUASTs(path, content, lang))
  }

  def apply(path: Column,
            content: Column,
            lang: Column): Column = {
    function(path, content, lang)
  }

}
