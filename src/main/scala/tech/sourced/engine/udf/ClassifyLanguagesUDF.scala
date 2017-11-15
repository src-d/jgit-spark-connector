package tech.sourced.engine.udf

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.{Column, SparkSession}
import tech.sourced.enry.Enry

/** User defined function to guess languages of files. */
case object ClassifyLanguagesUDF extends CustomUDF {

  override val name = "classifyLanguages"

  override def function(session: SparkSession = null): UserDefinedFunction =
    udf[Option[String], Boolean, String, Array[Byte]](getLanguage)

  def apply(isBinary: Column, path: Column, content: Column): Column = {
    function()(isBinary, path, content)
  }

  private def getLanguage(isBinary: Boolean, path: String, content: Array[Byte]): Option[String] = {
    if (isBinary) {
      None
    } else {
      val lang = Enry.getLanguage(path, content)
      if (null == lang || lang.isEmpty) None else Some(lang)
    }
  }

}
