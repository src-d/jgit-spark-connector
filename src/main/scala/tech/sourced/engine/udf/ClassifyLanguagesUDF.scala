package tech.sourced.engine.udf

import org.apache.spark.internal.Logging
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.{Column, SparkSession}
import tech.sourced.enry.Enry

/** User defined function to guess languages of files. */
case object ClassifyLanguagesUDF extends CustomUDF with Logging {

  override val name = "classifyLanguages"

  override def function(session: SparkSession = null): UserDefinedFunction =
    udf[Option[String], Boolean, String, Array[Byte]](getLanguage)

  def apply(isBinary: Column, path: Column, content: Column): Column = {
    function()(isBinary, path, content)
  }

  /**
    * Gets the language of the given file and returns the guessed language or none.
    *
    * @param isBinary whether it's a binary file or not
    * @param path     file path
    * @param content  file content
    * @return `None` if no language could be guessed, `Some(language)` otherwise.
    */
  private def getLanguage(isBinary: Boolean, path: String, content: Array[Byte]): Option[String] = {
    if (isBinary) {
      None
    } else {
      val lang = try {
        Enry.getLanguage(path, content)
      } catch {
        case e @ (_: RuntimeException | _: Exception) =>
          log.error(s"get language for file '$path' failed", e)
          null
      }
      if (null == lang || lang.isEmpty) None else Some(lang)
    }
  }

}
