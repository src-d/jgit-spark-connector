package tech.sourced.api.udf

import org.apache.spark.sql.functions.udf
import tech.sourced.enry.Enry


object ClassifyLanguagesUDF extends CustomUDF {
  val name = "classifyLanguages"
  val function = udf[Option[String], Boolean, String, Array[Byte]](getLanguage)

  def getLanguage(isBinary: Boolean, path: String, content: Array[Byte]): Option[String] =
    if (isBinary) {
      None
    } else {
      val lang = Enry.getLanguage(path, content)
      if (null == lang || lang.isEmpty) None else Some(lang)
    }
}
