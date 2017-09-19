package tech.sourced.api.customudf

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import tech.sourced.enry.Enry


abstract class CustomUDF {
  val name: String
  val function: UserDefinedFunction
}

object ClassifyLanguagesUDF extends CustomUDF {
  val name = "classifyLanguages"
  val function = udf[Option[String], Boolean, String, Array[Byte]](getLanguage)

  private def getLanguage(isBinary: Boolean, path: String, content: Array[Byte]): Option[String] = if (isBinary) {
    None
  } else {
    val lang = Enry.getLanguage(path, content)
    if (lang != "") Some(lang) else None
  }
}
