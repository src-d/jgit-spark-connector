package tech.sourced.api

trait LanguageDetector {
  def detect(path: String): String
  def detect(path: String, content: Array[Byte]): String
}

object EnryLanguageDetector extends EnryLanguageDetector

class EnryLanguageDetector extends LanguageDetector {

  override def detect(path: String): String = {
    // TODO: implement with Enry
    "Go"
  }

  override def detect(path: String, content: Array[Byte]): String = {
    // TODO: implement with Enry
    "Go"
  }

}
