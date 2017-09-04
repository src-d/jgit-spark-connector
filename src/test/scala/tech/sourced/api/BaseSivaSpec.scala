package tech.sourced.api

trait BaseSivaSpec {
  val resourcePath: String = getClass.getResource("/siva-files").toString
}
