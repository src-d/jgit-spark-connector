package tech.sourced.engine

import org.scalatest.{FlatSpec, Matchers}

class FilterUDFSpec extends FlatSpec with Matchers with BaseSivaSpec with BaseSparkSpec {

  var engine: Engine = _

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    engine = Engine(ss, resourcePath, "siva")
  }

  "Filter by language" should "work properly" in {
    val langDf = engine
      .getRepositories
      .getReferences
      .getCommits
      .getBlobs
      .classifyLanguages

    val filteredLang = langDf.select("repository_id", "path", "lang").where("lang='Python'")
    filteredLang.count() should be(6)
  }

  override protected def afterAll(): Unit = {
    super.afterAll()
    engine = _: Engine
  }
}
