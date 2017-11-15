package tech.sourced.engine

import java.io.File
import java.nio.file.Files

import org.scalatest.{FlatSpec, Matchers}

class EngineSpec extends FlatSpec with Matchers with BaseSivaSpec with BaseSparkSpec {

  var engine: Engine = _

  override protected def beforeAll(): Unit = {
    super.beforeAll()

    engine = Engine(ss, resourcePath)
  }

  "backToMetadataSource" should "back to other sources" in {
    val tmpDir = Files.createTempDirectory("engine-back")

    engine.backToMetadataSource(
      "json",
      table => Map("path" -> new File(tmpDir.toString, table).getAbsolutePath)
    )

    val assertExistsBackup = (table: String) => {
      val f = new File(tmpDir.toString, table)
      f.exists() should be(true)
      f.isDirectory should be(true)
    }

    val assertCount = (table: String) => {
      val f = new File(tmpDir.toString, table)
      val df = ss.table(table)
      val jsonDf = ss.read.json(f.getAbsolutePath)
      jsonDf.count() should be(df.count())
    }

    val tables = Seq("repositories", "references", "commits")

    tables.foreach(assertExistsBackup)
    tables.foreach(assertCount)
  }

  "fromMetadataSource" should "use metadata source" in {
    val tmpDir = Files.createTempDirectory("engine-back")

    engine.backToMetadataSource(
      "json",
      table => Map("path" -> new File(tmpDir.toString, table).getAbsolutePath)
    )

    // if fromMetadataSource does not load the metadata, it will use the ones from
    // the DefaultSource and the result will be wrong
    ss.table("repositories").createOrReplaceTempView("repos_tmp")
    ss.table("commits").createOrReplaceTempView("repositories")
    ss.table("references").createOrReplaceTempView("commits")
    ss.table("repos_tmp").createOrReplaceTempView("references")

    val reposDf = engine.fromMetadataSource(
      "json",
      table => Map("path" -> new File(tmpDir.toString, table).getAbsolutePath)
    ).getRepositories
    reposDf.count() should be(5)

    val refsDf = reposDf.getReferences
    refsDf.count() should be(56)

    val commitsDf = refsDf.getCommits
    commitsDf.count() should be(4444)
  }

}
