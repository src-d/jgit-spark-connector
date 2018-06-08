package tech.sourced.engine

import java.nio.file.{Path, Paths}
import java.util.{Properties, UUID}

import org.apache.commons.io.FileUtils
import org.apache.spark.SparkException
import org.scalatest.{FlatSpec, Matchers}

class EngineSpec extends FlatSpec with Matchers with BaseSivaSpec with BaseSparkSpec {

  var engine: Engine = _
  var tmpPath: Path = _

  override protected def beforeAll(): Unit = {
    super.beforeAll()

    engine = Engine(ss, resourcePath, "siva")
    tmpPath = Paths.get(System.getProperty("java.io.tmpdir"))
      .resolve(UUID.randomUUID.toString)
    tmpPath.toFile.mkdir()
  }

  override def afterAll(): Unit = {
    super.afterAll()
    FileUtils.deleteQuietly(tmpPath.toFile)
  }

  "saveMetadata" should "store all metadata tables in a SQLite db" in {
    engine.saveMetadata(tmpPath.toString)

    val dbFile = tmpPath.resolve("engine_metadata.db")
    dbFile.toFile.exists should be(true)

    val properties = new Properties()
    properties.put("driver", "org.sqlite.JDBC")

    val reposDf = engine.getRepositories
    val refsDf = reposDf.getReferences
    val repoHasCommitsDf = refsDf.getAllReferenceCommits
      .select("reference_name", "repository_id", "hash", "index")
    val commitsDf = refsDf.getAllReferenceCommits
      .drop("index", "reference_name", "repository_id")
      .distinct()
    val treeEntriesDf = refsDf.getAllReferenceCommits.getTreeEntries
      .drop("reference_name", "repository_id")
      .distinct()

    Seq(
      (RepositoriesTable, reposDf),
      (ReferencesTable, refsDf),
      (RepositoryHasCommitsTable, repoHasCommitsDf),
      (CommitsTable, commitsDf),
      (TreeEntriesTable, treeEntriesDf)
    ).foreach {
      case (table, df) =>
        val count = df.count()
        ss.read.jdbc(s"jdbc:sqlite:$dbFile", Tables.prefix(table), properties)
          .count() should be(count)
    }
  }

  "skipReadErrors" should "skip all read errors" in {
    val resourcePath = getClass.getResource("/bad-siva-files").toString
    val engine = Engine(ss, resourcePath, "siva").skipReadErrors(true)
    val tmpPath = Paths.get(System.getProperty("java.io.tmpdir"))
      .resolve(UUID.randomUUID.toString)
    tmpPath.toFile.mkdir()

    val cnt = engine
      .getRepositories
      .getReferences
      .getCommits
      .getTreeEntries
      .getBlobs
      .count()

    cnt should be(8663)

    FileUtils.deleteQuietly(tmpPath.toFile)
  }

  "engine" should "throw an error when a siva file contains a zip-slip vulnerability" in {
    val resourcePath = getClass.getResource("/zip-slip-siva-files").toString
    val engine = Engine(ss, resourcePath, "siva")
    val tmpPath = Paths.get(System.getProperty("java.io.tmpdir"))
      .resolve(UUID.randomUUID.toString)
    tmpPath.toFile.mkdir()

    val ex = intercept[SparkException] {
      engine
        .getRepositories
        .getReferences
        .getCommits
        .getTreeEntries
        .getBlobs
        .count()
    }

    ex.getCause.getMessage should be("Entry is outside of the target dir: objects/../../imoutside")

    FileUtils.deleteQuietly(tmpPath.toFile)
  }

}
