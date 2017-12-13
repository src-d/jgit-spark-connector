package tech.sourced.engine

import java.nio.file.{Path, Paths}
import java.util.{Properties, UUID}

import org.apache.commons.io.FileUtils
import org.scalatest.{FlatSpec, Matchers}

class EngineSpec extends FlatSpec with Matchers with BaseSivaSpec with BaseSparkSpec {

  var engine: Engine = _
  var tmpPath: Path = _

  override protected def beforeAll(): Unit = {
    super.beforeAll()

    engine = Engine(ss, resourcePath)
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

    val reposDf = getDataSource(RepositoriesTable, ss)
    val refsDf = getDataSource(ReferencesTable, ss)
    val repoHasCommitsDf = getDataSource(CommitsTable, ss)
      .select("reference_name", "repository_id", "hash", "index")
    val commitsDf = getDataSource(CommitsTable, ss)
      .drop("index", "reference_name", "repository_id")
      .distinct()
    val treeEntriesDf = getDataSource(TreeEntriesTable, ss)
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

}
