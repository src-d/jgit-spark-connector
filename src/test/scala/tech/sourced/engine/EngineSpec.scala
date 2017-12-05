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
    Seq("repositories", "references", "commits", "tree_entries").foreach {
      table =>
        val count = getDataSource(table, ss).count()
        ss.read.jdbc(s"jdbc:sqlite:$dbFile", s"engine_$table", properties)
          .count() should be(count)
    }
  }

}
