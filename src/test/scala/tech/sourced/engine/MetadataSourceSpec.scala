package tech.sourced.engine

import java.nio.file.{Path, Paths}
import java.util.UUID

import org.apache.commons.io.FileUtils

class MetadataSourceSpec extends BaseSourceSpec("MetadataSource") {

  private var tmpDir: Path = Paths.get(
    System.getProperty("java.io.tmpdir"),
    UUID.randomUUID().toString
  )

  override protected def beforeAll(): Unit = {
    super.beforeAll()

    tmpDir.toFile.mkdir()

    engine = Engine(ss, resourcePath, "siva")
    engine.saveMetadata(tmpDir.toString)
    engine = engine.fromMetadata(tmpDir.toString)
  }

  override protected def afterAll(): Unit = {
    super.afterAll()
    FileUtils.deleteQuietly(tmpDir.toFile)
  }

}
