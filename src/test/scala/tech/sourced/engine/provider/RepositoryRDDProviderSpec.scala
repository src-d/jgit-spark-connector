package tech.sourced.engine.provider

import java.nio.file.{Path, Paths}
import java.util.UUID

import org.apache.commons.io.FileUtils
import org.scalatest.{BeforeAndAfterEach, FlatSpec, Matchers}
import tech.sourced.engine.util.RepoUtils
import tech.sourced.engine.{BaseSivaSpec, BaseSparkSpec}

class RepositoryRDDProviderSpec extends FlatSpec with Matchers with BeforeAndAfterEach
  with BaseSparkSpec with BaseSivaSpec {

  private var provider: RepositoryRDDProvider = _
  private var tmpPath: Path = _

  override def beforeEach(): Unit = {
    super.beforeEach()
    provider = RepositoryRDDProvider(ss.sparkContext)
    tmpPath = Paths.get(
      System.getProperty("java.io.tmpdir"),
      UUID.randomUUID().toString
    )
  }

  override def afterEach(): Unit = {
    super.afterEach()

    FileUtils.deleteQuietly(tmpPath.toFile)
  }

  "RepositoryRDDProvider" should "retrieve bucketized raw repositories" in {
    tmpPath.resolve("a").toFile.mkdir()
    createRepo(tmpPath.resolve("a").resolve("repo"))

    tmpPath.resolve("b").toFile.mkdir()
    createRepo(tmpPath.resolve("b").resolve("repo"))

    createRepo(tmpPath.resolve("repo"))

    val repos = provider.get(tmpPath.toString, "standard").collect()
    repos.length should be(3)
  }

  it should "retrieve non-bucketized raw repositories" in {
    tmpPath.resolve("a").toFile.mkdir()
    createRepo(tmpPath.resolve("repo"))

    tmpPath.resolve("b").toFile.mkdir()
    createRepo(tmpPath.resolve("repo2"))

    val repos = provider.get(tmpPath.toString, "standard").collect()
    repos.length should be(2)
  }

  it should "retrieve bucketized siva repositories" in {
    val repos = provider.get(resourcePath, "siva").collect()
    repos.length should be(3)
  }

  it should "retrieve non-bucketized siva repositories" in {
    val repos = provider.get(Paths.get(resourcePath, "ff").toString, "siva").collect()
    repos.length should be(1)
  }

  private def createRepo(path: Path) = {
    val repo = RepoUtils.createRepo(path)
    RepoUtils.commitFile(repo, "file.txt", "something something", "some commit")
  }

}
