package tech.sourced.engine.provider

import java.io.File
import java.nio.file.Paths
import java.util.UUID

import org.apache.commons.io.FileUtils
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import tech.sourced.engine.{BaseSivaSpec, BaseSparkSpec}
import tech.sourced.engine.util.RepoUtils._

class RepositoryRDDProviderSpec extends FlatSpec with BeforeAndAfterAll
  with Matchers with BaseSivaSpec with BaseSparkSpec {

  private var rootDir = Paths.get(
    System.getProperty("java.io.tmpdir"),
    UUID.randomUUID().toString + "-repos-rdd"
  )

  private var bareRepoDir = rootDir.resolve("bare-repo")
  private var regularRepoDir = rootDir.resolve("regular-repo")
  private var randomDir = rootDir.resolve("some-dir")
  private var bucketedSivaDir = rootDir.resolve("bucket")

  override def beforeAll(): Unit = {
    super.beforeAll()

    rootDir.toFile.mkdir()

    val bareRepo = createBareRepo(bareRepoDir)
    val regularRepo = createRepo(regularRepoDir)

    addRemote(bareRepo, "bare-repo", "git@github.com:repos/bare.git")
    addRemote(regularRepo, "regular-repo", "git@github.com:repos/regular.git")
    addRemote(regularRepo, "another-remote", "git@github.com:repos/regular-2.git")

    FileUtils.write(regularRepoDir.resolve("README.md").toFile, "hello world")
    regularRepo.add().addFilepattern("README.md").call()
    regularRepo.commit().setMessage("first commit on regular repo").call()

    randomDir.toFile.mkdir()
    randomDir.resolve("somefile.txt").toFile.createNewFile()


    val sivaFiles = new File(resourcePath.substring(5))
      .listFiles()
      .toList
      .filter(_.getAbsolutePath.endsWith(".siva"))

    if (sivaFiles.length < 2) {
      throw new IllegalStateException("there should be at least two siva files in test resources")
    }

    FileUtils.copyFile(sivaFiles.head, rootDir.resolve(sivaFiles.head.getName).toFile, true)
    FileUtils.copyFile(sivaFiles(1), randomDir.resolve(sivaFiles(1).getName).toFile, true)
  }

  "RepositoryRDDProvider" should "return all repositories in given path" in {
    val rdd = RepositoryRDDProvider(ss.sparkContext).get(rootDir.toString)
    val sources = rdd.collect()
    sources.length should be(4)

    val gitRepos = sources.filter { case _: GitRepository => true; case _ => false }
    gitRepos.length should be(1)
    gitRepos.head.asInstanceOf[GitRepository].root.substring(5) should be(regularRepoDir.toString)

    val bareRepos = sources.filter { case _: BareRepository => true; case _ => false }
    bareRepos.length should be(1)
    bareRepos.head.asInstanceOf[BareRepository].root.substring(5) should be(bareRepoDir.toString)

    val sivaRepos = sources.filter { case _: SivaRepository => true; case _ => false }
    sivaRepos.length should be(2)
  }

  override def afterAll(): Unit = {
    FileUtils.deleteQuietly(rootDir.toFile)
  }

}
