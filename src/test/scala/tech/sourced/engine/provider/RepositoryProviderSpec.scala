package tech.sourced.engine.provider

import java.nio.file.Paths
import java.util.UUID

import org.apache.commons.io.FileUtils
import org.apache.hadoop.fs.{FileSystem, Path}
import org.eclipse.jgit.lib.ObjectId
import org.scalatest.{BeforeAndAfterEach, FlatSpec, Matchers}
import tech.sourced.engine.{BaseSivaSpec, BaseSparkSpec}

import scala.collection.JavaConverters._

class RepositoryProviderSpec
  extends FlatSpec
    with Matchers
    with BaseSivaSpec
    with BaseSparkSpec
    with BeforeAndAfterEach {

  private var tmpDir: java.nio.file.Path = _

  override def beforeEach(): Unit = {
    super.beforeEach()
    tmpDir = Paths.get(System.getProperty("java.io.tmpdir"), UUID.randomUUID().toString)
    tmpDir.toFile.mkdir()
  }

  override def afterEach(): Unit = {
    super.afterEach()
    FileUtils.deleteQuietly(tmpDir.toFile)
  }

  "RepositoryRDDProvider" should "return always the same instance" in {
    val prov = RepositoryRDDProvider(ss.sparkContext)
    val prov2 = RepositoryRDDProvider(ss.sparkContext)

    prov should equal(prov2)
    prov should not equal new RepositoryRDDProvider(ss.sparkContext)

  }

  "RepositoryRDDProvider" should "return the exact name of siva files" in {
    val prov = RepositoryRDDProvider(ss.sparkContext)

    val reposRDD = prov.get(resourcePath)

    reposRDD.count() should be(3)
  }

  "RepositoryProvider" should "read correctly siva repositories" in {
    val prov = RepositoryRDDProvider(ss.sparkContext)

    val reposRDD = prov.get(resourcePath)

    val refs = reposRDD.flatMap(pds => {
      val repo = RepositoryProvider("/tmp").get(pds)

      repo.getAllRefs.asScala.mapValues(r => ObjectId.toString(r.getPeeledObjectId))
    }).collect()

    refs.length should be(56)
  }

  "RepositoryProvider" should "not delete siva file with skipCleanup = true" in {
    val prov = RepositoryRDDProvider(ss.sparkContext)
    val reposRDD = prov.get(resourcePath)
    val sivaFilesExist = reposRDD.map(source => {
      val provider = RepositoryProvider("/tmp", skipCleanup = true)
      val repo = provider.get(source)
      val localSivaPath = new Path("/tmp",
        new Path(RepositoryProvider.temporalSivaFolder, source.pds.getPath()))
      provider.close(source, repo)
      FileSystem.get(source.pds.getConfiguration).exists(localSivaPath)
    }).collect()

    assert(sivaFilesExist.length == 3)
    assert(sivaFilesExist.forall(_ == true))
  }

  "RepositoryProvider" should "delete siva file with skipCleanup = false" in {
    val prov = RepositoryRDDProvider(ss.sparkContext)

    val reposRDD = prov.get(resourcePath)

    val sivaFilesExist = reposRDD.map(source => {
      val provider = new RepositoryProvider("/tmp/two")
      val repo = provider.get(source)
      provider.close(source, repo)
      val localSivaPath = new Path("/tmp/two",
        new Path(RepositoryProvider.temporalSivaFolder, new Path(source.pds.getPath()).getName))
      FileSystem.get(source.pds.getConfiguration).exists(localSivaPath)
    }).collect()

    assert(sivaFilesExist.length == 3)
    assert(sivaFilesExist.forall(!_))
  }

  "RepositoryProvider" should "cleanup unpacked files when nobody else is using the repo" in {
    val prov = RepositoryRDDProvider(ss.sparkContext)
    val reposRDD = prov.get(resourcePath)
    val source = reposRDD.first()

    // needs to be a fresh instance, since some of the tests may not cleanup
    val provider = new RepositoryProvider("/tmp/cleanup-test-" + System.currentTimeMillis())

    val repo1 = provider.get(source)
    val fs = FileSystem.get(source.pds.getConfiguration)
    val repo2 = provider.get(source)

    provider.close(source, repo1)
    fs.exists(new Path(repo1.getDirectory.toString)) should be(true)

    provider.close(source, repo2)
    fs.exists(new Path(repo2.getDirectory.toString)) should be(false)
  }

  "RepositoryProvider with skipCleanup = true"
    .should("not cleanup unpacked files when nobody else is using the repo").in({
    val prov = RepositoryRDDProvider(ss.sparkContext)
    val reposRDD = prov.get(resourcePath)
    val source = reposRDD.first()

    // needs to be a fresh instance, since some of the tests may not cleanup
    val provider = new RepositoryProvider("/tmp/cleanup-test-"
      + System.currentTimeMillis(), skipCleanup = true)

    val repo = provider.get(source)
    val fs = FileSystem.get(source.pds.getConfiguration)
    val repo2 = provider.get(source)

    provider.close(source, repo)
    repo.getDirectory.toPath
    fs.exists(new Path(repo.getDirectory.toString)) should be(true)

    provider.close(source, repo2)
    fs.exists(new Path(repo2.getDirectory.toString)) should be(true)
  })

  "RepositoryProvider" should "return a repository given a SivaRepository" in {
    val prov = RepositoryRDDProvider(ss.sparkContext)
    val reposRDD = prov.get(resourcePath)
    val source = reposRDD.first()

    val provider = RepositoryProvider(tmpDir.toString)

    val repo = provider.get(RepositoryProvider.keyForSource(source))
    repo.getAllRefs.size should not(be(0))
  }

  "RepositoryProvider" should "return a repository given a BareRepository" in {
    import tech.sourced.engine.util.RepoUtils._

    val bareRepo = createBareRepo(tmpDir.resolve("bare-repo"))

    addRemote(bareRepo, "bare-repo", "git@github.com:bare/repo.git")

    val prov = RepositoryRDDProvider(ss.sparkContext)
    val reposRDD = prov.get(tmpDir.toString)
    val source = reposRDD.first()

    tmpDir.resolve("tmp").toFile.mkdir()
    val provider = RepositoryProvider(tmpDir.resolve("tmp").toString)

    val repo = provider.get(RepositoryProvider.keyForSource(source))
    repo.getRemoteNames.size should be(1)
  }

  "RepositoryProvider" should "return a repository given a GitRepository" in {
    import tech.sourced.engine.util.RepoUtils._

    val gitRepo = createRepo(tmpDir.resolve("repo"))

    addRemote(gitRepo, "repo", "git@github.com:git/repo.git")

    FileUtils.write(tmpDir.resolve("repo").resolve("README.md").toFile, "hello world")
    gitRepo.add().addFilepattern("README.md").call()
    gitRepo.commit().setMessage("first commit on regular repo").call()

    val prov = RepositoryRDDProvider(ss.sparkContext)
    val reposRDD = prov.get(tmpDir.toString)
    val source = reposRDD.first()

    tmpDir.resolve("tmp").toFile.mkdir()
    val provider = RepositoryProvider(tmpDir.resolve("tmp").toString)

    val repo = provider.get(RepositoryProvider.keyForSource(source))
    repo.getRemoteNames.size should be(1)
  }

}
