package tech.sourced.engine

import java.nio.file.{Path, Paths}
import java.util.UUID

import org.apache.commons.io.FileUtils
import org.eclipse.jgit.api.Git

class DefaultSourceSpec extends BaseSourceSpec("DefaultSource") {

  var tmpPath: Path = Paths.get(System.getProperty("java.io.tmpdir"), UUID.randomUUID.toString)

  override protected def beforeAll(): Unit = {
    super.beforeAll()

    tmpPath.toFile.mkdir()
  }

  "DefaultSource" should "not optimize if the conditions on the " +
    "join are not the expected ones" in {
    val repos = engine.getRepositories
    val references = ss.read.format("tech.sourced.engine").option("table", "references").load()
    val out = repos.join(references,
      (references("repository_id") === repos("id"))
        .and(references("name").startsWith("refs/pull"))
    ).count()

    val df = references.limit(1).getCommits
    df.count() should be(1)
  }

  it should "return the remote branches renamed to refs/heads" in {
    val repoDir = tmpPath.resolve("repo")

    Git.cloneRepository()
      .setURI("https://github.com/src-d/engine.git")
      .setDirectory(repoDir.toFile)
      .call()

    val engine = Engine(ss, tmpPath.toString, "standard")
    val masters = engine.getRepositories
      .getMaster
      .collect()
      .sortBy(_.getAs[String]("repository_id"))

    masters.length should be(2)
    masters(0).getAs[String]("repository_id") should startWith("file")
    masters(0).getAs[Boolean]("is_remote") should be(false)

    masters(1).getAs[String]("repository_id") should startWith("github")
    masters(1).getAs[Boolean]("is_remote") should be(true)

    engine.getRepositories.getRemoteReferences.getMaster.count() should be(1)
  }

  it should "match HEAD and not just refs/heads/HEAD" in {
    val repoDir = tmpPath.resolve("repo")

    import tech.sourced.engine.util.RepoUtils._

    val repo = createRepo(repoDir)
    commitFile(repo, "foo", "bar", "baz")

    Engine(ss, tmpPath.toString, "standard").getRepositories.getHEAD.count() should be(1)
  }

  it should "traverse all commits if it's not chained" in {
    val row = engine.session.sql("SELECT COUNT(*) FROM commits").first()
    row(0) should be(4444)

    val row2 = engine.session.sql("SELECT COUNT(*) FROM commits WHERE index > 0").first()
    row2(0) should be(4390)
  }

  override protected def afterAll(): Unit = {
    super.afterAll()

    FileUtils.deleteQuietly(tmpPath.toFile)
  }
}
