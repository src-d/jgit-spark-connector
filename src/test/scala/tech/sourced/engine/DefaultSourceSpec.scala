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

    info("Files/blobs with commit hashes:\n")
    val blobsDf = references.getCommits.getBlobs.select(
      "path", "commit_hash"
    )
    out should be(37)
  }

  it should "return the remote branches renamed to refs/heads" in {
    val repoDir = tmpPath.resolve("repo")

    Git.cloneRepository()
      .setURI("https://github.com/src-d/engine.git")
      .setDirectory(repoDir.toFile)
      .call()

    val masters = Engine(ss, tmpPath.toString, "standard").getRepositories.getMaster.count()
    masters should be(2)
  }

  it should "match HEAD and not just refs/heads/HEAD" in {
    val repoDir = tmpPath.resolve("repo")

    import tech.sourced.engine.util.RepoUtils._

    val repo = createRepo(repoDir)
    commitFile(repo, "foo", "bar", "baz")

    Engine(ss, tmpPath.toString, "standard").getRepositories.getHEAD.count() should be(1)
  }

  override protected def afterAll(): Unit = {
    super.afterAll()

    FileUtils.deleteQuietly(tmpPath.toFile)
  }
}
