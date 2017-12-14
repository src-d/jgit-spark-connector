package tech.sourced.engine

class DefaultSourceSpec extends BaseSourceSpec("DefaultSource") {
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
}
