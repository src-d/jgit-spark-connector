package tech.sourced.engine

import org.scalatest._

class DefaultSourceSpec extends FlatSpec with Matchers with BaseSivaSpec with BaseSparkSpec {

  var engine: Engine = _

  override protected def beforeAll(): Unit = {
    super.beforeAll()

    engine = Engine(ss, resourcePath)
  }

  "Default source" should "get heads of all repositories and count the files" in {
    val df = engine.getRepositories
      .getHEAD
      .getCommits
      .getFirstReferenceCommit
      .getTreeEntries
      .getBlobs
      .select("commit_hash", "path", "content", "is_binary")
      .distinct()
    df.show(457)
    df.count should be(457)
  }

  it should "count all the commit messages from all masters that are not forks" in {
    val commits = engine.getRepositories.filter("is_fork = false").getMaster.getCommits
    val df = commits.select("message").filter(commits("message").startsWith("a"))
    df.show(false)
    df.count should be(7)
  }

  it should "count all commits messages from all references that are not forks" in {
    val commits = engine.getRepositories.filter("is_fork = false").getReferences.getCommits
    val df = commits.select("message", "reference_name", "hash").
      filter(commits("message").startsWith("a"))
    df.show
    df.count should be(98)
  }

  it should "get all files from HEADS that are Ruby" in {
    val blobs = engine.getRepositories.filter("is_fork = false")
      .getHEAD
      .getCommits
      .getFirstReferenceCommit
      .getTreeEntries
      .getBlobs
      .classifyLanguages
    val df = blobs.filter(blobs("lang") === "Ruby").select("lang", "path")
    df.explain(true)
    df.show(453, truncate = false)
    df.count should be(169)
  }

  it should "not optimize if the conditions on the " +
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
    blobsDf.explain(true)
    blobsDf.show()

    out should be(37)
  }

  it should "get all tree entries" in {
    val df = engine.getRepositories.getReferences.getCommits.getTreeEntries
    df.count() should be(304362)
  }

  "Convenience for getting files" should "work without reading commits" in {
    val spark = ss
    import spark.implicits._

    val blobsDf = engine
      .getRepositories.filter($"id" === "github.com/mawag/faq-xiyoulinux")
      .getReferences.getHEAD
      .getCommits.getBlobs
      .select(
        "path",
        "commit_hash",
        "blob_id",
        "content",
        "is_binary"
      )

    val cnt = blobsDf.count()
    info(s"Total $cnt rows")
    cnt should be(2)

    info("UAST for files:\n")
    val filesCols = blobsDf.columns.length
    val uasts = blobsDf.classifyLanguages.extractUASTs()

    val uastsCols = uasts.columns.length
    assert(uastsCols - 2 == filesCols)
  }

  it should "filter by reference from repos dataframe" in {
    val spark = ss

    val df = Engine(spark, resourcePath)
      .getRepositories
      .getReference("refs/heads/develop")

    df.show
    assert(df.count == 2)
  }

  "Filter by HEAD reference" should "return only HEAD references" in {
    val spark = ss
    val df = Engine(spark, resourcePath).getRepositories.getHEAD
    df.show
    assert(df.count == 5)
  }

  "Filter by master reference" should "return only master references" in {
    val spark = ss
    val df = engine.getRepositories.getMaster

    df.explain(true)
    assert(df.count == 5)
  }

  "Get develop commits" should "return only develop commits" in {
    val spark = ss
    val df = engine.getRepositories
      .getReference("refs/heads/develop").getCommits
      .select("hash", "repository_id")

    df.show(200, truncate = false)
    assert(df.count == 103)
  }

  "Get files after reading commits" should "return the correct files" in {
    val files = engine.getRepositories
      .getReferences
      .getCommits
      .getBlobs
      .drop("repository_id", "reference_name")
      .distinct()

    assert(files.count == 91944)
  }

  "Get files without reading tree entries" should "return the correct files" in {
    val files = engine.getRepositories
      .getReferences
      .getCommits
      .getBlobs
      .drop("repository_id", "reference_name")
      .distinct()

    assert(files.count == 91944)
  }

  "Get files" should "return the correct files" in {
    val df = engine.getRepositories.getHEAD.getCommits
      .sort("hash").limit(10)
    val rows = df.collect()
      .map(row => (row.getString(row.fieldIndex("repository_id")),
        row.getString(row.fieldIndex("hash"))))
    val repositories = rows.map(_._1)
    val hashes = rows.map(_._2)

    df.show(truncate = false)

    val files = engine
      .getBlobs(repositories.distinct, List("refs/heads/HEAD"), hashes.distinct)
      .drop("repository_id", "reference_name")
      .distinct()

    files.explain(true)

    assert(files.count == 655)
  }

  it should "return the correct files if we filter by repository" in {
    val files = engine
      .getBlobs(repositoryIds = List("github.com/xiyou-linuxer/faq-xiyoulinux"))
      .drop("repository_id", "reference_name")
      .distinct()

    assert(files.count == 2421)
  }

  it should "return the correct files if we filter by reference" in {
    val files = engine
      .getBlobs(referenceNames = List("refs/heads/develop"))
      .drop("repository_id", "reference_name")
      .distinct()

    assert(files.count == 425)
  }

  it should "return the correct files if we filter by commit" in {
    val files = engine
      .getBlobs(commitHashes = List("fff7062de8474d10a67d417ccea87ba6f58ca81d"))
      .drop("repository_id", "reference_name")
      .distinct()
    files.explain(true)

    assert(files.count == 2)
  }

  override protected def afterAll(): Unit = {
    super.afterAll()

    engine = _: Engine
  }
}
