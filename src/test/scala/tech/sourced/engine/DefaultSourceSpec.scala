package tech.sourced.engine

import org.scalatest._

class DefaultSourceSpec extends FlatSpec with Matchers with BaseSivaSpec with BaseSparkSpec {

  var engine: Engine = _

  override protected def beforeAll(): Unit = {
    super.beforeAll()

    engine = Engine(ss, resourcePath, "siva")
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

    df.count should be(457)
  }

  it should "count all the commit messages from all masters that are not forks" in {
    val commitsDf = engine.getRepositories.filter("is_fork = false").getMaster.getCommits
    val df = commitsDf.select("message").filter(commitsDf("message").startsWith("a"))
    df.count should be(7)
  }

  it should "count all commits messages from all references that are not forks" in {
    val commitsDf = engine.getRepositories.filter("is_fork = false").getReferences.getCommits
    val df = commitsDf.select("message", "reference_name", "hash").
      filter(commitsDf("message").startsWith("a"))

    df.count should be(98)
  }

  it should "get all files from HEADS that are Ruby" in {
    val blobsDf = engine.getRepositories.filter("is_fork = false")
      .getHEAD
      .getCommits
      .getFirstReferenceCommit
      .getTreeEntries
      .getBlobs
      .classifyLanguages

    val df = blobsDf.filter(blobsDf("lang") === "Ruby").select("lang", "path")
    df.count should be(169)
  }

  it should "not optimize if the conditions on the " +
    "join are not the expected ones" in {
    val reposDf = engine.getRepositories
    val referencesDf = ss.read.format("tech.sourced.engine").option("table", "references").load()
    val joinDf = reposDf.join(referencesDf,
      (referencesDf("repository_id") === reposDf("id"))
        .and(referencesDf("name").startsWith("refs/pull"))
    )

    joinDf.count should be(37)
  }

  it should "get all tree entries" in {
    val df = engine.getRepositories.getReferences.getCommits.getTreeEntries
    df.count should be(304362)
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

    blobsDf.count should be(2)

    val filesCols = blobsDf.columns.length
    val uastsDf = blobsDf.classifyLanguages.extractUASTs()

    val uastsCols = uastsDf.columns.length
    uastsCols should be(filesCols + 2)
  }

  it should "filter by reference from repos dataframe" in {
    val spark = ss

    val df = engine
      .getRepositories
      .getReference("refs/heads/develop")

    df.count should be(2)
  }

  "Filter by HEAD reference" should "return only HEAD references" in {
    val spark = ss
    val df = engine.getRepositories.getHEAD
    df.count should be(5)
  }

  "Filter by master reference" should "return only master references" in {
    val spark = ss
    val df = engine.getRepositories.getMaster
    df.count should be(5)
  }

  "Get develop commits" should "return only develop commits" in {
    val spark = ss
    val df = engine.getRepositories
      .getReference("refs/heads/develop").getCommits
      .select("hash", "repository_id")

    df.count should be(103)
  }

  "Get blobs after reading commits without reading tree entries" should
    "return the correct blobs" in {
    val blobsDf = engine.getRepositories
      .getReferences
      .getCommits
      .getBlobs
      .drop("repository_id", "reference_name")
      .distinct()

    blobsDf.count should be(91944)
  }

  "Get blobs" should "return the correct blobs" in {
    val df = engine.getRepositories.getHEAD.getCommits
      .sort("hash").limit(10)

    val rows = df.collect()
      .map(row => (row.getString(row.fieldIndex("repository_id")),
        row.getString(row.fieldIndex("hash"))))

    val repositories = rows.map(_._1)
    val hashes = rows.map(_._2)

    val blobsDf = engine
      .getBlobs(repositories.distinct, List("refs/heads/HEAD"), hashes.distinct)
      .drop("repository_id", "reference_name")
      .distinct()

    blobsDf.count should be(655)
  }

  it should "return the correct blobs if we filter by repository" in {
    val blobsDf = engine
      .getBlobs(repositoryIds = List("github.com/xiyou-linuxer/faq-xiyoulinux"))
      .drop("repository_id", "reference_name")
      .distinct()

    blobsDf.count should be(2421)
  }

  it should "return the correct blobs if we filter by reference" in {
    val blobsDf = engine
      .getBlobs(referenceNames = List("refs/heads/develop"))
      .drop("repository_id", "reference_name")
      .distinct()

    blobsDf.count should be(425)
  }

  it should "return the correct blobs if we filter by commit" in {
    val blobsDf = engine
      .getBlobs(commitHashes = List("fff7062de8474d10a67d417ccea87ba6f58ca81d"))
      .drop("repository_id", "reference_name")
      .distinct()

    blobsDf.count should be(2)
  }

  override protected def afterAll(): Unit = {
    super.afterAll()

    engine = _: Engine
  }
}
