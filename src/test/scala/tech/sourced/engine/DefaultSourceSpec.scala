package tech.sourced.engine

import org.scalatest._

class DefaultSourceSpec extends FlatSpec with Matchers with BaseSivaSpec with BaseSparkSpec {

  var engine: Engine = _

  override protected def beforeAll(): Unit = {
    super.beforeAll()

    engine = Engine(ss, resourcePath)
  }

  "Default source" should "get heads of all repositories and count the files" in {
    val df = engine.getRepositories.getHEAD.getCommits.getFirstReferenceCommit.getFiles
    engine.getRepositories.getHEAD.getCommits.getFirstReferenceCommit.show
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
    val files = engine.getRepositories.filter("is_fork = false")
      .getHEAD
      .getCommits
      .getFirstReferenceCommit
      .getFiles
      .classifyLanguages
    val df = files.filter(files("lang") === "Ruby").select("lang", "path")
    df.show(169, truncate = false)
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
    val filesDf = references.getCommits.getFiles.select(
      "path", "commit_hash", "file_hash"
    )
    filesDf.explain(true)
    filesDf.show()

    out should be(37)
  }

  "Convenience for getting files" should "work without reading commits" in {
    val spark = ss
    import spark.implicits._

    val filesDf = engine
      .getRepositories.filter($"id" === "github.com/mawag/faq-xiyoulinux")
      .getReferences.getHEAD
      .getFiles
      .select(
        "path",
        "commit_hash",
        "file_hash",
        "content",
        "is_binary"
      )

    val cnt = filesDf.count()
    info(s"Total $cnt rows")
    cnt should be(2)

    info("UAST for files:\n")
    val filesCols = filesDf.columns.length
    val uasts = filesDf.classifyLanguages.extractUASTs()

    val uastsCols = uasts.columns.length
    assert(uastsCols - 2 == filesCols)
  }

  "Filter by reference from repos dataframe" should "work" in {
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
    val files = engine.getRepositories.getReferences.getCommits.getFiles

    files.show(truncate = false)
    assert(files.count == 91944)
  }

  "Get files without reading commits" should "return the correct files" in {
    val files = engine.getRepositories.getReferences.getFiles

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
      .getFiles(repositories.distinct, List("refs/heads/HEAD"), hashes.distinct)

    assert(files.count == 655)
  }

  it should "return the correct files if we filter by repository" in {
    val files = engine
      .getFiles(repositoryIds = List("github.com/xiyou-linuxer/faq-xiyoulinux"))

    assert(files.count == 2421)
  }

  it should "return the correct files if we filter by reference" in {
    val files = engine
      .getFiles(referenceNames = List("refs/heads/develop"))

    assert(files.count == 425)
  }

  it should "return the correct files if we filter by commit" in {
    val files = engine
      .getFiles(commitHashes = List("fff7062de8474d10a67d417ccea87ba6f58ca81d"))
    files.explain(true)

    assert(files.count == 2)
  }

  override protected def afterAll(): Unit = {
    super.afterAll()

    engine = _: Engine
  }
}
