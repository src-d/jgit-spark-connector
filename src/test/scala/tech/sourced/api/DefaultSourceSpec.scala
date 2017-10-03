package tech.sourced.api

import org.scalatest._

class DefaultSourceSpec extends FlatSpec with Matchers with BaseSivaSpec with BaseSparkSpec {

  "Default source" should "load correctly" in {
    val reposDf = ss.read.format("tech.sourced.api")
      .option("table", "repositories")
      .load(resourcePath)

    reposDf.filter("is_fork=true or is_fork is null").show()

    reposDf.filter("array_contains(urls, 'urlA')").show()

    val referencesDf = ss.read.format("tech.sourced.api")
      .option("table", "references")
      .load(resourcePath)

    referencesDf.filter("repository_id = 'ID1'").show()

    val commitsDf = ss.read.format("tech.sourced.api")
      .option("table", "commits")
      .load(resourcePath)

    commitsDf.show()

    info("Files/blobs (without commit hash filtered) at HEAD or every ref:\n")
    val filesDf = ss.read.format("tech.sourced.api")
      .option("table", "files")
      .load(resourcePath)

    filesDf.explain(true)
    filesDf.show()

    assert(filesDf.count() != 0)
  }

  "Additional methods" should "work correctly" in {
    val spark = ss

    import spark.implicits._

    val reposDf = SparkAPI(spark, resourcePath).getRepositories
      .filter($"id" === "github.com/mawag/faq-xiyoulinux" || $"id" === "github.com/xiyou-linuxer/faq-xiyoulinux")
    val refsDf = reposDf.getReferences.getHEAD

    val commitsDf = refsDf.getCommits.select("repository_id", "reference_name", "message", "hash")
    commitsDf.show()

    info("Files/blobs with commit hashes:\n")
    val filesDf = refsDf.getCommits.getFiles.select("repository_id", "reference_name", "path", "commit_hash", "file_hash")
    filesDf.explain(true)
    filesDf.show()

    val cnt = filesDf.count()
    info(s"Total $cnt rows")
    assert(cnt != 0)
  }

  "Convenience for getting files" should "work without reading commits" in {
    val spark = ss
    import spark.implicits._

    val filesDf = SparkAPI(spark, resourcePath)
      .getRepositories.filter($"id" === "github.com/mawag/faq-xiyoulinux")
      .getReferences.getHEAD
      .getFiles
      .select("repository_id", "name", "path", "commit_hash", "file_hash", "content", "is_binary")

    val cnt = filesDf.count()
    info(s"Total $cnt rows")
    assert(cnt != 0)

    filesDf.show()

    info("UAST for files:\n")
    val filesCols = filesDf.columns.length
    val uasts = filesDf.classifyLanguages.extractUASTs
    uasts.show()
    val uastsCols = uasts.columns.length
    assert(uastsCols - 2 == filesCols)
  }

  "Filter by reference from repos dataframe" should "work" in {
    val spark = ss

    val count = SparkAPI(spark, resourcePath)
      .getRepositories
      .getReference("refs/heads/develop")
      .count()

    assert(count == 2)
  }

  "Filter by reference from commits dataframe" should "work" in {
    val spark = ss

    val count = SparkAPI(spark, resourcePath)
      .getRepositories
      .getReferences
      .getCommits
      .getReference("refs/heads/develop")
      .count()

    assert(count == 103)
  }

  "Filter by reference from references dataframe" should "work" in {
    val spark = ss

    val count = SparkAPI(spark, resourcePath)
      .getRepositories
      .getReferences
      .getReference("refs/heads/develop")
      .count()

    assert(count == 2)
  }

  "Filter by HEAD reference" should "return only HEAD references" in {
    val spark = ss
    val count = SparkAPI(spark, resourcePath).getRepositories.getHEAD
      .select("name").distinct().count()

    assert(count == 1)
  }

  "Filter by master reference" should "return only master references" in {
    val spark = ss
    val count = SparkAPI(spark, resourcePath).getRepositories.getMaster
      .select("name").distinct().count()

    assert(count == 1)
  }

  "Get files after reading commits" should "return the correct files" in {
    val spark = ss
    val files = SparkAPI(spark, resourcePath).getRepositories.getReferences.getCommits.getFiles

    assert(files.count == 1536360)
  }

  "Get files without reading commits" should "return the correct files" in {
    val spark = ss
    val api = SparkAPI(spark, resourcePath)
    val files = api.getRepositories.getReferences.getFiles

    assert(files.count == 19126)
  }

  "Get files" should "return the correct files" in {
    val spark = ss
    val api = SparkAPI(spark, resourcePath)
    val df = api.getRepositories.getHEAD.getCommits
      .sort("hash").limit(10)
    val rows = df.collect()
      .map(row => (row.getString(row.fieldIndex("repository_id")), row.getString(row.fieldIndex("hash"))))
    val repositories = rows.map(_._1)
    val hashes = rows.map(_._2)

    val files = SparkAPI(spark, resourcePath)
      .getFiles(repositories.distinct, List("refs/heads/HEAD"), hashes.distinct)

    assert(files.count == 745)
  }

  "Get files by repository" should "return the correct files" in {
    val spark = ss
    val files = SparkAPI(spark, resourcePath)
      .getFiles(repositoryIds = List("github.com/xiyou-linuxer/faq-xiyoulinux"))

    assert(files.count == 20048)
  }

  "Get files by reference" should "return the correct files" in {
    val spark = ss
    val files = SparkAPI(spark, resourcePath)
      .getFiles(referenceNames = List("refs/heads/develop"))

    assert(files.count == 404)
  }

  "Get files by commit" should "return the correct files" in {
    val spark = ss
    val files = SparkAPI(spark, resourcePath)
      .getFiles(commitHashes = List("fff7062de8474d10a67d417ccea87ba6f58ca81d"))

    assert(files.count == 86)
  }

}
