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
    val refsDf = reposDf.getReferences.filter($"name".equalTo("refs/heads/HEAD"))

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
      .getReferences.filter($"name".equalTo("refs/heads/HEAD"))
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

}
