package tech.sourced.engine.iterator

import java.nio.file.Paths
import java.util.UUID

import org.apache.commons.io.FileUtils
import org.scalatest.{BeforeAndAfterEach, FlatSpec}
import tech.sourced.engine.provider.{RepositoryProvider, RepositoryRDDProvider}
import tech.sourced.engine.util.{Attr, EqualFilter}

class RepositoryIteratorSpec extends FlatSpec with BaseRootedRepoIterator with BeforeAndAfterEach {

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

  "RepositoryIterator" should "return data for all repositories into a siva file" in {
    testIterator(
      new RepositoryIterator(Array("id", "urls", "is_fork"), _, Seq()), {
        case (0, row) =>
          row.getString(0) should be("github.com/xiyou-linuxer/faq-xiyoulinux")
          row.getAs[Array[String]](1).length should be(3)
          row.getBoolean(2) should be(false)
        case (1, row) =>
          row.getString(0) should be("github.com/mawag/faq-xiyoulinux")
          row.getAs[Array[String]](1).length should be(3)
          row.getBoolean(2) should be(true)
        case (c, _) => fail(s"unexpected row number: $c")
      }, total = 2, columnsCount = 3
    )
  }

  it should "return only specified columns" in {
    testIterator(
      new RepositoryIterator(Array("id", "is_fork"), _, Seq()), {
        case (0, row) =>
          row.getString(0) should be("github.com/xiyou-linuxer/faq-xiyoulinux")
          row.getBoolean(1) should be(false)
        case (1, row) =>
          row.getString(0) should be("github.com/mawag/faq-xiyoulinux")
          row.getBoolean(1) should be(true)
        case (c, _) => fail(s"unexpected row number: $c")
      }, total = 2, columnsCount = 2
    )
  }

  it should "apply passed filters" in {
    testIterator(
      new RepositoryIterator(
        Array("id", "is_fork"),
        _,
        Seq(EqualFilter(Attr("id", "repository"), "github.com/mawag/faq-xiyoulinux"))
      ), {
        case (0, row) =>
          row.getString(0) should be("github.com/mawag/faq-xiyoulinux")
          row.getBoolean(1) should be(true)
        case (c, _) => fail(s"unexpected row number: $c")
      }, total = 1, columnsCount = 2
    )
  }

  it should "return a repository for each distinct remote and the local dir" in {
    import tech.sourced.engine.util.RepoUtils._

    val gitRepo = createRepo(tmpDir.resolve("repo"))

    addRemote(gitRepo, "repo", "git@github.com:git/repo.git")

    FileUtils.write(tmpDir.resolve("repo").resolve("README.md").toFile, "hello world")
    gitRepo.add().addFilepattern("README.md").call()
    gitRepo.commit().setMessage("first commit on regular repo").call()

    val rdd = RepositoryRDDProvider(ss.sparkContext).get(tmpDir.toString)
    val source = rdd.first()
    val repo = RepositoryProvider(tmpDir.toString).get(source)

    val iter = new RepositoryIterator(Array("id"), repo, Seq())
    val repos = iter.toList

    repos.length should be(2)
    repos.head(0).toString should be("github.com/git/repo")
    repos(1)(0).toString should startWith("file://")
  }

}
