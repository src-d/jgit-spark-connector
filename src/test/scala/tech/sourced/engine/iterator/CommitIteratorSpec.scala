package tech.sourced.engine.iterator

import java.sql.Timestamp

import org.scalatest.FlatSpec
import tech.sourced.engine.util.{Attr, EqualFilter, InFilter}

class CommitIteratorSpec extends FlatSpec with BaseRootedRepoIterator {

  private val cols = Array(
    "repository_id",
    "reference_name",
    "index",
    "hash",
    "message",
    "parents",
    "parents_count",
    "author_email",
    "author_name",
    "author_date",
    "committer_email",
    "committer_name",
    "committer_date"
  )

  "CommitIterator" should "return all commits from all repositories into a siva file" in {
    testIterator(
      new CommitIterator(
        cols, _, null, Seq()), {
        case (0, row) =>
          row.getString(0) should be("github.com/xiyou-linuxer/faq-xiyoulinux")
          row.getString(1) should be("refs/heads/HEAD")
          row.getInt(2) should be(0)
          row.getString(3) should be("fff7062de8474d10a67d417ccea87ba6f58ca81d")
          row.getString(4) should be("Initial commit\n")
          row.getAs[Array[String]](5) should be(Array())
          row.getInt(6) should be(0)
          row.getString(7) should be("wangbo2008@vip.qq.com")
          row.getString(8) should be("wangbo")
          row.getTimestamp(9) should be(new Timestamp(1438072751000L))
          row.getString(10) should be("wangbo2008@vip.qq.com")
          row.getString(11) should be("wangbo")
          row.getTimestamp(12) should be(new Timestamp(1438072751000L))
        case (1, row) =>
          row.getString(0) should be("github.com/mawag/faq-xiyoulinux")
          row.getString(1) should be("refs/heads/HEAD")
          row.getInt(2) should be(0)
          row.getString(3) should be("fff7062de8474d10a67d417ccea87ba6f58ca81d")
          row.getString(4) should be("Initial commit\n")
          row.getAs[Array[String]](5) should be(Array())
          row.getInt(6) should be(0)
          row.getString(7) should be("wangbo2008@vip.qq.com")
          row.getString(8) should be("wangbo")
          row.getTimestamp(9) should be(new Timestamp(1438072751000L))
          row.getString(10) should be("wangbo2008@vip.qq.com")
          row.getString(11) should be("wangbo")
          row.getTimestamp(12) should be(new Timestamp(1438072751000L))
        case (23, row) =>
          row.getString(0) should be("github.com/xiyou-linuxer/faq-xiyoulinux")
          row.getString(1) should be("refs/heads/develop")
          row.getInt(2) should be(21)
          row.getString(3) should be("25fda7c4d616c8a7c017384d7745342c73eb214d")
          row.getString(4) should be("Merge pull request #5 from xiyou-linuxer/develop\n\nDevelop")
          row.getAs[Array[String]](5) should be(
            Array("531f574cf8c457cbeb4f6a5bae2d81db22c5dc1a",
              "8331656181f9040d805b729946b12fd4382c4665"))
          row.getInt(6) should be(2)
          row.getString(7) should be("1679211339@qq.com")
          row.getString(8) should be("磊磊")
          row.getTimestamp(9) should be(new Timestamp(1439552359000L))
          row.getString(10) should be("1679211339@qq.com")
          row.getString(11) should be("磊磊")
          row.getTimestamp(12) should be(new Timestamp(1439552359000L))
        case (i, _) if i > 1061 => fail("commits not expected")

        case _ =>
      }, total = 1062, columnsCount = 13
    )
  }

  it should "return all commits from all repositories into a siva file, " +
    "filtering columns" in {
    testIterator(
      new CommitIterator(
        Array(
          "repository_id",
          "parents"
        ), _, null, Seq()), {
        case (0, row) =>
          row.getString(0) should be("github.com/xiyou-linuxer/faq-xiyoulinux")
          row.getAs[Array[String]](1) should be(Array())
        case (1, row) =>
          row.getString(0) should be("github.com/mawag/faq-xiyoulinux")
          row.getAs[Array[String]](1) should be(Array())
        case (23, row) =>
          row.getString(0) should be("github.com/xiyou-linuxer/faq-xiyoulinux")
          row.getAs[Array[String]](1) should be(
            Array("531f574cf8c457cbeb4f6a5bae2d81db22c5dc1a",
              "8331656181f9040d805b729946b12fd4382c4665"))
        case (i, _) if i > 1061 => fail("commits not expected")
        case _ =>
      }, total = 1062, columnsCount = 2
    )
  }

  it should "apply passed filters" in {
    val commits = Array(
      "fff7062de8474d10a67d417ccea87ba6f58ca81d",
      "531f574cf8c457cbeb4f6a5bae2d81db22c5dc1a"
    )

    testIterator(
      new CommitIterator(
        Array(
          "repository_id",
          "reference_name",
          "hash"
        ),
        _,
        null,
        Seq(InFilter(Attr("hash", "commits"), commits))
      ), {
        case (0, row) =>
          row.getString(0) should be("github.com/xiyou-linuxer/faq-xiyoulinux")
          row.getString(1) should be("refs/heads/HEAD")
          assert(commits.contains(row.getString(2)))
        case (1, row) =>
          row.getString(0) should be("github.com/mawag/faq-xiyoulinux")
          row.getString(1) should be("refs/heads/HEAD")
          assert(commits.contains(row.getString(2)))
        case _ =>
      }, total = 58, columnsCount = 3
    )
  }

  it should "apply use prev iterator" in {
    val commits = Array(
      "fff7062de8474d10a67d417ccea87ba6f58ca81d",
      "531f574cf8c457cbeb4f6a5bae2d81db22c5dc1a"
    )

    testIterator(repo =>
      new CommitIterator(
        Array(
          "repository_id",
          "reference_name",
          "hash"
        ),
        repo,
        new ReferenceIterator(
          Array("name"),
          repo,
          new RepositoryIterator(
            Array("id"),
            repo,
            Seq()
          ),
          Seq(InFilter(Attr("name", "references"), Array(
            "refs/heads/master", "refs/heads/develop"
          )))
        ),
        Seq()
      ), {
      case (0, row) =>
        row.getString(0) should be("github.com/xiyou-linuxer/faq-xiyoulinux")
        row.getString(1) should be("refs/heads/develop")
        row.getString(2) should be("880653c14945dbbc915f1145561ed3df3ebaf168")
      case (1, row) =>
        row.getString(0) should be("github.com/xiyou-linuxer/faq-xiyoulinux")
        row.getString(1) should be("refs/heads/develop")
        row.getString(2) should be("c83aaf5eb40dfe6639d3e400fc07c7360b818404")
      case _ =>
    }, total = 105, columnsCount = 3
    )
  }

}
