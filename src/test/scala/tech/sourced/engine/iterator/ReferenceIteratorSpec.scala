package tech.sourced.engine.iterator

import org.scalatest.FlatSpec
import tech.sourced.engine.util.{Attr, EqualFilter}

class ReferenceIteratorSpec extends FlatSpec with BaseRootedRepoIterator {

  "ReferenceIterator" should "return all references from all repositories into a siva file" in {
    testIterator(
      new ReferenceIterator(Array("repository_id", "name", "hash"), _, null, Seq()), {
        case (0, row) =>
          row.getString(0) should be("github.com/xiyou-linuxer/faq-xiyoulinux")
          row.getString(1) should be("refs/heads/HEAD")
          row.getString(2) should be("fff7062de8474d10a67d417ccea87ba6f58ca81d")
        case (1, row) =>
          row.getString(0) should be("github.com/mawag/faq-xiyoulinux")
          row.getString(1) should be("refs/heads/HEAD")
          row.getString(2) should be("fff7062de8474d10a67d417ccea87ba6f58ca81d")
        case (2, row) =>
          row.getString(0) should be("github.com/xiyou-linuxer/faq-xiyoulinux")
          row.getString(1) should be("refs/heads/develop")
          row.getString(2) should be("880653c14945dbbc915f1145561ed3df3ebaf168")
        case _ =>
      }, total = 43, columnsCount = 3
    )
  }

  it should "return only specified columns" in {
    testIterator(
      new ReferenceIterator(Array("repository_id", "name"), _, null, Seq()), {
        case (0, row) =>
          row.getString(0) should be("github.com/xiyou-linuxer/faq-xiyoulinux")
          row.getString(1) should be("refs/heads/HEAD")
        case (1, row) =>
          row.getString(0) should be("github.com/mawag/faq-xiyoulinux")
          row.getString(1) should be("refs/heads/HEAD")
        case (2, row) =>
          row.getString(0) should be("github.com/xiyou-linuxer/faq-xiyoulinux")
          row.getString(1) should be("refs/heads/develop")
        case _ =>
      }, total = 43, columnsCount = 2
    )
  }

  it should "apply passed filters" in {
    testIterator(
      new ReferenceIterator(
        Array("repository_id", "name"),
        _,
        null,
        Seq(EqualFilter(Attr("name", "references"), "refs/heads/develop"))
      ), {
        case (0, row) =>
          row.getString(0) should be("github.com/xiyou-linuxer/faq-xiyoulinux")
          row.getString(1) should be("refs/heads/develop")
        case (1, row) =>
          row.getString(0) should be("github.com/mawag/faq-xiyoulinux")
          row.getString(1) should be("refs/heads/develop")
      }, total = 2, columnsCount = 2
    )
  }

  it should "use previously passed iterator" in {
    testIterator(repo =>
      new ReferenceIterator(
        Array("repository_id", "name"),
        repo,
        new RepositoryIterator(
          Array("id"),
          repo,
          Seq(EqualFilter(Attr("id", "repository"), "github.com/xiyou-linuxer/faq-xiyoulinux"))
        ),
        Seq(EqualFilter(Attr("name", "references"), "refs/heads/develop"))
      ), {
      case (0, row) =>
        row.getString(0) should be("github.com/xiyou-linuxer/faq-xiyoulinux")
        row.getString(1) should be("refs/heads/develop")
    }, total = 1, columnsCount = 2
    )
  }
}
