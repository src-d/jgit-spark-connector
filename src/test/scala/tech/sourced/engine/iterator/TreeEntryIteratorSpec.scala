package tech.sourced.engine.iterator

import org.scalatest.FlatSpec
import tech.sourced.engine.util.{Attr, EqualFilter}

class TreeEntryIteratorSpec extends FlatSpec with BaseRootedRepoIterator {

  private val cols = Array(
    "commit_hash",
    "repository_id",
    "reference_name",
    "path",
    "blob"
  )

  "TreeEntryIterator" should "return all tree entries from all commits " +
    "from all repositories into a siva file" in {
    testIterator(
      new TreeEntryIterator(
        cols, _, null, Seq()), {
        case (0, row) =>
          row.getString(0) should be("fff7062de8474d10a67d417ccea87ba6f58ca81d")
          row.getString(1) should be("github.com/xiyou-linuxer/faq-xiyoulinux")
          row.getString(2) should be("refs/heads/HEAD")
          row.getString(3) should be("LICENSE")
          row.getString(4) should be("733c072369ca77331f392c40da7404c85c36542c")
        case (1, row) =>
          row.getString(0) should be("fff7062de8474d10a67d417ccea87ba6f58ca81d")
          row.getString(1) should be("github.com/xiyou-linuxer/faq-xiyoulinux")
          row.getString(2) should be("refs/heads/HEAD")
          row.getString(3) should be("README.md")
          row.getString(4) should be("2d2ad68c14c51e62595125b86b464427f6bf2126")
        case (2, row) =>
          row.getString(0) should be("fff7062de8474d10a67d417ccea87ba6f58ca81d")
          row.getString(1) should be("github.com/mawag/faq-xiyoulinux")
          row.getString(2) should be("refs/heads/HEAD")
          row.getString(3) should be("LICENSE")
          row.getString(4) should be("733c072369ca77331f392c40da7404c85c36542c")
        case (3, row) =>
          row.getString(0) should be("fff7062de8474d10a67d417ccea87ba6f58ca81d")
          row.getString(1) should be("github.com/mawag/faq-xiyoulinux")
          row.getString(2) should be("refs/heads/HEAD")
          row.getString(3) should be("README.md")
          row.getString(4) should be("2d2ad68c14c51e62595125b86b464427f6bf2126")
        case _ =>
      }, total = 23189, columnsCount = cols.length
    )
  }

  it should "filter by path" in {
    val filters = Seq(EqualFilter(
      Attr("path", "tree_entries"),
      "README.md")
    )

    testIterator(
      new TreeEntryIterator(
        cols, _, null, filters), {
        case (_, r) =>
          r.getString(3) should be("README.md")
      }, total = 1062, columnsCount = cols.length
    )
  }

  it should "filter by blob" in {
    val filters = Seq(EqualFilter(
      Attr("blob", "tree_entries"),
      "733c072369ca77331f392c40da7404c85c36542c")
    )

    testIterator(
      new TreeEntryIterator(
        cols, _, null, filters), {
        case (_, r) =>
          r.getString(4) should be("733c072369ca77331f392c40da7404c85c36542c")
      }, total = 1062, columnsCount = cols.length
    )
  }

  it should "work when it's chained" in {
    val filters = Seq(EqualFilter(
      Attr("hash", "commits"),
      "fff7062de8474d10a67d417ccea87ba6f58ca81d")
    )

    testIterator(repo =>
      new TreeEntryIterator(
        cols,
        repo,
        new CommitIterator(Array("hash"), repo, null, filters),
        Seq()
      ), {
      case (i, r) if i % 2 == 0 =>
        r.getString(4) should be("733c072369ca77331f392c40da7404c85c36542c")
        r.getString(3) should be("LICENSE")
        r.getString(0) should be("fff7062de8474d10a67d417ccea87ba6f58ca81d")

      case (_, r) =>
        r.getString(4) should be("2d2ad68c14c51e62595125b86b464427f6bf2126")
        r.getString(3) should be("README.md")
        r.getString(0) should be("fff7062de8474d10a67d417ccea87ba6f58ca81d")
    }, total = 86, columnsCount = cols.length
    )
  }

  it should "filter by commit hash" in {
    val filters = Seq(EqualFilter(
      Attr("commit_hash", "tree_entries"),
      "fff7062de8474d10a67d417ccea87ba6f58ca81d")
    )

    testIterator(
      new TreeEntryIterator(
        cols, _, null, filters), {
        case (i, r) if i % 2 == 0 =>
          r.getString(4) should be("733c072369ca77331f392c40da7404c85c36542c")
          r.getString(3) should be("LICENSE")
          r.getString(0) should be("fff7062de8474d10a67d417ccea87ba6f58ca81d")

        case (_, r) =>
          r.getString(4) should be("2d2ad68c14c51e62595125b86b464427f6bf2126")
          r.getString(3) should be("README.md")
          r.getString(0) should be("fff7062de8474d10a67d417ccea87ba6f58ca81d")
      }, total = 86, columnsCount = cols.length
    )
  }

}
