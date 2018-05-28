package tech.sourced.engine.iterator

import java.nio.charset.StandardCharsets

import org.scalatest.FlatSpec
import tech.sourced.engine.util._

class BlobIteratorSpec extends FlatSpec with BaseChainableIterator {

  val columns = Array(
    "blob_id",
    "commit_hash",
    "repository_id",
    "reference_name",
    "content",
    "is_binary"
  )

  private val allCommitsFilter = NotFilter(EqualFilter(Attr("index", "commits"), -1))

  "BlobIterator" should "return all blobs for files at every commit of all refs in repository" in {
    testIterator(repo =>
      new BlobIterator(
        columns, repo, null, Seq(allCommitsFilter), false), {
      case (0, row) =>
        row.getString(0) should be("733c072369ca77331f392c40da7404c85c36542c")
        row.getString(1) should be("fff7062de8474d10a67d417ccea87ba6f58ca81d")
        row.getString(2) should be("github.com/xiyou-linuxer/faq-xiyoulinux")
        row.getString(3) should be("refs/heads/HEAD")
        new String(row.getAs[Array[Byte]](4), StandardCharsets.UTF_8)
          .should(startWith("                    GNU GENERAL PUBLIC LICENSE"))
        row.getBoolean(5) should be(false)
      case (1, row) =>
        row.getString(0) should be("2d2ad68c14c51e62595125b86b464427f6bf2126")
        row.getString(1) should be("fff7062de8474d10a67d417ccea87ba6f58ca81d")
        row.getString(2) should be("github.com/xiyou-linuxer/faq-xiyoulinux")
        row.getString(3) should be("refs/heads/HEAD")
        new String(row.getAs[Array[Byte]](4), StandardCharsets.UTF_8)
          .should(startWith("# faq-xiyoulinux"))
        row.getBoolean(5) should be(false)
      case (2, row) =>
        row.getString(0) should be("733c072369ca77331f392c40da7404c85c36542c")
        row.getString(1) should be("fff7062de8474d10a67d417ccea87ba6f58ca81d")
        row.getString(2) should be("github.com/mawag/faq-xiyoulinux")
        row.getString(3) should be("refs/heads/HEAD")
        new String(row.getAs[Array[Byte]](4), StandardCharsets.UTF_8)
          .should(startWith("                    GNU GENERAL PUBLIC LICENSE"))
        row.getBoolean(5) should be(false)
      case _ =>
    }, total = 22187, columnsCount = columns.length
      // NOTE: it differs from the number of tree entries in GitTreeEntryIteratorSpec because
      // Blob Objects can be missing
    )
  }

  it should "filter refs and return only blobs in HEAD of the given ref" in {
    val filters = Seq(
      EqualFilter(
        Attr("reference_name", "commits"),
        "refs/heads/HEAD"
      ),
      allCommitsFilter
    )

    testIterator(repo =>
      new BlobIterator(
        columns, repo, null, filters, false), {
      case (0, row) =>
        row.getString(0) should be("733c072369ca77331f392c40da7404c85c36542c")
        row.getString(1) should be("fff7062de8474d10a67d417ccea87ba6f58ca81d")
        row.getString(2) should be("github.com/xiyou-linuxer/faq-xiyoulinux")
        row.getString(3) should be("refs/heads/HEAD")
        new String(row.getAs[Array[Byte]](4), StandardCharsets.UTF_8)
          .should(startWith("                    GNU GENERAL PUBLIC LICENSE"))
        row.getBoolean(5) should be(false)
      case (1, row) =>
        row.getString(0) should be("2d2ad68c14c51e62595125b86b464427f6bf2126")
        row.getString(1) should be("fff7062de8474d10a67d417ccea87ba6f58ca81d")
        row.getString(2) should be("github.com/xiyou-linuxer/faq-xiyoulinux")
        row.getString(3) should be("refs/heads/HEAD")
        new String(row.getAs[Array[Byte]](4), StandardCharsets.UTF_8)
          .should(startWith("# faq-xiyoulinux"))
        row.getBoolean(5) should be(false)
      case (2, row) =>
        row.getString(0) should be("733c072369ca77331f392c40da7404c85c36542c")
        row.getString(1) should be("fff7062de8474d10a67d417ccea87ba6f58ca81d")
        row.getString(2) should be("github.com/mawag/faq-xiyoulinux")
        row.getString(3) should be("refs/heads/HEAD")
        new String(row.getAs[Array[Byte]](4), StandardCharsets.UTF_8)
          .should(startWith("                    GNU GENERAL PUBLIC LICENSE"))
        row.getBoolean(5) should be(false)
      case (_, row) =>
        row.getString(3) should be("refs/heads/HEAD")
    }, total = 4, columnsCount = columns.length
    )

  }

  it should "filter repositories if given" in {
    val filters = Seq(
      EqualFilter(
        Attr("repository_id", "references"),
        "github.com/mawag/faq-xiyoulinux"
      ),
      allCommitsFilter
    )

    testIterator(repo =>
      new BlobIterator(
        columns, repo, null, filters, false), {
      case (0, row) =>
        row.getString(0) should be("733c072369ca77331f392c40da7404c85c36542c")
        row.getString(1) should be("fff7062de8474d10a67d417ccea87ba6f58ca81d")
        row.getString(2) should be("github.com/mawag/faq-xiyoulinux")
        row.getString(3) should be("refs/heads/HEAD")
        new String(row.getAs[Array[Byte]](4), StandardCharsets.UTF_8)
          .should(startWith("                    GNU GENERAL PUBLIC LICENSE"))
        row.getBoolean(5) should be(false)
      case (1, row) =>
        row.getString(0) should be("2d2ad68c14c51e62595125b86b464427f6bf2126")
        row.getString(1) should be("fff7062de8474d10a67d417ccea87ba6f58ca81d")
        row.getString(2) should be("github.com/mawag/faq-xiyoulinux")
        row.getString(3) should be("refs/heads/HEAD")
        new String(row.getAs[Array[Byte]](4), StandardCharsets.UTF_8)
          .should(startWith("# faq-xiyoulinux"))
        row.getBoolean(5) should be(false)
      case (_, row) =>
        row.getString(2) should be("github.com/mawag/faq-xiyoulinux")
    }, total = 2139, columnsCount = columns.length
    )
  }

  it should "filter commits and return only blobs of the given commit" in {
    val filters = Seq(EqualFilter(
      Attr("commit_hash", "commits"), "fff7062de8474d10a67d417ccea87ba6f58ca81d"),
      allCommitsFilter
    )

    testIterator(repo =>
      new BlobIterator(
        columns, repo, null, filters, false), {
      case (_, row) =>
        row.getString(1) should be("fff7062de8474d10a67d417ccea87ba6f58ca81d")
    }, total = 86, columnsCount = columns.length
    )
  }

  it should "return only blobs for given hashes if they are given" in {
    val commits = Array(
      "a574356dab47de78259713af2f62955408395974",
      "fff7062de8474d10a67d417ccea87ba6f58ca81d"
    )
    val filters = Array[CompiledFilter](
      InFilter(Attr("hash", "commits"), commits),
      allCommitsFilter
    )

    testIterator(repo =>
      new BlobIterator(
        columns,
        repo,
        new GitTreeEntryIterator(
          Array("blob"),
          repo,
          new CommitIterator(
            Array("commit_hash"),
            repo,
            null,
            filters,
            false
          ),
          Seq(),
          false
        ),
        Seq(),
        false
      ), {
      case (_, row) =>
        commits should contain(row.getString(1))
      case _ =>

    }, total = 476, columnsCount = columns.length
    )
  }

  it should "return only blobs for given hashes and repos if they are given" in {
    val commits = Array(
      "fff7062de8474d10a67d417ccea87ba6f58ca81d",
      "f9e36cc24da9d36bab1222ae7e81a783dab83dd0"
    )
    testIterator(repo =>
      new BlobIterator(
        columns,
        repo,
        new GitTreeEntryIterator(
          Array("blob"),
          repo,
          new CommitIterator(
            Array("hash"),
            repo,
            new ReferenceIterator(
              Array("name"),
              repo,
              new RepositoryIterator(
                "/foo/bar",
                Array("id"),
                repo,
                Seq(EqualFilter(
                  Attr("id", "repositories"),
                  "github.com/xiyou-linuxer/faq-xiyoulinux"
                )),
                false
              ),
              Seq(),
              false
            ),
            Seq(InFilter(Attr("hash", "commits"), commits), allCommitsFilter),
            false
          ),
          Seq(),
          false
        ),
        Seq(),
        false
      ), {
      case (_, row) =>
        commits should contain(row.getString(1))
        row.getString(2) should be("github.com/xiyou-linuxer/faq-xiyoulinux")
      case _ =>
    }, total = 72, columnsCount = columns.length
    )
  }

  it should "not fail with other, un-supported filters" in {
    val filters = Array[CompiledFilter](
      EqualFilter(Attr("message", "commits"), "README"),
      allCommitsFilter
    )

    testIterator(repo =>
      new BlobIterator(columns, repo, null, filters, false)
      , (_, _) => (),
      total = 22187,
      columnsCount = columns.length
    )
  }
}
