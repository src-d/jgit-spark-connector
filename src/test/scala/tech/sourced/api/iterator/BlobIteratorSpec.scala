package tech.sourced.api.iterator

import java.nio.charset.StandardCharsets

import org.scalatest.FlatSpec
import tech.sourced.api.util.{Attr, CompiledFilter, EqualFilter, InFilter}

class BlobIteratorSpec extends FlatSpec with BaseRootedRepoIterator {

  "BlobIterator" should "return all blobs for files at every commit of all refs in repository" in {
    testIterator(
      new BlobIterator(
        Array(
          "file_hash",
          "content",
          "commit_hash",
          "is_binary",
          "path"
        ), _, null, Seq()), {
        case (0, row) =>
          row.getString(0) should be("733c072369ca77331f392c40da7404c85c36542c")
          new String(row.getAs[Array[Byte]](1), StandardCharsets.UTF_8)
            .should(startWith("                    GNU GENERAL PUBLIC LICENSE"))
          row.getString(2) should be("fff7062de8474d10a67d417ccea87ba6f58ca81d")
          row.getBoolean(3) should be(false)
          row.getString(4) should be("LICENSE")
        case (1, row) =>
          row.getString(0) should be("2d2ad68c14c51e62595125b86b464427f6bf2126")
          new String(row.getAs[Array[Byte]](1), StandardCharsets.UTF_8)
            .should(startWith("# faq-xiyoulinux"))
          row.getString(2) should be("fff7062de8474d10a67d417ccea87ba6f58ca81d")
          row.getBoolean(3) should be(false)
          row.getString(4) should be("README.md")
        case (2, row) =>
          row.getString(0) should be("733c072369ca77331f392c40da7404c85c36542c")
          new String(row.getAs[Array[Byte]](1), StandardCharsets.UTF_8)
            .should(startWith("                    GNU GENERAL PUBLIC LICENSE"))
          row.getString(2) should be("fff7062de8474d10a67d417ccea87ba6f58ca81d")
          row.getBoolean(3) should be(false)
          row.getString(4) should be("LICENSE")

        case (i, _) if i >= 22187 => fail("commits not expected")
        case _ =>
      }, total = 22187, columnsCount = 5
    )
  }


  "BlobIterator" should "filter refs and return only files in HEAD of the given ref" in {
    val refFilters = Seq(EqualFilter(
      Attr("reference_name", "commits"),
      "refs/heads/HEAD")
    )

    testIterator(
      new BlobIterator(
        Array(
          "file_hash",
          "content",
          "commit_hash",
          "is_binary",
          "path"
        ), _, null, refFilters), {
        case (0, row) =>
          row.getString(0) should be("733c072369ca77331f392c40da7404c85c36542c")
          new String(row.getAs[Array[Byte]](1), StandardCharsets.UTF_8)
            .should(startWith("                    GNU GENERAL PUBLIC LICENSE"))
          row.getString(2) should be("fff7062de8474d10a67d417ccea87ba6f58ca81d")
          row.getBoolean(3) should be(false)
          row.getString(4) should be("LICENSE")
        case (1, row) =>
          row.getString(0) should be("2d2ad68c14c51e62595125b86b464427f6bf2126")
          new String(row.getAs[Array[Byte]](1), StandardCharsets.UTF_8).
            should(startWith("# faq-xiyoulinux"))
          row.getString(2) should be("fff7062de8474d10a67d417ccea87ba6f58ca81d")
          row.getBoolean(3) should be(false)
          row.getString(4) should be("README.md")
        case (2, row) =>
          row.getString(0) should be("733c072369ca77331f392c40da7404c85c36542c")
          new String(row.getAs[Array[Byte]](1), StandardCharsets.UTF_8)
            .should(startWith("                    GNU GENERAL PUBLIC LICENSE"))
          row.getString(2) should be("fff7062de8474d10a67d417ccea87ba6f58ca81d")
          row.getBoolean(3) should be(false)
          row.getString(4) should be("LICENSE")
        case (3, row) =>
          row.getString(0) should be("2d2ad68c14c51e62595125b86b464427f6bf2126")
          new String(row.getAs[Array[Byte]](1), StandardCharsets.UTF_8)
            .should(startWith("# faq-xiyoulinux"))
          row.getString(2) should be("fff7062de8474d10a67d417ccea87ba6f58ca81d")
          row.getBoolean(3) should be(false)
          row.getString(4) should be("README.md")
        case (i, _) if i > 3 => fail("commits not expected")
        case _ =>
      }, total = 4, columnsCount = 5
    )
  }

  "BlobIterator" should "filter repositories if given" in {
    val refFilters = Seq(EqualFilter(Attr("reference_name", "commits"), "refs/heads/HEAD"))

    testIterator(
      new BlobIterator(
        Array(
          "file_hash",
          "content",
          "commit_hash",
          "is_binary",
          "path"
        ), _, null, refFilters), {
        case (0, row) =>
          row.getString(0) should be("733c072369ca77331f392c40da7404c85c36542c")
          new String(row.getAs[Array[Byte]](1), StandardCharsets.UTF_8)
            .should(startWith("                    GNU GENERAL PUBLIC LICENSE"))
          row.getString(2) should be("fff7062de8474d10a67d417ccea87ba6f58ca81d")
          row.getBoolean(3) should be(false)
          row.getString(4) should be("LICENSE")
        case (1, row) =>
          row.getString(0) should be("2d2ad68c14c51e62595125b86b464427f6bf2126")
          new String(row.getAs[Array[Byte]](1), StandardCharsets.UTF_8)
            .should(startWith("# faq-xiyoulinux"))
          row.getString(2) should be("fff7062de8474d10a67d417ccea87ba6f58ca81d")
          row.getBoolean(3) should be(false)
          row.getString(4) should be("README.md")
        case (2, row) =>
          row.getString(0) should be("733c072369ca77331f392c40da7404c85c36542c")
          new String(row.getAs[Array[Byte]](1), StandardCharsets.UTF_8)
            .should(startWith("                    GNU GENERAL PUBLIC LICENSE"))
          row.getString(2) should be("fff7062de8474d10a67d417ccea87ba6f58ca81d")
          row.getBoolean(3) should be(false)
          row.getString(4) should be("LICENSE")
        case (3, row) =>
          row.getString(0) should be("2d2ad68c14c51e62595125b86b464427f6bf2126")
          new String(row.getAs[Array[Byte]](1), StandardCharsets.UTF_8)
            .should(startWith("# faq-xiyoulinux"))
          row.getString(2) should be("fff7062de8474d10a67d417ccea87ba6f58ca81d")
          row.getBoolean(3) should be(false)
          row.getString(4) should be("README.md")

        case (i, _) if i > 3 => fail("commits not expected")
        case _ =>

      }, total = 4, columnsCount = 5
    )
  }

  "BlobIterator" should "return only files for given hashes if they are given" in {
    val filters = Array[CompiledFilter](
      InFilter(Attr("hash", "commits"), Array(
        "a574356dab47de78259713af2f62955408395974",
        "fff7062de8474d10a67d417ccea87ba6f58ca81d"
      ))
    )

    testIterator(repo =>
      new BlobIterator(
        Array(
          "file_hash",
          "content",
          "commit_hash",
          "is_binary",
          "path"
        ),
        repo,
        new CommitIterator(
          Array("hash"),
          repo,
          null,
          filters
        ),
        Seq()
      ), {
      case (0, row) =>
        row.getString(0) should be("733c072369ca77331f392c40da7404c85c36542c")
        new String(row.getAs[Array[Byte]](1), StandardCharsets.UTF_8)
          .should(startWith("                    GNU GENERAL PUBLIC LICENSE"))
        row.getString(2) should be("fff7062de8474d10a67d417ccea87ba6f58ca81d")
        row.getBoolean(3) should be(false)
        row.getString(4) should be("LICENSE")
      case (1, row) =>
        row.getString(0) should be("2d2ad68c14c51e62595125b86b464427f6bf2126")
        new String(row.getAs[Array[Byte]](1), StandardCharsets.UTF_8)
          .should(startWith("# faq-xiyoulinux"))
        row.getString(2) should be("fff7062de8474d10a67d417ccea87ba6f58ca81d")
        row.getBoolean(3) should be(false)
        row.getString(4) should be("README.md")
      case _ =>

    }, total = 12, columnsCount = 5
    )
  }

  "BlobIterator" should "return only files for given hashes and repos if they are given" in {
    testIterator(repo =>
      new BlobIterator(
        Array(
          "file_hash",
          "content",
          "commit_hash",
          "is_binary",
          "path"
        ),
        repo,
        new CommitIterator(
          Array("hash"),
          repo,
          new ReferenceIterator(
            Array("name"),
            repo,
            new RepositoryIterator(
              Array("id"),
              repo,
              Seq(EqualFilter(
                Attr("id", "repositories"),
                "github.com/xiyou-linuxer/faq-xiyoulinux"
              ))
            ),
            Seq()
          ),
          Seq(InFilter(Attr("hash", "commits"), Array(
            "fff7062de8474d10a67d417ccea87ba6f58ca81d",
            "f9e36cc24da9d36bab1222ae7e81a783dab83dd0"
          )))
        ),
        Seq()
      ), {
      case (0, row) =>
        row.getString(0) should be("733c072369ca77331f392c40da7404c85c36542c")
        new String(row.getAs[Array[Byte]](1), StandardCharsets.UTF_8)
          .should(startWith("                    GNU GENERAL PUBLIC LICENSE"))
        row.getString(2) should be("fff7062de8474d10a67d417ccea87ba6f58ca81d")
        row.getBoolean(3) should be(false)
        row.getString(4) should be("LICENSE")
      case (1, row) =>
        row.getString(0) should be("2d2ad68c14c51e62595125b86b464427f6bf2126")
        new String(row.getAs[Array[Byte]](1), StandardCharsets.UTF_8)
          .should(startWith("# faq-xiyoulinux"))
        row.getString(2) should be("fff7062de8474d10a67d417ccea87ba6f58ca81d")
        row.getBoolean(3) should be(false)
        row.getString(4) should be("README.md")
      case _ =>
    }, total = 2, columnsCount = 5
    )
  }

  "BlobIterator" should "not fail with other, un-supported filters" in {
    val filters = Array[CompiledFilter](EqualFilter(Attr("path", "files"), "README"))

    testIterator(
      new BlobIterator(
        Array(
          "file_hash",
          "content",
          "commit_hash",
          "is_binary",
          "path"
        ), _, null, filters), {
        case _ =>
      }, total = 22187, columnsCount = 5)
  }

}
