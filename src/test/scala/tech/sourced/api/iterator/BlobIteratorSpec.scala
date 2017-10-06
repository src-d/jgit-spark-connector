package tech.sourced.api.iterator

import java.nio.charset.StandardCharsets

import org.scalatest.FlatSpec
<<<<<<< 0ce62ae8d9cd1aac3c00b9d922b362a742e51dc5
import tech.sourced.api.util.{CompiledFilter, EqualFilter, InFilter}

class BlobIteratorSpec extends FlatSpec with BaseRootedRepoIterator {

  "BlobIterator" should "return all blobs for files at every commit of all refs in repository" in {
    testIterator(
      new BlobIterator(
        Array(
          "file_hash",
          "content",
          "commit_hash",
          "is_binary",
          "path",
          "repository_id",
          "reference_name"
        ), _, Array[CompiledFilter]()), {
        case (0, row) =>
          row.getString(0) should be("733c072369ca77331f392c40da7404c85c36542c")
          new String(row.getAs[Array[Byte]](1), StandardCharsets.UTF_8)
            .should(startWith("                    GNU GENERAL PUBLIC LICENSE"))
          row.getString(2) should be("fff7062de8474d10a67d417ccea87ba6f58ca81d")
          row.getBoolean(3) should be(false)
          row.getString(4) should be("LICENSE")
          row.getString(5) should be("github.com/xiyou-linuxer/faq-xiyoulinux")
          row.getString(6) should be("refs/heads/HEAD")
        case (1, row) =>
          row.getString(0) should be("2d2ad68c14c51e62595125b86b464427f6bf2126")
          new String(row.getAs[Array[Byte]](1), StandardCharsets.UTF_8)
            .should(startWith("# faq-xiyoulinux"))
          row.getString(2) should be("fff7062de8474d10a67d417ccea87ba6f58ca81d")
          row.getBoolean(3) should be(false)
          row.getString(4) should be("README.md")
          row.getString(5) should be("github.com/xiyou-linuxer/faq-xiyoulinux")
          row.getString(6) should be("refs/heads/HEAD")
        case (2, row) =>
          row.getString(0) should be("733c072369ca77331f392c40da7404c85c36542c")
          new String(row.getAs[Array[Byte]](1), StandardCharsets.UTF_8)
            .should(startWith("                    GNU GENERAL PUBLIC LICENSE"))
          row.getString(2) should be("fff7062de8474d10a67d417ccea87ba6f58ca81d")
          row.getBoolean(3) should be(false)
          row.getString(4) should be("LICENSE")
          row.getString(5) should be("github.com/mawag/faq-xiyoulinux")
          row.getString(6) should be("refs/heads/HEAD")

        case (i, _) if i >= 22187 => fail("commits not expected")
        case _ =>
      }, total = 22187, columnsCount = 7
    )
  }


  "BlobIterator" should "filter refs and return only files in HEAD of the given ref" in {
    val refFilters = Array[CompiledFilter](new EqualFilter("reference_name", "refs/heads/HEAD"))

    testIterator(
      new BlobIterator(
        Array(
          "file_hash",
          "content",
          "commit_hash",
          "is_binary",
          "path",
          "repository_id",
          "reference_name"
        ), _, refFilters), {
        case (0, row) =>
          row.getString(0) should be("733c072369ca77331f392c40da7404c85c36542c")
          new String(row.getAs[Array[Byte]](1), StandardCharsets.UTF_8)
            .should(startWith("                    GNU GENERAL PUBLIC LICENSE"))
          row.getString(2) should be("fff7062de8474d10a67d417ccea87ba6f58ca81d")
          row.getBoolean(3) should be(false)
          row.getString(4) should be("LICENSE")
          row.getString(5) should be("github.com/xiyou-linuxer/faq-xiyoulinux")
          row.getString(6) should be("refs/heads/HEAD")
        case (1, row) =>
          row.getString(0) should be("2d2ad68c14c51e62595125b86b464427f6bf2126")
          new String(row.getAs[Array[Byte]](1), StandardCharsets.UTF_8).
            should(startWith("# faq-xiyoulinux"))
          row.getString(2) should be("fff7062de8474d10a67d417ccea87ba6f58ca81d")
          row.getBoolean(3) should be(false)
          row.getString(4) should be("README.md")
          row.getString(5) should be("github.com/xiyou-linuxer/faq-xiyoulinux")
          row.getString(6) should be("refs/heads/HEAD")
        case (2, row) =>
          row.getString(0) should be("733c072369ca77331f392c40da7404c85c36542c")
          new String(row.getAs[Array[Byte]](1), StandardCharsets.UTF_8)
            .should(startWith("                    GNU GENERAL PUBLIC LICENSE"))
          row.getString(2) should be("fff7062de8474d10a67d417ccea87ba6f58ca81d")
          row.getBoolean(3) should be(false)
          row.getString(4) should be("LICENSE")
          row.getString(5) should be("github.com/mawag/faq-xiyoulinux")
          row.getString(6) should be("refs/heads/HEAD")
        case (3, row) =>
          row.getString(0) should be("2d2ad68c14c51e62595125b86b464427f6bf2126")
          new String(row.getAs[Array[Byte]](1), StandardCharsets.UTF_8)
            .should(startWith("# faq-xiyoulinux"))
          row.getString(2) should be("fff7062de8474d10a67d417ccea87ba6f58ca81d")
          row.getBoolean(3) should be(false)
          row.getString(4) should be("README.md")
          row.getString(5) should be("github.com/mawag/faq-xiyoulinux")
          row.getString(6) should be("refs/heads/HEAD")
        case (i, _) if i > 3 => fail("commits not expected")
        case _ =>
      }, total = 4, columnsCount = 7
    )
  }

  "BlobIterator" should "filter repositories if given" in {
    val refFilters = Array[CompiledFilter](new EqualFilter("reference_name", "refs/heads/HEAD"))

    testIterator(
      new BlobIterator(
        Array(
          "file_hash",
          "content",
          "commit_hash",
          "is_binary",
          "path",
          "repository_id"
        ), _, refFilters), {
        case (0, row) =>
          row.getString(0) should be("733c072369ca77331f392c40da7404c85c36542c")
          new String(row.getAs[Array[Byte]](1), StandardCharsets.UTF_8)
            .should(startWith("                    GNU GENERAL PUBLIC LICENSE"))
          row.getString(2) should be("fff7062de8474d10a67d417ccea87ba6f58ca81d")
          row.getBoolean(3) should be(false)
          row.getString(4) should be("LICENSE")
          row.getString(5) should be("github.com/xiyou-linuxer/faq-xiyoulinux")
        case (1, row) =>
          row.getString(0) should be("2d2ad68c14c51e62595125b86b464427f6bf2126")
          new String(row.getAs[Array[Byte]](1), StandardCharsets.UTF_8)
            .should(startWith("# faq-xiyoulinux"))
          row.getString(2) should be("fff7062de8474d10a67d417ccea87ba6f58ca81d")
          row.getBoolean(3) should be(false)
          row.getString(4) should be("README.md")
          row.getString(5) should be("github.com/xiyou-linuxer/faq-xiyoulinux")
        case (2, row) =>
          row.getString(0) should be("733c072369ca77331f392c40da7404c85c36542c")
          new String(row.getAs[Array[Byte]](1), StandardCharsets.UTF_8)
            .should(startWith("                    GNU GENERAL PUBLIC LICENSE"))
          row.getString(2) should be("fff7062de8474d10a67d417ccea87ba6f58ca81d")
          row.getBoolean(3) should be(false)
          row.getString(4) should be("LICENSE")
          row.getString(5) should be("github.com/mawag/faq-xiyoulinux")
        case (3, row) =>
          row.getString(0) should be("2d2ad68c14c51e62595125b86b464427f6bf2126")
          new String(row.getAs[Array[Byte]](1), StandardCharsets.UTF_8)
            .should(startWith("# faq-xiyoulinux"))
          row.getString(2) should be("fff7062de8474d10a67d417ccea87ba6f58ca81d")
          row.getBoolean(3) should be(false)
          row.getString(4) should be("README.md")
          row.getString(5) should be("github.com/mawag/faq-xiyoulinux")

        case (i, _) if i > 3 => fail("commits not expected")
        case _ =>

      }, total = 4, columnsCount = 6
    )
  }

  "BlobIterator" should "return only files for given hashes if they are given" in {
    val refFilters = Array[CompiledFilter](
      new InFilter("commit_hash", Array(
        "fff7062de8474d10a67d417ccea87ba6f58ca81d", "a574356dab47de78259713af2f62955408395974"
      ))
    )

    testIterator(
      new BlobIterator(
        Array(
          "file_hash",
          "content",
          "commit_hash",
          "is_binary",
          "path"
        ), _, refFilters), {
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

        case (i, _) if i > 11 => fail("commits not expected")
        case _ =>

      }, total = 12, columnsCount = 5
    )
  }

  "BlobIterator" should "return only files for given hashes and repos if they are given" in {
    val refFilters = Array[CompiledFilter](
      new EqualFilter("repository_id", "github.com/xiyou-linuxer/faq-xiyoulinux"),
      new InFilter("commit_hash", Array(
        "fff7062de8474d10a67d417ccea87ba6f58ca81d", "a574356dab47de78259713af2f62955408395974"
      ))
    )

    testIterator(
      new BlobIterator(
        Array(
          "file_hash",
          "content",
          "commit_hash",
          "is_binary",
          "path",
          "repository_id",
          "reference_name"
        ), _, refFilters), {
        case (0, row) =>
          row.getString(0) should be("733c072369ca77331f392c40da7404c85c36542c")
          new String(row.getAs[Array[Byte]](1), StandardCharsets.UTF_8)
            .should(startWith("                    GNU GENERAL PUBLIC LICENSE"))
          row.getString(2) should be("fff7062de8474d10a67d417ccea87ba6f58ca81d")
          row.getBoolean(3) should be(false)
          row.getString(4) should be("LICENSE")
          row.getString(5) should be("github.com/xiyou-linuxer/faq-xiyoulinux")
          row.getString(6) should be("refs/heads/HEAD")
        case (1, row) =>
          row.getString(0) should be("2d2ad68c14c51e62595125b86b464427f6bf2126")
          new String(row.getAs[Array[Byte]](1), StandardCharsets.UTF_8)
            .should(startWith("# faq-xiyoulinux"))
          row.getString(2) should be("fff7062de8474d10a67d417ccea87ba6f58ca81d")
          row.getBoolean(3) should be(false)
          row.getString(4) should be("README.md")
          row.getString(5) should be("github.com/xiyou-linuxer/faq-xiyoulinux")
          row.getString(6) should be("refs/heads/HEAD")
        case (2, row) =>
          row.getString(0) should be("047b4a9cfea20a4485b5413a8771e98f7aa1a5c7")
          new String(row.getAs[Array[Byte]](1), StandardCharsets.UTF_8) should startWith(".idea")
          row.getString(2) should be("a574356dab47de78259713af2f62955408395974")
          row.getBoolean(3) should be(false)
          row.getString(4) should be(".gitignore")
          row.getString(5) should be("github.com/xiyou-linuxer/faq-xiyoulinux")
          row.getString(6) should be("refs/heads/develop")
        case (3, row) =>
          row.getString(0) should be("733c072369ca77331f392c40da7404c85c36542c")
          new String(row.getAs[Array[Byte]](1), StandardCharsets.UTF_8)
            .should(startWith("                    GNU GENERAL PUBLIC LICENSE"))
          row.getString(2) should be("a574356dab47de78259713af2f62955408395974")
          row.getBoolean(3) should be(false)
          row.getString(4) should be("LICENSE")
          row.getString(5) should be("github.com/xiyou-linuxer/faq-xiyoulinux")
          row.getString(6) should be("refs/heads/develop")

        case (i, _) if i >= 412 => fail("commits not expected")
        case _ =>

      }, total = 412, columnsCount = 7
    )
  }

  "BlobIterator" should "not fail with other, un-supported filters" in {
    val filters = Array[CompiledFilter](new EqualFilter("path", "README"))

    testIterator(
      new BlobIterator(
        Array(
          "file_hash",
          "content",
          "commit_hash",
          "is_binary",
          "path"
        ), _, filters), {
        case _ =>
      }, total = 22187, columnsCount = 5)
  }

}
=======
//import tech.sourced.api.util.{CompiledFilter, EqualFilter}

class BlobIteratorSpec extends FlatSpec with BaseRootedRepoIterator {

}
>>>>>>> WIP: chainable iterators
