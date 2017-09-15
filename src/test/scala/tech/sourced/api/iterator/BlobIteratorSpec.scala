package tech.sourced.api.iterator

import java.nio.charset.StandardCharsets

import org.scalatest.FlatSpec
import tech.sourced.api.util.{CompiledFilter, EqualFilter}

class BlogIteratorSpec extends FlatSpec with BaseRootedRepoIterator {

  "BlobIterator" should "return all blobs for files at heads of all refs in repository" in {
    testIterator(
      new BlobIterator(
        Array(
          "file_hash",
          "content",
          "commit_hash",
          "is_binary",
          "path"
        ), _, Array[CompiledFilter]()), {
        case (0, row) =>
          row.getString(0) should be("733c072369ca77331f392c40da7404c85c36542c")
          new String(row.getAs[Array[Byte]](1), StandardCharsets.UTF_8) should startWith("                    GNU GENERAL PUBLIC LICENSE")
          row.getString(2) should be("fff7062de8474d10a67d417ccea87ba6f58ca81d")
          row.getBoolean(3) should be(false)
          row.getString(4) should be("LICENSE")
        case (1, row) =>
          row.getString(0) should be("2d2ad68c14c51e62595125b86b464427f6bf2126")
          new String(row.getAs[Array[Byte]](1), StandardCharsets.UTF_8) should startWith("# faq-xiyoulinux")
          row.getString(2) should be("fff7062de8474d10a67d417ccea87ba6f58ca81d")
          row.getBoolean(3) should be(false)
          row.getString(4) should be("README.md")
        case (2, row) =>
          row.getString(0) should be("733c072369ca77331f392c40da7404c85c36542c")
          new String(row.getAs[Array[Byte]](1), StandardCharsets.UTF_8) should startWith("                    GNU GENERAL PUBLIC LICENSE")
          row.getString(2) should be("fff7062de8474d10a67d417ccea87ba6f58ca81d")
          row.getBoolean(3) should be(false)
          row.getString(4) should be("LICENSE")
        case (3, row) =>
          row.getString(0) should be("2d2ad68c14c51e62595125b86b464427f6bf2126")
          new String(row.getAs[Array[Byte]](1), StandardCharsets.UTF_8) should startWith("# faq-xiyoulinux")
          row.getString(2) should be("fff7062de8474d10a67d417ccea87ba6f58ca81d")
          row.getBoolean(3) should be(false)
          row.getString(4) should be("README.md")
        case (4, row) =>
          row.getString(0) should be("047b4a9cfea20a4485b5413a8771e98f7aa1a5c7")
          new String(row.getAs[Array[Byte]](1), StandardCharsets.UTF_8) should startWith(".idea/*")
          row.getString(2) should be("880653c14945dbbc915f1145561ed3df3ebaf168")
          row.getBoolean(3) should be(false)
          row.getString(4) should be(".gitignore")

        case (i, _) if i > 432 => fail("commits not expected")
        case _ =>

      }, total = 433, columnsCount = 5
    )
  }


  "BlobIterator" should "filter refs" in {
    val refFilters = Array[CompiledFilter](new EqualFilter("reference_name", "refs/heads/HEAD"))

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
          new String(row.getAs[Array[Byte]](1), StandardCharsets.UTF_8) should startWith("                    GNU GENERAL PUBLIC LICENSE")
          row.getString(2) should be("fff7062de8474d10a67d417ccea87ba6f58ca81d")
          row.getBoolean(3) should be(false)
          row.getString(4) should be("LICENSE")
        case (1, row) =>
          row.getString(0) should be("2d2ad68c14c51e62595125b86b464427f6bf2126")
          new String(row.getAs[Array[Byte]](1), StandardCharsets.UTF_8) should startWith("# faq-xiyoulinux")
          row.getString(2) should be("fff7062de8474d10a67d417ccea87ba6f58ca81d")
          row.getBoolean(3) should be(false)
          row.getString(4) should be("README.md")

        case (i, _) if i > 3 => fail("commits not expected")
        case _ =>

      }, total = 4, columnsCount = 5
    )
  }
}