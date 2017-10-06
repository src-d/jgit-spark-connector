package tech.sourced.api.iterator

import org.scalatest.FlatSpec

class RepositoryIteratorSpec extends FlatSpec with BaseRootedRepoIterator {
/*
  "RepositoryIterator" should "return data for all repositories into a siva file" in {
    testIterator(
      new RepositoryIterator(Array("id", "urls", "is_fork"), _), {
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

  "RepositoryIterator" should "return only specified columns" in {
    testIterator(
      new RepositoryIterator(Array("id", "is_fork"), _), {
        case (0, row) =>
          row.getString(0) should be("github.com/xiyou-linuxer/faq-xiyoulinux")
          row.getBoolean(1) should be(false)
        case (1, row) =>
          row.getString(0) should be("github.com/mawag/faq-xiyoulinux")
          row.getBoolean(1) should be(true)
        case (c, _) => fail(s"unexpected row number: $c")
      }, total = 2, columnsCount = 2
    )
  }*/
}
