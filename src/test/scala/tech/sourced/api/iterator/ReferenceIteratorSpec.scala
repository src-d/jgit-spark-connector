package tech.sourced.api.iterator

import org.scalatest.FlatSpec

class ReferenceIteratorSpec extends FlatSpec with BaseRootedRepoIterator {
/*
  "ReferenceIterator" should "return all references from all repositories into a siva file" in {
    testIterator(
      new ReferenceIterator(Array("repository_id", "name", "hash"), _), {
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

  "ReferenceIterator" should "return only specified columns" in {
    testIterator(
      new ReferenceIterator(Array("repository_id", "name"), _), {
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
  }*/
}
