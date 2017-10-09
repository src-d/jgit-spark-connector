package tech.sourced.api.iterator

import java.sql.Timestamp

import org.scalatest.FlatSpec
import tech.sourced.api.util.{Attr, EqualFilter, InFilter}

class CommitIteratorSpec extends FlatSpec with BaseRootedRepoIterator {

  private val cols = Array(
    "repository_id",
    "reference_name",
    "index",
    "hash",
    "message",
    "parents",
    "tree",
    "blobs",
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
          row.getAs[Map[String, String]](6) should be(
            Map(
              "LICENSE" -> "733c072369ca77331f392c40da7404c85c36542c",
              "README.md" -> "2d2ad68c14c51e62595125b86b464427f6bf2126"
            )
          )
          row.getAs[Array[String]](7) should be(
            Array("733c072369ca77331f392c40da7404c85c36542c",
              "2d2ad68c14c51e62595125b86b464427f6bf2126"))
          row.getInt(8) should be(0)
          row.getString(9) should be("wangbo2008@vip.qq.com")
          row.getString(10) should be("wangbo")
          row.getTimestamp(11) should be(new Timestamp(1438072751000L))
          row.getString(12) should be("wangbo2008@vip.qq.com")
          row.getString(13) should be("wangbo")
          row.getTimestamp(14) should be(new Timestamp(1438072751000L))
        case (1, row) =>
          row.getString(0) should be("github.com/mawag/faq-xiyoulinux")
          row.getString(1) should be("refs/heads/HEAD")
          row.getInt(2) should be(0)
          row.getString(3) should be("fff7062de8474d10a67d417ccea87ba6f58ca81d")
          row.getString(4) should be("Initial commit\n")
          row.getAs[Array[String]](5) should be(Array())
          row.getAs[Map[String, String]](6) should be(
            Map(
              "LICENSE" -> "733c072369ca77331f392c40da7404c85c36542c",
              "README.md" -> "2d2ad68c14c51e62595125b86b464427f6bf2126"
            )
          )
          row.getAs[Array[String]](7) should be(
            Array("733c072369ca77331f392c40da7404c85c36542c",
              "2d2ad68c14c51e62595125b86b464427f6bf2126"))
          row.getInt(8) should be(0)
          row.getString(9) should be("wangbo2008@vip.qq.com")
          row.getString(10) should be("wangbo")
          row.getTimestamp(11) should be(new Timestamp(1438072751000L))
          row.getString(12) should be("wangbo2008@vip.qq.com")
          row.getString(13) should be("wangbo")
          row.getTimestamp(14) should be(new Timestamp(1438072751000L))
        case (23, row) =>
          row.getString(0) should be("github.com/xiyou-linuxer/faq-xiyoulinux")
          row.getString(1) should be("refs/heads/develop")
          row.getInt(2) should be(21)
          row.getString(3) should be("25fda7c4d616c8a7c017384d7745342c73eb214d")
          row.getString(4) should be("Merge pull request #5 from xiyou-linuxer/develop\n\nDevelop")
          row.getAs[Array[String]](5) should be(
            Array("531f574cf8c457cbeb4f6a5bae2d81db22c5dc1a",
              "8331656181f9040d805b729946b12fd4382c4665"))
          row.getAs[Map[String, String]](6) should be(
            Map(
              "source/search.php" -> "be1dd14a91679b91151357fc37a84fc6b59be1a6",
              "source/config/db.simple.config.php" -> "a7b19793b35a11d92ee4594829e23392ddf11f79",
              "source/includes/oauth.class.php" -> "95d9b676fc34a4a2c352c905242b03740fe5d2f2",
              "source/oauth/login.php" -> "935780f405f7602ac5008792c73aa932762b4013",
              "source/includes/errshow.class.php" -> "389f98e2d60c5839812ea5bbd2fef0941929a47c",
              "source/includes/show.function.php" -> "3c36e3c735d6411855dee5eaa0b83f378b05341e",
              "README.md" -> "2d2ad68c14c51e62595125b86b464427f6bf2126",
              "source/addquestion.php" -> "97030825f145faee7fb1b275c16b0c369f763ec2",
              "source/index.php" -> "c0e106625193a645af54fda2762d879c990e728e",
              "source/includes/user.class.php" -> "32abf82d6c3b9203b28eb4784a4ad70bdfc3bf0c",
              "source/init.php" -> "42073993b9a76a4cb8af9d6bece8bd7e7a65ab19",
              ".gitignore" -> "047b4a9cfea20a4485b5413a8771e98f7aa1a5c7",
              "docs/class_user接口规范.docx" -> "9ed4707aafb7fab4aa1289b8f44e1c3c2942b2e6",
              "source/includes/index.function.php" -> "9d2fd6ebbb938b6936f5a8beda2e72a3c2acf35b",
              "source/question.php" -> "2453ceecbd5a60937db12ba2886197b3d6cb793d",
              "source/oauth/index.php" -> "e64428200e23f2891d9f1fb8ac6e73270558fcf8",
              "source/includes/db_function.class.php" -> "186255ff7f0b4973d8ec11a5f5aa0ff92f70c24e",
              "source/includes/db.class.php" -> "04da61f5a68624459dbb89c7f9324bca80f7041d",
              ".gitmodules" -> "a206844708eca67b4221c97d7d5dd39127b4fd95",
              "source/view" -> "3558dd448c31f10f3e1b518c39d633fc9396cb69",
              "source/config/oauth.simple.config.php" -> "2d2a7516fa6e64ba9487f6ae9396335eb9b15704",
              "LICENSE" -> "733c072369ca77331f392c40da7404c85c36542c"
            )
          )
          row.getAs[Array[String]](7) should be(
            Array(
              "be1dd14a91679b91151357fc37a84fc6b59be1a6",
              "a7b19793b35a11d92ee4594829e23392ddf11f79",
              "95d9b676fc34a4a2c352c905242b03740fe5d2f2",
              "935780f405f7602ac5008792c73aa932762b4013",
              "389f98e2d60c5839812ea5bbd2fef0941929a47c",
              "3c36e3c735d6411855dee5eaa0b83f378b05341e",
              "2d2ad68c14c51e62595125b86b464427f6bf2126",
              "97030825f145faee7fb1b275c16b0c369f763ec2",
              "c0e106625193a645af54fda2762d879c990e728e",
              "32abf82d6c3b9203b28eb4784a4ad70bdfc3bf0c",
              "42073993b9a76a4cb8af9d6bece8bd7e7a65ab19",
              "047b4a9cfea20a4485b5413a8771e98f7aa1a5c7",
              "9ed4707aafb7fab4aa1289b8f44e1c3c2942b2e6",
              "9d2fd6ebbb938b6936f5a8beda2e72a3c2acf35b",
              "2453ceecbd5a60937db12ba2886197b3d6cb793d",
              "e64428200e23f2891d9f1fb8ac6e73270558fcf8",
              "186255ff7f0b4973d8ec11a5f5aa0ff92f70c24e",
              "04da61f5a68624459dbb89c7f9324bca80f7041d",
              "a206844708eca67b4221c97d7d5dd39127b4fd95",
              "3558dd448c31f10f3e1b518c39d633fc9396cb69",
              "2d2a7516fa6e64ba9487f6ae9396335eb9b15704",
              "733c072369ca77331f392c40da7404c85c36542c"
            )
          )
          row.getInt(8) should be(2)
          row.getString(9) should be("1679211339@qq.com")
          row.getString(10) should be("磊磊")
          row.getTimestamp(11) should be(new Timestamp(1439552359000L))
          row.getString(12) should be("1679211339@qq.com")
          row.getString(13) should be("磊磊")
          row.getTimestamp(14) should be(new Timestamp(1439552359000L))
        case (i, _) if i > 1061 => fail("commits not expected")

        case _ =>
      }, total = 1062, columnsCount = 15
    )
  }

  "CommitIterator" should "return all commits from all repositories into a siva file, " +
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

  "CommitIterator" should "apply passed filters" in {
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

  "CommitIterator" should "apply use prev iterator" in {
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
