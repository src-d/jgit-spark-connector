package tech.sourced.api.udf

import org.apache.spark.sql.types.{StringType, StructField}
import org.scalatest.{FlatSpec, Matchers}
import tech.sourced.api._


class CustomUDFSpec extends FlatSpec with Matchers with BaseSparkSpec {

  val fileSeq = Seq(
    ("hash1", false, "foo.py", "with open('somefile.txt') as f: contents=f.read()".getBytes),
    ("hash2", false, "bar.java", "public class Hello extends GenericServlet { }".getBytes),
    ("hash3", false, "baz.go", null.asInstanceOf[Array[Byte]]),
    ("hash3", false, "buz.go", "".getBytes()),
    ("hash4", false, "no-filename", "#!/usr/bin/env python -tt".getBytes()),
    ("hash5", false, "unknown", null.asInstanceOf[Array[Byte]]),
    ("hash6", true, "binary-file", Array[Byte](0, 0, 0, 0))
  )

  val fileColumns = Array("file_hash", "is_binary", "path", "content")

  "Language detection" should "works correctly" in {
    val spark = ss
    import spark.implicits._

    val languagesDf = fileSeq.toDF(fileColumns: _*).classifyLanguages

    languagesDf.schema.fields should contain(StructField("lang", StringType))
    languagesDf.show
  }

  it should "guess the correct language" in {
    val spark = ss
    import spark.implicits._

    val languagesDf = fileSeq.toDF(fileColumns: _*).classifyLanguages
    languagesDf.select('path, 'lang).collect().foreach(row => row.getString(0) match {
      case "foo.py" => row.getString(1) should be("Python")
      case "bar.java" => row.getString(1) should be("Java")
      case "baz.go" => row.getString(1) should be("Go")
      case "buz.go" => row.getString(1) should be("Go")
      case "no-filename" => row.getString(1) should be("Python")
      case _ => row.getString(1) should be(null)
    })

    languagesDf.show
  }

  it should "works as a registered udf" in {
    val spark = ss
    import spark.implicits._

    spark.catalog.listFunctions().filter('name like "%" + ClassifyLanguagesUDF.name + "%").show(false)
    fileSeq.toDF(fileColumns: _*).createTempView("files")

    val languagesDf = spark.sqlContext.sql("SELECT *, " + ClassifyLanguagesUDF.name + "(is_binary, path, content) AS lang FROM files")
    languagesDf.schema.fields should contain(StructField("lang", StringType))

    languagesDf.show
  }

  "UAST parsing of content" should "produce non-empty results" in {
    val spark = ss
    import spark.implicits._

    val uastDf = fileSeq.toDF(fileColumns: _*).extractUASTs

    uastDf.columns should contain("uast")
    uastDf.show

    uastDf.take(2).zipWithIndex.map {
      case (row, 0) => assert(row(3).asInstanceOf[Array[Byte]].isEmpty == false)
      case (row, 1) => assert(row(3).asInstanceOf[Array[Byte]].isEmpty == false)
    }
  }

  "UAST parsing of content with language" should "produce non-empty results" in {
    val spark = ss
    import spark.implicits._

    val uastDf = fileSeq.toDF(fileColumns: _*).classifyLanguages.extractUASTs

    uastDf.columns should contain("lang")
    uastDf.columns should contain("uast")
    uastDf.show

    uastDf.take(2).zipWithIndex.map {
      case (row, 0) => assert(row(3).asInstanceOf[Array[Byte]].isEmpty == false)
      case (row, 1) => assert(row(3).asInstanceOf[Array[Byte]].isEmpty == false)
    }
  }

  it should "work as a registered udf in SQL" in {
    val spark = ss
    import spark.implicits._

    spark.catalog.listFunctions().filter('name like "%" + ExtractUASTsUDF.name + "%").show(false)
    fileSeq.toDF(fileColumns: _*).createTempView("uasts")

    val uastsDF = spark.sqlContext.sql("SELECT *, " + ExtractUASTsUDF.name + "(path, content) AS uast FROM uasts")
    uastsDF.collect
    uastsDF.columns should contain("uast")
  }

}
