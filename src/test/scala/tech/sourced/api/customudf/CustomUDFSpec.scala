package tech.sourced.api.customudf

import org.apache.spark.sql.types.{StringType, StructField}
import org.scalatest.{FlatSpec, Matchers}
import tech.sourced.api.{BaseSparkSpec, Implicits}

class CustomUDFSpec extends FlatSpec with Matchers with BaseSparkSpec {

  val fileSeq = Seq(
    ("hash1", false, "foo.py", null.asInstanceOf[Array[Byte]]),
    ("hash2", false, "bar.scala", null.asInstanceOf[Array[Byte]]),
    ("hash3", false, "baz.go", null.asInstanceOf[Array[Byte]]),
    ("hash4", false, "no-filename", "#!/usr/bin/env python -tt".getBytes()),
    ("hash5", false, "unknown", null.asInstanceOf[Array[Byte]]),
    ("hash6", true, "binary-file", Array[Byte](0, 0, 0, 0))
  )

  val fileColumns = Array("file_hash", "is_binary", "path", "content")

  "Language detection" should "works correctly" in {
    val spark = ss

    import Implicits._
    import spark.implicits._

    val languagesDf = fileSeq.toDF(fileColumns: _*).classifyLanguages

    languagesDf.schema.fields should contain(StructField("lang", StringType))
    languagesDf.show
  }

  it should "guess the correct language" in {
    val spark = ss
    import Implicits._
    import spark.implicits._

    val languagesDf = fileSeq.toDF(fileColumns: _*).classifyLanguages
    languagesDf.select('path, 'lang).collect().foreach(row => row.getString(0) match {
      case "foo.py" => row.getString(1) should be("Python")
      case "bar.scala" => row.getString(1) should be("Scala")
      case "baz.go" => row.getString(1) should be("Go")
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

}
