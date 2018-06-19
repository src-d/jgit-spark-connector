package tech.sourced.engine.udf

import gopkg.in.bblfsh.sdk.v1.uast.generated.{Node, Role}
import org.apache.spark.sql.types.{StringType, StructField}
import org.scalatest.{FlatSpec, Matchers}
import tech.sourced.engine._

class CustomUDFSpec extends FlatSpec with Matchers with BaseSparkSpec {

  val fileSeq = Seq(
    ("hash1", false, "foo.py", "with open('somefile.txt') as f: contents=f.read()".getBytes),
    ("hash2", false, "bar.java", "public class Hello extends GenericServlet { }".getBytes),
    ("hash3", false, "baz.go", null.asInstanceOf[Array[Byte]]),
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
  }

  it should "guess the correct language" in {
    val spark = ss
    import spark.implicits._

    val languagesDf = fileSeq.toDF(fileColumns: _*).classifyLanguages
    languagesDf.select('path, 'lang).collect().foreach(row => row.getString(0) match {
      case "foo.py" => row.getString(1) should be("Python")
      case "bar.java" => row.getString(1) should be("Java")
      case "baz.go" => row.getString(1) should be("Go")
      case "no-filename" => row.getString(1) should be("Python")
      case _ => row.getString(1) should be(null)
    })
  }

  it should "works as a registered udf" in {
    val spark = ss
    import spark.implicits._

    fileSeq.toDF(fileColumns: _*).createOrReplaceTempView("files")

    val languagesDf = spark.sqlContext.sql("SELECT *, "
      + ClassifyLanguagesUDF.name + "(is_binary, path, content) AS lang FROM files")
    languagesDf.schema.fields should contain(StructField("lang", StringType))
  }

  "UAST parsing of content" should "produce non-empty results" in {
    val spark = ss
    import spark.implicits._

    val uastDf = fileSeq.toDF(fileColumns: _*).extractUASTs()

    uastDf.columns should contain("uast")

    uastDf.take(2).zipWithIndex.map {
      case (row, 0) => assert(row(4).asInstanceOf[Seq[Byte]].nonEmpty)
      case (row, 1) => assert(row(4).asInstanceOf[Seq[Byte]].nonEmpty)
    }
  }

  "UAST parsing of content with language" should "produce non-empty results" in {
    val spark = ss
    import spark.implicits._

    val uastDf = fileSeq.toDF(fileColumns: _*).classifyLanguages.extractUASTs()

    uastDf.columns should contain("lang")
    uastDf.columns should contain("uast")

    uastDf.take(2).zipWithIndex.map {
      case (row, 0) => assert(row(5).asInstanceOf[Seq[Byte]].nonEmpty)
      case (row, 1) => assert(row(5).asInstanceOf[Seq[Byte]].nonEmpty)
    }
  }

  "UAST on unsupported language" should "not query bblfsh" in {
    val spark = ss
    import spark.implicits._

    val uastDf = Seq(
      ("hash1", false, "foo.md", "yada yada".getBytes)
    ).toDF(fileColumns: _*).classifyLanguages.extractUASTs()

    uastDf.collect().zipWithIndex.map {
      case (row, 0) => row(5).asInstanceOf[Seq[_]].length should be(0)
    }
  }

  it should "work as a registered udf in SQL" in {
    val spark = ss
    import spark.implicits._

    spark.catalog.listFunctions()
      .filter('name like "%" + ExtractUASTsUDF.name + "%")

    fileSeq.toDF(fileColumns: _*).createOrReplaceTempView("uasts")

    val uastsDF = spark.sqlContext.sql("SELECT *, "
      + ExtractUASTsUDF.name + "(path, content, null) AS uast FROM uasts")
    uastsDF.collect
    uastsDF.columns should contain("uast")
  }

  "QueryXPath" should "query an UAST using xpath" in {
    val spark = ss
    import spark.implicits._

    fileSeq.take(1).toDF(fileColumns: _*).createOrReplaceTempView("files")

    val uastsDF = spark.sqlContext
      .sql(s"SELECT path, ${ExtractUASTsUDF.name}" + s"(path, content, null) "
        + s"as uast FROM files")

    val uast = uastsDF.first()

    val uasts: Seq[(String, Seq[Array[Byte]])] = Seq(
      (uast(0).asInstanceOf[String], uast(1).asInstanceOf[Seq[Array[Byte]]])
    )
    uasts.toDF("path", "uast").createOrReplaceTempView("uasts")

    val filteredDf = spark.sqlContext.sql(s"SELECT ${QueryXPathUDF.name}" +
      s"(uast, '//*[@roleIdentifier]') FROM uasts")
    val nodes = filteredDf.first()(0).asInstanceOf[Seq[Array[Byte]]]
      .map(Node.parseFrom)
      .filter(!_.roles.contains(Role.INCOMPLETE))

    nodes.length should be(5)
    nodes.map(_.token) should contain allOf(
      "contents",
      "read",
      "open",
      "f"
    )
  }

  it should "query using queryUAST method of dataframe" in {
    val spark = ss
    import spark.implicits._

    val identifiers = fileSeq.take(1).toDF(fileColumns: _*)
      .classifyLanguages
      .extractUASTs()
      .queryUAST("//*[@roleIdentifier and not(@roleIncomplete)]")
      .collect()
      .map(row => row(row.fieldIndex("result")))
      .flatMap(_.asInstanceOf[Seq[Array[Byte]]])
      .map(Node.parseFrom)
      .map(_.token)

    identifiers.length should be(5)
    identifiers should contain allOf(
      "contents",
      "read",
      "open",
      "f"
    )
  }

  it should "query using queryUAST method of dataframe with custom cols" in {
    val spark = ss
    import spark.implicits._

    val identifiers = fileSeq.take(1).toDF(fileColumns: _*)
      .classifyLanguages
      .extractUASTs()
      .queryUAST("//*[@roleIdentifier]")
      .queryUAST("/*[not(@roleIncomplete)]", "result", "result2")
      .collect()
      .map(row => row(row.fieldIndex("result2")))
      .flatMap(_.asInstanceOf[Seq[Array[Byte]]])
      .map(Node.parseFrom)
      .map(_.token)

    identifiers.length should be(5)
    identifiers should contain allOf(
      "contents",
      "read",
      "open",
      "f"
    )
  }

  "ExtractTokensUDF" should "extract the tokens in a column" in {
    val spark = ss
    import spark.implicits._

    val identifiers = fileSeq.take(1).toDF(fileColumns: _*)
      .classifyLanguages
      .extractUASTs()
      .queryUAST("//*[@roleIdentifier and not(@roleIncomplete)]")
      .extractTokens()
      .collect()
      .map(row => row(row.fieldIndex("tokens")))
      .flatMap(_.asInstanceOf[Seq[String]])

    identifiers.length should be(5)
    identifiers should contain allOf(
      "contents",
      "read",
      "open",
      "f"
    )
  }

}
