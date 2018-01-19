package tech.sourced.engine

import java.sql.{Date, Timestamp}

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types.{IntegerType, MetadataBuilder, StringType}
import org.apache.spark.unsafe.types.UTF8String
import org.scalatest.{FlatSpec, Matchers}
import QueryBuilder._

class QueryBuilderSpec extends FlatSpec with Matchers {

  "QueryBuilder.qualify" should "qualify and quote col" in {
    val expected = s"${prefixTable("foo")}.`bar`"
    qualify("foo", "bar") should be(expected)
    qualify(attr("foo", "bar")) should be(expected)
  }

  "QueryBuilder.compileValue" should "return compiled value" in {
    val now = System.currentTimeMillis
    val cases = Seq(
      (UTF8String.fromString("foo"), "'foo'"),
      ("fo'o", "'fo''o'"),
      (new Timestamp(now), s"'${new Timestamp(now)}'"),
      (new Date(now), s"'${new Date(now)}'"),
      (Seq("a", 1, true), "'a', 1, 1"),
      (true, 1),
      (false, 0)
    )

    cases.foreach {
      case (input, expected) =>
        compileValue(input) should be(expected)
    }
  }

  "QueryBuilder.compileFilter" should "compile the filters to SQL" in {
    val col = qualify("foo", "bar")
    val cases = Seq(
      (EqualTo(attr("foo", "bar"), Literal(1, IntegerType)),
        s"$col = 1"),
      (EqualNullSafe(attr("foo", "bar"), Literal(1, IntegerType)),
        s"(NOT ($col != 1 OR $col IS NULL OR 1 IS NULL) OR ($col IS NULL AND 1 IS NULL))"),
      (LessThan(attr("foo", "bar"), Literal(1, IntegerType)),
        s"$col < 1"),
      (GreaterThan(attr("foo", "bar"), Literal(1, IntegerType)),
        s"$col > 1"),
      (LessThanOrEqual(attr("foo", "bar"), Literal(1, IntegerType)),
        s"$col <= 1"),
      (GreaterThanOrEqual(attr("foo", "bar"), Literal(1, IntegerType)),
        s"$col >= 1"),
      (IsNull(attr("foo", "bar")), s"$col IS NULL"),
      (IsNotNull(attr("foo", "bar")), s"$col IS NOT NULL"),
      (In(attr("foo", "bar"), Seq()), s"CASE WHEN $col IS NULL THEN NULL ELSE FALSE END"),
      (In(attr("foo", "bar"), Seq(Literal(1, IntegerType), Literal(2, IntegerType))),
        s"$col IN (1, 2)"),
      (Not(EqualTo(attr("foo", "bar"), Literal(1, IntegerType))),
        s"(NOT ($col = 1))"),
      (Or(EqualTo(attr("foo", "bar"), Literal(1, IntegerType)),
        EqualTo(attr("foo", "bar"), Literal(2, IntegerType))
      ),
        s"(($col = 1) OR ($col = 2))"),
      (And(EqualTo(attr("foo", "bar"), Literal(1, IntegerType)),
        EqualTo(attr("foo", "bar"), Literal(2, IntegerType))
      ),
        s"($col = 1) AND ($col = 2)")
    )

    cases.foreach {
      case (expr, expected) =>
        compileFilter(expr).get should be(expected)
    }
  }

  "QueryBuilder.selectedFields" should "return SQL for selected tables" in {
    QueryBuilder(tables = Seq("repositories"))
      .selectedFields should be(s"${qualify("repositories", "id")}")

    QueryBuilder(fields = Seq(
      attr("repositories", "id"),
      attr("references", "name")
    )).selectedFields should be(
      s"${qualify("repositories", "id")}, ${qualify("references", "name")}"
    )
  }

  "QueryBuilder.whereClause" should "return SQL for where clause" in {
    QueryBuilder().whereClause should be("")

    QueryBuilder(filters = Seq(
      EqualTo(attr("foo", "bar"), Literal(1, IntegerType))
    )).whereClause should be(s"WHERE ${qualify("foo", "bar")} = 1")

    QueryBuilder(filters = Seq(
      EqualTo(attr("foo", "bar"), Literal(1, IntegerType)),
      EqualTo(attr("foo", "baz"), Literal(2, IntegerType))
    )).whereClause should be(s"WHERE ${qualify("foo", "bar")} = 1 AND ${qualify("foo", "baz")} = 2")
  }

  "QueryBuilder.selectedTables" should "return SQL for selected tables" in {
    QueryBuilder(tables = Seq("repositories"))
      .selectedTables should be(s"${prefixTable("repositories")}")

    QueryBuilder(joins = Seq(
      Join("repositories", "references", Seq(
        JoinCondition("repositories", "id", "references", "repository_id")
      )),
      Join("references", "commits", Seq(
        JoinCondition("references", "repository_id", "commits", "repository_id"),
        JoinCondition("references", "name", "commits", "reference_name")
      ))
    )).selectedTables should be(s"${prefixTable("repositories")} INNER JOIN " +
      s"${prefixTable("references")} ON (" +
      s"${qualify("repositories", "id")} = ${qualify("references", "repository_id")}) INNER JOIN " +
      s"${prefixTable("commits")} ON (${qualify("references", "repository_id")} = " +
      s"${qualify("commits", "repository_id")} AND ${qualify("references", "name")} = " +
      s"${qualify("commits", "reference_name")})")
  }

  "QueryBuilder.sql" should "return SQL for the query" in {
    QueryBuilder(
      fields = Seq(attr("repositories", "id")),
      tables = Seq("repositories"),
      filters = Seq(EqualTo(attr("repositories", "id"), Literal("foo", StringType)))
    ).sql should be(s"SELECT ${qualify("repositories", "id")} " +
      s"FROM ${prefixTable("repositories")} " +
      s"WHERE ${qualify("repositories", "id")} = ${compileValue("foo")}")
  }

  def attr(table: String, name: String): Attribute =
    AttributeReference(
      name,
      StringType,
      nullable = false,
      new MetadataBuilder().putString(Sources.SourceKey, table).build()
    )()

}
