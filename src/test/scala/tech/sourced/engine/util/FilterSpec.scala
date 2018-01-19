package tech.sourced.engine.util

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types.StringType
import org.scalatest.{FlatSpec, Matchers}

class FilterSpec extends FlatSpec with Matchers {
  "CompiledFilters" should "filter properly depending of his type" in {
    val eq = EqualFilter(Attr("test", ""), "a")

    eq.eval("a") should be(true)
    eq.eval("b") should be(false)

    val notEq = NotFilter(EqualFilter(Attr("test", ""), "a"))

    notEq.eval("a") should be(false)
    notEq.eval("b") should be(true)

    val in = InFilter(Attr("test", ""), Array("a", "b", "c"))

    in.eval("a") should be(true)
    in.eval("b") should be(true)
    in.eval("c") should be(true)
    in.eval("d") should be(false)

    val gt = GreaterThanFilter(Attr("test", ""), 5)

    gt.eval(4) should be(false)
    gt.eval(5) should be(false)
    gt.eval(6) should be(true)

    val gte = GreaterThanOrEqualFilter(Attr("test", ""), 5)

    gte.eval(4) should be(false)
    gte.eval(5) should be(true)
    gte.eval(6) should be(true)

    val lt = LessThanFilter(Attr("test", ""), 5)

    lt.eval(4) should be(true)
    lt.eval(5) should be(false)
    lt.eval(6) should be(false)

    val lte = LessThanOrEqualFilter(Attr("test", ""), 5)

    lte.eval(4) should be(true)
    lte.eval(5) should be(true)
    lte.eval(6) should be(false)
  }

  "ColumnFilter" should "process correctly columns" in {
    // test = 'val' AND test IS NOT NULL AND test2 = 'val2' AND test3 IN ('a', 'b')
    val f = Filter.compile(And(
      And(
        And(
          EqualTo(AttributeReference("test", StringType)(), Literal("val")),
          IsNotNull(AttributeReference("test", StringType)())
        ),
        EqualTo(AttributeReference("test2", StringType)(), Literal("val2"))
      ),
      In(AttributeReference("test3", StringType)(), Seq(Literal("a"), Literal("b")))
    ))

    f.length should be(4)
    val filters = Filters(f)
    filters.matches(Seq("test"), "val") should be(true)
    filters.matches(Seq("test2"), "val") should be(false)
    filters.matches(Seq("test3"), "b") should be(true)
  }

  "ColumnFilter" should "handle correctly unsupported filters" in {
    val f = Filter.compile(StartsWith(AttributeReference("test", StringType)(), Literal("a")))

    f.length should be(0)
  }
}
