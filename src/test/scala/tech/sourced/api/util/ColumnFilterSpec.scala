package tech.sourced.api.util

import org.apache.spark.sql.sources._

import org.scalatest.{FlatSpec, Matchers}

class ColumnFilterSpec extends FlatSpec with Matchers {
  "CompiledFilters" should "filter properly depending of his type" in {
    val eq = new EqualFilter("test", "a")

    eq.matchingCases should be(Map("test" -> Seq("a")))

    eq.eval(Map("test" -> "a")) should be(Some(true))
    eq.eval(Map("test" -> "a", "test3" -> "a")) should be(Some(true))
    eq.eval(Map("test" -> "b")) should be(Some(false))
    eq.eval(Map("test2" -> "b")) should be(None)

    val or = new OrFilter(new EqualFilter("test", "a"), new EqualFilter("test", "b"))

    or.matchingCases should be(Map("test" -> Seq("a", "b")))

    or.eval(Map("test" -> "a")) should be(Some(true))
    or.eval(Map("test" -> "b")) should be(Some(true))
    or.eval(Map("test" -> "c")) should be(Some(false))
    or.eval(Map("test2" -> "b")) should be(None)

    val orTwo = new OrFilter(new EqualFilter("test", "a"), new EqualFilter("test2", "b"))

    orTwo.matchingCases should be(Map("test" -> Seq("a"), "test2" -> Seq("b")))

    orTwo.eval(Map("test" -> "a")) should be(Some(true))
    orTwo.eval(Map("test" -> "b")) should be(Some(false))
    orTwo.eval(Map("test" -> "c")) should be(Some(false))
    orTwo.eval(Map("test2" -> "b")) should be(Some(true))
    orTwo.eval(Map("test3" -> "b")) should be(None)

    val and = new AndFilter(new EqualFilter("test", "a"), new EqualFilter("test2", "b"))

    and.matchingCases should be(Map("test" -> Seq("a"), "test2" -> Seq("b")))

    and.eval(Map("test" -> "a", "test2" -> "b")) should be(Some(true))
    and.eval(Map("test" -> "a", "test2" -> "b", "test3" -> "c")) should be(Some(true))
    and.eval(Map("test" -> "a")) should be(Some(false))
    and.eval(Map("test" -> "c")) should be(Some(false))
    and.eval(Map("test2" -> "b")) should be(Some(false))
    and.eval(Map("test3" -> "b")) should be(None)

    val notEq = NotFilter(new EqualFilter("test", "a"))

    notEq.matchingCases should be(Map())

    notEq.eval(Map("test" -> "a")) should be(Some(false))
    notEq.eval(Map("test" -> "a", "test2" -> "a")) should be(Some(false))
    notEq.eval(Map("test" -> "b", "test2" -> "a")) should be(Some(true))
    notEq.eval(Map("test2" -> "a")) should be(None)
  }

  "ColumnFilter" should "process correctly columns" in {
    val f = ColumnFilter.compileFilter(Or(Or(EqualTo("test", "val"), IsNull("test")), EqualTo("test2", "val2")))

    f.matchingCases should be(Map("test" -> Seq("val", null), "test2" -> Seq("val2")))

    f.toString should be ("((test = val OR test = null) OR test2 = val2)")

    f.eval(Map("test2" -> "val2")) should be(Some(true))
    f.eval(Map("test2" -> "val1")) should be(Some(false))
    f.eval(Map("test" -> "no")) should be(Some(false))
    f.eval(Map("bla" -> "bla")) should be(None)
    f.eval(Map("test" -> null)) should be(Some(true))

    // TODO add more real use cases
  }

  "ColumnFilter" should "handle correctly unsupported filters" in {
    val f = ColumnFilter.compileFilter(In("test", Array("a", "b", "c")))

    f.matchingCases should be(Map())
    f.eval(Map("test" -> "a")) should be(None)
    f.eval(Map("test2" -> "a")) should be(None)
  }
}
