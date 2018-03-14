package tech.sourced.engine.util

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.unsafe.types.UTF8String

case class Filters(filters: Seq[CompiledFilter]) {

  private val indexedFilters: Map[String, Seq[CompiledFilter]] = filters
    .map(f => (f.field, f))
    .groupBy(_._1)
    .mapValues(_.map(_._2))

  /**
    * Returns whether the given values matches all the filters of all the given fields.
    *
    * @param fields field names
    * @param value  values to check
    * @return whether the value matches
    */
  def matches(fields: Seq[String], value: Any): Boolean = fields
    .flatMap(f => indexedFilters.find {
      case (field, _) => field == f
    }.map(_._2).getOrElse(Seq()))
    .forall(f => f.eval(value))

  /**
    * Returns whether the filter collection contains any filters for any of the fields.
    *
    * @param fields field names
    * @return whether there's filters for any of the fields
    */
  def hasFilters(fields: String*): Boolean = fields.exists(indexedFilters.keySet.contains(_))

  /**
    * Returns all the sources in all the filters.
    *
    * @return sequence of sources
    */
  def sources: Seq[String] = filters.flatMap(_.sources).distinct

}

object Filter {

  /**
    * Compiles the given sequence of expressions and returns a collection of filters.
    *
    * @param e expressions
    * @return filters
    */
  def compile(e: Seq[Expression]): Filters = Filters(e.flatMap(compile))

  /**
    * Compiles expressions into filters. All filters returned can be joined
    * using ANDs. Ands are split in multiple filters.
    *
    * @param e expression to compile
    * @return compiled filters
    */
  def compile(e: Expression): Seq[CompiledFilter] = e match {
    case Equality(attr: AttributeReference, Literal(value, _)) =>
      Seq(EqualFilter(new Attr(attr.toAttribute), transformLiteral(value)))

    case IsNull(attr: AttributeReference) =>
      Seq(EqualFilter(new Attr(attr.toAttribute), null))

    case IsNotNull(attr: AttributeReference) =>
      Seq(NotFilter(EqualFilter(new Attr(attr.toAttribute), null)))

    case Not(expr) => compile(expr).map(NotFilter)

    case LessThan(attr: AttributeReference, Literal(value, _)) =>
      Seq(LessThanFilter(new Attr(attr.toAttribute), value))

    case GreaterThan(attr: AttributeReference, Literal(value, _)) =>
      Seq(GreaterThanFilter(new Attr(attr.toAttribute), value))

    case GreaterThanOrEqual(attr: AttributeReference, Literal(value, _)) =>
      Seq(GreaterThanOrEqualFilter(new Attr(attr.toAttribute), value))

    case LessThanOrEqual(attr: AttributeReference, Literal(value, _)) =>
      Seq(LessThanOrEqualFilter(new Attr(attr.toAttribute), value))

    case In(attr: AttributeReference, values)
      if values.forall(_.isInstanceOf[Literal]) =>
      Seq(InFilter(
        new Attr(attr.toAttribute),
        values.map({ case Literal(value, _) => transformLiteral(value) })
      ))

    case And(l, r) => compile(l) ++ compile(r)

    case _ => Seq()
  }

  private def transformLiteral(value: Any): Any = value match {
    case v: UTF8String => v.toString
    case v => v
  }

}

/**
  * Filter compiled from an expression to be used inside the iterators.
  */
sealed trait CompiledFilter {

  /**
    * Evaluate the filter and check whether the value matches.
    *
    * @param value value to check
    * @return whether it matches
    */
  def eval(value: Any): Boolean

  /**
    * Returns the list of sources in this filter.
    *
    * @return sources list
    */
  def sources: Seq[String]

  /**
    * Returns the name of the field being checked in the filter.
    *
    * @return field name
    */
  def field: String

}

abstract class AttrFilter(attr: Attr) extends CompiledFilter {

  def this() = this(Attr("",""))

  /**
    * @inheritdoc
    */
  def sources: Seq[String] = Seq(attr.source)

  /**
    * @inheritdoc
    */
  def field: String = attr.name

}

/**
  * Represents a column with a name and a source.
  *
  * @param name   column name
  * @param source table source
  */
case class Attr(name: String, source: String) {

  def this() = this("","")

  /**
    * Creates a new Attr from an Attribute
    *
    * @constructor
    * @param attr attribute
    * @return
    */
  def this(attr: Attribute) = this(attr.name, if (attr.metadata.contains("source")) {
    attr.metadata.getString("source")
  } else {
    ""
  })

}

/**
  * Filter that checks that the value of a column is in the given sequence of values.
  *
  * @param attr column attribute
  * @param vals values
  */
case class InFilter(attr: Attr, vals: Seq[Any]) extends AttrFilter(attr) {

  def this() = this(Attr("",""), Seq())

  /**
    * @inheritdoc
    */
  override def toString: String = s"${attr.name} IN (${vals.mkString(", ")})"

  /**
    * @inheritdoc
    */
  def eval(value: Any): Boolean = vals.contains(value)

}

/**
  * Filter that checks the column value is the given value.
  *
  * @param attr  column attribute
  * @param value value to check
  */
case class EqualFilter(attr: Attr, value: Any) extends AttrFilter(attr) {

  def this() = this(Attr("",""), "")

  /**
    * @inheritdoc
    */
  override def toString: String = s"${attr.name} = '$value'"

  /**
    * @inheritdoc
    */
  def eval(v: Any): Boolean = value == v

}

/**
  * Filter that negates the match of the given filter.
  *
  * @param f filter to negate
  */
case class NotFilter(f: CompiledFilter) extends CompiledFilter {

  def this() = this(EqualFilter(Attr("",""), ""))

  /**
    * @inheritdoc
    */
  override def toString: String = s"NOT (${f.toString})"

  /**
    * @inheritdoc
    */
  def eval(value: Any): Boolean = !f.eval(value)

  /**
    * @inheritdoc
    */
  def sources: Seq[String] = f.sources

  /**
    * @inheritdoc
    */
  def field: String = f.field

}

case class GreaterThanFilter(attr: Attr, value: Any) extends AttrFilter(attr) {

  def this() = this(Attr("",""), "")

  /**
    * @inheritdoc
    */
  override def toString: String = s"${attr.name} > $value"

  /**
    * @inheritdoc
    */
  def eval(v: Any): Boolean = (v, value) match {
    case (l: Int, r: Int) => l > r
    case (l: Float, r: Float) => l > r
    case (l: Double, r: Double) => l > r
    case (l: Long, r: Long) => l > r
  }

}

case class LessThanFilter(attr: Attr, value: Any) extends AttrFilter(attr) {

  def this() = this(Attr("",""), "")

  /**
    * @inheritdoc
    */
  override def toString: String = s"${attr.name} < $value"

  /**
    * @inheritdoc
    */
  def eval(v: Any): Boolean = (v, value) match {
    case (l: Int, r: Int) => l < r
    case (l: Float, r: Float) => l < r
    case (l: Double, r: Double) => l < r
    case (l: Long, r: Long) => l < r
  }

}

case class GreaterThanOrEqualFilter(attr: Attr, value: Any) extends AttrFilter(attr) {

  def this() = this(Attr("",""), "")

  /**
    * @inheritdoc
    */
  override def toString: String = s"${attr.name} >= $value"

  /**
    * @inheritdoc
    */
  def eval(v: Any): Boolean = (v, value) match {
    case (l: Int, r: Int) => l >= r
    case (l: Float, r: Float) => l >= r
    case (l: Double, r: Double) => l >= r
    case (l: Long, r: Long) => l >= r
  }

}

case class LessThanOrEqualFilter(attr: Attr, value: Any) extends AttrFilter(attr) {

  def this() = this(Attr("",""), "")

  /**
    * @inheritdoc
    */
  override def toString: String = s"${attr.name} <= $value"

  /**
    * @inheritdoc
    */
  def eval(v: Any): Boolean = (v, value) match {
    case (l: Int, r: Int) => l <= r
    case (l: Float, r: Float) => l <= r
    case (l: Double, r: Double) => l <= r
    case (l: Long, r: Long) => l <= r
  }

}
