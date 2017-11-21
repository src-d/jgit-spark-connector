package tech.sourced.engine.util

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.sources.Filter
import org.apache.spark.unsafe.types.UTF8String

object Filters {

  /** Match of an expression filter as a tuple with the column name and the matching values. */
  type Match = (String, Seq[Any])

  /**
    * Compiles expressions into filters.
    *
    * @param e expression to compile
    * @return compiled expression
    */
  def compile(e: Expression): CompiledFilter = e match {
    case Equality(attr: AttributeReference, Literal(value, _)) =>
      EqualFilter(new Attr(attr.toAttribute), transformLiteral(value))

    case IsNull(attr: AttributeReference) =>
      EqualFilter(new Attr(attr.toAttribute), null)

    case IsNotNull(attr: AttributeReference) =>
      NotFilter(EqualFilter(new Attr(attr.toAttribute), null))

    case Not(expr) => NotFilter(compile(expr))

    case In(attr: AttributeReference, values)
      if values.forall(_.isInstanceOf[Literal]) =>
      InFilter(
        new Attr(attr.toAttribute),
        values.map({ case Literal(value, _) => transformLiteral(value) })
      )

    case And(l, r) => AndFilter(compile(l), compile(r))

    case Or(l, r) => OrFilter(compile(l), compile(r))

    case _ => UnhandledFilter()
  }

  private def transformLiteral(value: Any): Any = value match {
    case v: UTF8String => v.toString
    case v => v
  }

  def isHandled(filter: Filter): Boolean = {
    import org.apache.spark.sql.sources._
    filter match {

      case _@(_: EqualTo |
              _: EqualNullSafe |
              _: IsNull |
              _: IsNotNull |
              _: Not |
              _: In |
              _: And |
              _: Not) => true
      case _ => false
    }
  }

}

/**
  * Filter compiled from an expression to be used inside the iterators.
  */
sealed trait CompiledFilter {

  /**
    * Returns if the filter matches or not. It's wrapped in an optional because
    * there might be cases where the columns is not present.
    *
    * @param cols map of columns and their values
    * @return match of the filter
    */
  def eval(cols: Map[String, Any]): Option[Boolean]

  /**
    * Returns all the matching cases in a map from column name to matching values.
    *
    * @return matchinh cases
    */
  def matchingCases: Map[String, Seq[Any]] =
    getMatchingCases.groupBy(_._1).mapValues(_.map(_._2))

  /**
    * Returns an array of matching cases.
    *
    * @return matching cases
    */
  protected def getMatchingCases: Array[(String, Any)] = this match {
    case EqualFilter(attr, value) => Array((attr.name, value))
    case InFilter(attr, values) => values.toArray.map(v => (attr.name, v))
    case BinaryFilter(left, right) => left.getMatchingCases ++ right.getMatchingCases
    case _ => Array()
  }

  /**
    * Returns the list of filters in this filter.
    *
    * @return filters
    */
  def filters: Seq[CompiledFilter] = Seq(this)

  /**
    * Returns the list of sources in this filter.
    *
    * @return sources list
    */
  def sources: Seq[String]

}

/**
  * Represents a column with a name and a source.
  *
  * @param name   column name
  * @param source table source
  */
case class Attr(name: String, source: String) {

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
case class InFilter(attr: Attr, vals: Seq[Any]) extends CompiledFilter {

  /** @inheritdoc */
  override def toString: String = s"${attr.name} IN (${vals.mkString(", ")})"

  /** @inheritdoc */
  def eval(cols: Map[String, Any]): Option[Boolean] = cols.get(attr.name).map(vals.contains)

  /** @inheritdoc */
  def sources: Seq[String] = Seq(attr.source)

}

/**
  * Binary expression with a left and right expression.
  *
  * @param left  left expression
  * @param right right expression
  */
class BinaryFilter(val left: CompiledFilter, val right: CompiledFilter) extends CompiledFilter {

  /**
    * Action to be executed with both branches of the expression that will return whether
    * or not the expression matches. Meant to be overridden.
    *
    * @param l left expression
    * @param r right expression
    * @return match result
    */
  def action(l: Option[Boolean], r: Option[Boolean]): Option[Boolean] = None

  /** @inheritdoc*/
  def eval(cols: Map[String, Any]): Option[Boolean] = action(left.eval(cols), right.eval(cols))

  /** @inheritdoc*/
  def sources: Seq[String] = left.sources ++ right.sources

}

object BinaryFilter {

  def unapply(e: BinaryFilter): Option[(CompiledFilter, CompiledFilter)] = Some((e.left, e.right))

}

/**
  * Filter that checks the column value is the given value.
  *
  * @param attr  column attribute
  * @param value value to check
  */
case class EqualFilter(attr: Attr, value: Any) extends CompiledFilter {

  /** @inheritdoc*/
  override def toString: String = s"${attr.name} = '$value'"

  /** @inheritdoc*/
  def eval(cols: Map[String, Any]): Option[Boolean] = cols.get(attr.name).map(_ == value)

  /** @inheritdoc*/
  def sources: Seq[String] = Seq(attr.source)

}

/**
  * Filter that negates the match of the given filter.
  *
  * @param f filter to negate
  */
case class NotFilter(f: CompiledFilter) extends CompiledFilter {

  override def toString: String = s"NOT (${f.toString})"

  def eval(cols: Map[String, Any]): Option[Boolean] = f.eval(cols).map(!_)

  def sources: Seq[String] = f.sources

}

/**
  * Filter that checks both its left and right filter match.
  *
  * @param l left filter
  * @param r right filter
  */
case class AndFilter(l: CompiledFilter, r: CompiledFilter) extends BinaryFilter(l, r) {

  /** @inheritdoc*/
  override def toString: String = s"(${l.toString} AND ${r.toString})"

  /** @inheritdoc*/
  override def action(l: Option[Boolean], r: Option[Boolean]): Option[Boolean] = {
    Seq(l, r) match {
      case seq if seq.flatten.isEmpty => None
      case seq => seq.map({
        case None => false
        case Some(v) => v
      }).reduceLeftOption((lb, rb) => lb && rb)
    }
  }

  /** @inheritdoc */
  override def filters: Seq[CompiledFilter] = Seq(l, r)

}

/**
  * Filter that checks either of its branches matches.
  *
  * @param l left filter
  * @param r right filter
  */
case class OrFilter(l: CompiledFilter, r: CompiledFilter) extends BinaryFilter(l, r) {

  /** @inheritdoc */
  override def toString: String = s"(${l.toString} OR ${r.toString})"

  /** @inheritdoc */
  override def action(l: Option[Boolean], r: Option[Boolean]): Option[Boolean] =
    Seq(l, r).flatten.reduceLeftOption((lb, rb) => lb || rb)

  /** @inheritdoc */
  override def filters: Seq[CompiledFilter] = Seq(l, r)

}

/**
  * Filter that is not handled.
  */
case class UnhandledFilter() extends CompiledFilter {

  /** @inheritdoc */
  override def eval(cols: Map[String, Any]): Option[Boolean] = None

  /** @inheritdoc */
  def sources: Seq[String] = Seq()

}
