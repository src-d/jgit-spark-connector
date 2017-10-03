package tech.sourced.api.util

import org.apache.spark.sql.sources._

/**
  * Companion object containing some methods to operate on [[org.apache.spark.sql.sources.Filter]]s.
  */
object ColumnFilter {

  /**
    * Compiles the given [[org.apache.spark.sql.sources.Filter]] into a custom filter.
    *
    * @param f filter to compile
    * @return compiled filter
    */
  def compileFilter(f: Filter): CompiledFilter = f match {
    case EqualTo(attr, value) => new EqualFilter(attr, value)
    case EqualNullSafe(attr, value) => new EqualFilter(attr, value)
    case IsNull(attr) => new EqualFilter(attr, null)
    case IsNotNull(attr) => NotFilter(new EqualFilter(attr, null))
    case Not(child) => NotFilter(compileFilter(child))
    case And(l, r) => new AndFilter(compileFilter(l), compileFilter(r))
    case Or(l, r) => new OrFilter(compileFilter(l), compileFilter(r))
    case In(attr, values) => new InFilter(attr, values)
    case _ => UnhandledFilter()
  }

}

/**
  * Custom filter compiled from a [[org.apache.spark.sql.sources.Filter]].
  */
sealed trait CompiledFilter {
  /**
    * Given a row (being row a map of column names to values of any type) returns whether the
    * filter is truthy or not.
    *
    * @param row Row with columns mapping to their values.
    * @return
    */
  def eval(row: Map[String, Any]): Option[Boolean]

  /**
    * Groups all the matching cases of this filter by the filter name, returning
    * a map of filter names to a sequence of values.
    *
    * @return Map from filter names to all its values.
    */
  def matchingCases: Map[String, Seq[Any]] =
    this.getMatchingCases.groupBy(_._1).mapValues(_.map(_._2))

  /**
    * Returns an array of tuples with the name of the filter and its value.
    *
    * @return Array of tuples with the filter and its value.
    */
  private def getMatchingCases: Array[(String, Any)] =
    this match {
      case f: EqualFilter => Array((f.name, f.value))
      case f: BinaryFilter => f.l.getMatchingCases ++ f.r.getMatchingCases
      case f: InFilter => f.value.asInstanceOf[Array[Any]].map((f.name, _))
      case _ => Array()
    }
}

/**
  * Unary operation filter.
  *
  * @param name  name of the column
  * @param value value to check against the column value
  */
case class UnaryFilter(name: String, value: Any) extends CompiledFilter {
  /**
    * Evaluates a the filter and returns a boolean value with whether it matches
    * or not.
    * It's meant to be overridden.
    *
    * @param value value of the column
    * @return whether the value matches the filter or not
    */
  def action(value: Any): Boolean = true

  /**
    * Given a row (a map from columns to values) returns whether the value of the
    * filter column matches or not.
    *
    * @param row Row to evaluate
    * @return `None` if the column does not exist, `Some(boolean)` otherwise.
    */
  override def eval(row: Map[String, Any]): Option[Boolean] = row.get(name).map(action)
}

/**
  * Binary operation filter.
  *
  * @param l Left operation
  * @param r Right operation
  */
case class BinaryFilter(l: CompiledFilter, r: CompiledFilter) extends CompiledFilter {
  /**
    * Evaluates a the filter and returns a boolean value with whether it matches
    * or not.
    * It's meant to be overridden.
    *
    * @param l match of the left operation
    * @param r match of the right operation
    * @return whether the value matches the filter or not, may be None
    */
  def action(l: Option[Boolean], r: Option[Boolean]): Option[Boolean] = None

  /**
    * Given a row (a map from columns to values) returns whether the value of the
    * filter left and right expression match or not.
    *
    * @param row Row to evaluate
    * @return `None` if any of the columns required by the left or right expression
    *         do not exist, `Some(boolean)` otherwise.
    */
  override def eval(row: Map[String, Any]): Option[Boolean] = action(l.eval(row), r.eval(row))
}

/**
  * Checks the column value equals the given value.
  *
  * @param name  name of the column
  * @param value value the column value must be equal to
  */
class EqualFilter(name: String, value: Any) extends UnaryFilter(name, value) {
  override def toString: String = s"$name = $value"

  override def action(v: Any): Boolean = v == value
}

/**
  * Checks that a column values is in the given set of values.
  *
  * @param name   name of the column to check
  * @param values the value of the column must be contained in this array of values
  */
class InFilter(name: String, values: Array[Any]) extends UnaryFilter(name, values) {
  override def toString: String = s"$name IN (${values.mkString(",")})"

  override def action(v: Any): Boolean = values.contains(v)
}

/**
  * Checks both the left and the right operation match.
  *
  * @param l Left operation
  * @param r Right operation
  */
class AndFilter(l: CompiledFilter, r: CompiledFilter) extends BinaryFilter(l, r) {
  override def toString: String = s"(${l.toString} AND ${r.toString})"

  override def action(l: Option[Boolean], r: Option[Boolean]): Option[Boolean] = {
    Seq(l, r) match {
      case seq if seq.flatten.isEmpty => None
      case seq => seq.map({
        case None => false
        case Some(v) => v
      }).reduceLeftOption((lb, rb) => lb && rb)
    }
  }
}

/**
  * Checks either the left or the right operation match.
  *
  * @param l Left operation
  * @param r Right operation
  */
class OrFilter(l: CompiledFilter, r: CompiledFilter) extends BinaryFilter(l, r) {
  override def toString: String = s"(${l.toString} OR ${r.toString})"

  override def action(l: Option[Boolean], r: Option[Boolean]): Option[Boolean] =
    Seq(l, r).flatten.reduceLeftOption((lb, rb) => lb || rb)
}

/**
  * Negates the condition of the passed filter.
  *
  * @param filter filter whose result will be negated
  */
case class NotFilter(filter: CompiledFilter) extends CompiledFilter {
  override def toString: String = s"NOT ${filter.toString}"

  override def eval(cols: Map[String, Any]): Option[Boolean] = filter.eval(cols).map(b => !b)
}

/**
  * Any other filter will not be handled because for now we don't need it.
  */
case class UnhandledFilter() extends CompiledFilter {
  override def eval(cols: Map[String, Any]): Option[Boolean] = None
}
