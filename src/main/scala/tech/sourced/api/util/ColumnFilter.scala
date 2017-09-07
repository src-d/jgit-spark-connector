package tech.sourced.api.util

import org.apache.spark.sql.sources._

object ColumnFilter {
  def compileFilter(f: Filter): CompiledFilter = {
    f match {
      case EqualTo(attr, value) => new EqualFilter(attr, value)
      case EqualNullSafe(attr, value) => new EqualFilter(attr, value)
      case IsNull(attr) => new EqualFilter(attr, null)
      case IsNotNull(attr) => NotFilter(new EqualFilter(attr, null))
      case Not(child) => NotFilter(compileFilter(child))
      case And(l, r) => new AndFilter(compileFilter(l), compileFilter(r))
      case Or(l, r) => new OrFilter(compileFilter(l), compileFilter(r))
      case _ => UnhandledFilter()
    }
  }
}

sealed trait CompiledFilter {
  def eval(cols: Map[String, Any]): Option[Boolean]

  def matchingCases: Map[String, Seq[Any]] =
    this.getMatchingCases.groupBy(_._1).mapValues(_.map(_._2))

  private def getMatchingCases: Array[(String, Any)] =
    this match {
      case ef: EqualFilter => Array((ef.name, ef.value))
      case bf: BinaryFilter => bf.l.getMatchingCases ++ bf.r.getMatchingCases
      case _ => Array()
    }
}

case class UnaryFilter(name: String, value: Any) extends CompiledFilter {
  // Expected to be overridden
  def action(value: Any): Boolean = true

  override def eval(cols: Map[String, Any]): Option[Boolean] = cols.get(name).map(action)
}

case class BinaryFilter(l: CompiledFilter, r: CompiledFilter) extends CompiledFilter {
  // Expected to be overridden
  def action(l: Option[Boolean], r: Option[Boolean]): Option[Boolean] = None

  override def eval(cols: Map[String, Any]): Option[Boolean] = action(l.eval(cols), r.eval(cols))
}

class EqualFilter(name: String, value: Any) extends UnaryFilter(name, value) {
  override def toString: String = s"$name = $value"

  override def action(v: Any): Boolean = v == value
}

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

class OrFilter(l: CompiledFilter, r: CompiledFilter) extends BinaryFilter(l, r) {
  override def toString: String = s"(${l.toString} OR ${r.toString})"

  override def action(l: Option[Boolean], r: Option[Boolean]): Option[Boolean] =
    Seq(l, r).flatten.reduceLeftOption((lb, rb) => lb || rb)
}

case class NotFilter(child: CompiledFilter) extends CompiledFilter {
  override def toString: String = s"NOT ${child.toString}"

  override def eval(cols: Map[String, Any]): Option[Boolean] = child.eval(cols).map(b => !b)
}

case class UnhandledFilter() extends CompiledFilter {
  override def eval(cols: Map[String, Any]): Option[Boolean] = None
}
