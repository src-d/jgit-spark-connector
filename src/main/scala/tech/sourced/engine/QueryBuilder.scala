package tech.sourced.engine

import java.sql.{Date, Timestamp}

import org.apache.commons.lang3.StringUtils
import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.unsafe.types.UTF8String

case class Join(left: String, right: String, conditions: Seq[JoinCondition])

case class JoinCondition(leftTable: String, leftCol: String, rightTable: String, rightCol: String)

/**
  * Immutable select query builder.
  *
  * @param fields  fields to select
  * @param tables  tables to select data from
  * @param joins   joins with other tables, if any
  * @param filters filters to apply
  */
private[engine] case class QueryBuilder(fields: Seq[Attribute] = Array[Attribute](),
                                        tables: Seq[String] = Array[String](),
                                        joins: Seq[Join] = Array[Join](),
                                        filters: Seq[Expression] = Array[Expression]()) {

  import QueryBuilder._

  /**
    * Creates a new QueryBuilder with the given fields added to the current ones.
    *
    * @param fields new fields to add
    * @return a new QueryBuilder with the given fields added to the current fields
    */
  def addFields(fields: Seq[Attribute]): QueryBuilder =
    QueryBuilder(this.fields ++ fields, tables, joins, filters)

  /**
    * Creates a new QueryBuilder with the given table added to the current ones.
    *
    * @param table new table to add
    * @return a new QueryBuilder with the given table added to the current fields
    */
  def addTable(table: String): QueryBuilder =
    QueryBuilder(fields, tables ++ Seq(table), joins, filters)

  def join(left: String, right: String, conditions: Seq[JoinCondition]): QueryBuilder =
    QueryBuilder(
      fields,
      tables,
      joins ++ Some(Join(left, right, conditions)),
      filters
    )

  /**
    * Creates a new QueryBuilder with the given filter added to the current ones.
    *
    * @param filters filters to add
    * @return new QueryBuilder with the filter added to the current ones
    */
  def addFilters(filters: Seq[Expression]): QueryBuilder =
    QueryBuilder(fields, tables, joins, this.filters ++ filters)

  def selectedFields: String =
    if (fields.nonEmpty) {
      fields.map(qualify).mkString(", ")
    } else {
      // when there is no field selected, such as a count of repositories,
      // just get the first field of the first table to avoid returning all
      // the fields
      tables.headOption match {
        case Some(table) =>
          qualify(table, Schema(table).head.name)
        case None =>
          throw new SparkException("unable to build sql query with no tables and no columns")
      }
    }

  def whereClause: String = {
    val compiledFilters = filters.flatMap(compileFilter)
    if (compiledFilters.isEmpty) {
      ""
    } else {
      s"WHERE ${compiledFilters.mkString(" AND ")}"
    }
  }

  def getOnClause(cond: JoinCondition): String =
    s"${qualify(cond.leftTable, cond.leftCol)} = ${qualify(cond.rightTable, cond.rightCol)}"

  def selectedTables: String =
    if (joins.isEmpty && tables.length == 1) {
      prefixTable(tables.head)
    } else if (joins.nonEmpty) {
      joins.zipWithIndex.map {
        case (Join(left, right, conditions), 0) =>
          s"${prefixTable(left)} INNER JOIN ${prefixTable(right)} " +
            s"ON (${conditions.map(getOnClause).mkString(" AND ")})"
        case (Join(_, right, conditions), _) =>
          s"INNER JOIN ${prefixTable(right)} " +
            s"ON (${conditions.map(getOnClause).mkString(" AND ")})"
      }.mkString(" ")
    } else {
      // should not happen
      throw new SparkException("this is likely a bug: no join conditions found, " +
        s"but multiple tables: '${tables.mkString(", ")}'")
    }

  /**
    * Returns the built select SQL query.
    *
    * @return select SQL query
    */
  def sql: String =
    s"SELECT $selectedFields FROM $selectedTables $whereClause"

}

private[engine] object QueryBuilder {

  def qualify(col: Attribute): String = {
    val (table, name) = Schema.metadataTableAndCol(col)
    qualify(table, name)
  }

  def qualify(table: String, col: String): String =
    s"${prefixTable(table)}.`$col`"

  def escapeSql(value: String): String =
    if (value == null) null else StringUtils.replace(value, "'", "''")

  def compileValue(value: Any): Any = value match {
    case v: UTF8String => compileValue(v.toString)
    case v: String => s"'${escapeSql(v)}'"
    case v: Timestamp => "'" + v + "'"
    case v: Date => "'" + v + "'"
    case v: Seq[Any] => v.map(compileValue).mkString(", ")
    case v: Boolean => if (v) 1 else 0
    case _ => value
  }

  /**
    * Compiles a filter expression into a SQL string if the filter can be handled.
    * Slightly modified from the JDBCRDD source code of spark.
    *
    * @param filter filter to compile
    * @return compiled filter
    */
  def compileFilter(filter: Expression): Option[String] = {
    // org.apache.spark.sql.sources._ is imported at the top level and
    // names collision with the expressions, so we import them here.
    import org.apache.spark.sql.catalyst.expressions._
    Option(filter match {
      case EqualTo(attr: AttributeReference, Literal(value, _)) =>
        s"${qualify(attr)} = ${compileValue(value)}"
      case EqualNullSafe(attr: AttributeReference, Literal(value, _)) =>
        val col = qualify(attr)
        s"(NOT ($col != ${compileValue(value)} OR $col IS NULL OR " +
          s"${compileValue(value)} IS NULL) OR " +
          s"($col IS NULL AND ${compileValue(value)} IS NULL))"
      case LessThan(attr: AttributeReference, Literal(value, _)) =>
        s"${qualify(attr)} < ${compileValue(value)}"
      case GreaterThan(attr: AttributeReference, value) =>
        s"${qualify(attr)} > ${compileValue(value)}"
      case LessThanOrEqual(attr: AttributeReference, Literal(value, _)) =>
        s"${qualify(attr)} <= ${compileValue(value)}"
      case GreaterThanOrEqual(attr: AttributeReference, Literal(value, _)) =>
        s"${qualify(attr)} >= ${compileValue(value)}"
      case IsNull(attr: AttributeReference) => s"${qualify(attr)} IS NULL"
      case IsNotNull(attr: AttributeReference) => s"${qualify(attr)} IS NOT NULL"
      case In(attr: AttributeReference, values) if values.isEmpty =>
        s"CASE WHEN ${qualify(attr)} IS NULL THEN NULL ELSE FALSE END"
      case In(attr: AttributeReference, values: Seq[Expression]) =>
        val literals = values.flatMap {
          case Literal(value, _) => Some(value)
          case _ => None
        }

        if (literals.length != values.length) {
          null
        } else {
          s"${qualify(attr)} IN (${compileValue(literals)})"
        }
      case Not(f) => compileFilter(f).map(p => s"(NOT ($p))").orNull
      case Or(f1, f2) =>
        // We can't compile Or filter unless both sub-filters are compiled successfully.
        // It applies too for the following And filter.
        // If we can make sure compileFilter supports all filters, we can remove this check.
        val or = Seq(f1, f2).flatMap(compileFilter)
        if (or.size == 2) {
          val clause = or.map(p => s"($p)").mkString(" OR ")
          s"($clause)"
        } else {
          null
        }
      case And(f1, f2) =>
        val and = Seq(f1, f2).flatMap(compileFilter)
        if (and.size == 2) {
          and.map(p => s"($p)").mkString(" AND ")
        } else {
          null
        }
      case _ => null
    })
  }

  def prefixTable(table: String): String = s"engine_$table"

}
