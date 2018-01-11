package tech.sourced.engine

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.types.StructType
import tech.sourced.engine.util.{CompiledFilter, Filter}

/**
  * Defines the hierarchy between data sources.
  */
object Sources {

  val SourceKey: String = "source"

  /** Sources ordered by their position in the hierarchy. */
  val orderedSources = Array(
    "repositories",
    "references",
    "commits",
    "tree_entries",
    "blobs"
  )

  /**
    * Compares two sources.
    *
    * @param a first source
    * @param b second source
    * @return comparison result
    */
  def compare(a: String, b: String): Int = orderedSources.indexOf(a)
    .compareTo(orderedSources.indexOf(b))

  /**
    * Returns the list of sources in the schema or the table source if any.
    *
    * @param tableSource optional source table
    * @param schema      resultant schema
    * @return sequence with table sources
    */
  def getSources(tableSource: Option[String],
                 schema: StructType): Seq[String] =
    tableSource match {
      case Some(ts) => Seq(ts)
      case None =>
        schema
          .map(_.metadata.getString(SourceKey))
          .distinct
          .sortWith(Sources.compare(_, _) < 0)
    }

  def getFiltersBySource(filters: Seq[Expression]): Map[String, Seq[CompiledFilter]] =
    filters.flatMap(Filter.compile)
      .map(e => (e.sources.distinct, e))
      .filter(_._1.lengthCompare(1) == 0)
      .groupBy(_._1)
      .map { case (k, v) => (k.head, v.map(_._2)) }

}
