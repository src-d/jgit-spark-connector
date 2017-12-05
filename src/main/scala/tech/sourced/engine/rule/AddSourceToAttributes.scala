package tech.sourced.engine.rule

import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.types.MetadataBuilder
import tech.sourced.engine.{GitRelation, MetadataRelation, Sources}

/**
  * Rule to assign to an [[AttributeReference]] metadata to identify the table it belongs to.
  */
object AddSourceToAttributes extends Rule[LogicalPlan] {

  /**
    * SOURCE is the key used for attach metadata to [[AttributeReference]]s.
    */
  private val SOURCE = Sources.SourceKey

  /** @inheritdoc */
  def apply(plan: LogicalPlan): LogicalPlan = plan transformUp {
    case LogicalRelation(rel@GitRelation(_, _, _, schemaSource), out, catalogTable) =>
      withMetadata(rel, schemaSource, out, catalogTable)

    case LogicalRelation(rel@MetadataRelation(_, _, _, _, schemaSource), out, catalogTable) =>
      withMetadata(rel, schemaSource, out, catalogTable)
  }

  private def withMetadata(relation: BaseRelation,
                           schemaSource: Option[String],
                           out: Seq[AttributeReference],
                           catalogTable: Option[CatalogTable]): LogicalRelation = {
    val processedOut = schemaSource match {
      case Some(table) => out.map(
        _.withMetadata(new MetadataBuilder().putString(SOURCE, table).build()
        ).asInstanceOf[AttributeReference]
      )
      case None => out
    }

    LogicalRelation(relation, processedOut, catalogTable)
  }

}
