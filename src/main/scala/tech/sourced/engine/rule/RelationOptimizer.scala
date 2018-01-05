package tech.sourced.engine.rule

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.{Inner, JoinType}
import org.apache.spark.sql.catalyst.plans.logical.Join
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.types.{StructField, StructType}
import tech.sourced.engine.Sources

private[rule] object RelationOptimizer extends Logging {
  private val supportedJoinTypes: Seq[JoinType] = Inner :: Nil

  /**
    * Reports whether the given join is supported.
    *
    * @param j join
    * @return is supported or not
    */
  def isJoinSupported(j: Join): Boolean = supportedJoinTypes.contains(j.joinType)

  /**
    * Retrieves all the unsupported conditions in the join.
    *
    * @param join  Join
    * @param left  left relation
    * @param right right relation
    * @return unsupported conditions
    */
  def getUnsupportedConditions(join: Join,
                               left: LogicalRelation,
                               right: LogicalRelation): Set[_] = {
    val leftReferences = left.references.baseSet
    val rightReferences = right.references.baseSet
    val joinReferences = join.references.baseSet
    joinReferences -- leftReferences -- rightReferences
  }

  /**
    * Mixes the two given expressions with the given join function if both exist
    * or returns the one that exists otherwise.
    *
    * @param l            left expression
    * @param r            right expression
    * @param joinFunction function used to join them
    * @return an optional expression
    */
  def mixExpressions(l: Option[Expression],
                     r: Option[Expression],
                     joinFunction: (Expression, Expression) => Expression):
  Option[Expression] = {
    (l, r) match {
      case (Some(expr1), Some(expr2)) => Some(joinFunction(expr1, expr2))
      case (None, None) => None
      case (le, None) => le
      case (None, re) => re
    }
  }

  /**
    * Creates a schema from a list of attributes.
    *
    * @param attributes list of attributes
    * @return resultant schema
    */
  def attributesToSchema(attributes: Seq[AttributeReference]): StructType =
    StructType(
      attributes
        .map((a: Attribute) => StructField(a.name, a.dataType, a.nullable, a.metadata))
        .toArray
    )

  /**
    * Takes the join conditions, if any, and transforms them to filters, by removing some filters
    * that don't make sense because they are already done inside the iterator.
    *
    * @param expr optional condition to transform
    * @return transformed join conditions or none
    */
  def joinConditionsToFilters(expr: Option[Expression]): Option[Expression] = expr match {
    case Some(e) =>
      e transformUp {
        case Equality(
        a: AttributeReference,
        b: AttributeReference
        ) if isRedundantAttributeFilter(a, b) =>
          EqualTo(Literal(1), Literal(1))

        case BinaryOperator(a, Equality(IntegerLiteral(1), IntegerLiteral(1))) =>
          a

        case BinaryOperator(Equality(IntegerLiteral(1), IntegerLiteral(1)), b) =>
          b
      } match {
        case Equality(IntegerLiteral(1), IntegerLiteral(1)) =>
          None
        case finalExpr =>
          Some(finalExpr)
      }
    case None => None
  }

  /**
    * Returns whether the equality between the two given attribute references is redundant
    * for a filter (because they are taken care of inside the iterators).
    *
    * @param a left attribute
    * @param b right attribute
    * @return is redundant or not
    */
  def isRedundantAttributeFilter(a: AttributeReference, b: AttributeReference): Boolean = {
    // to avoid case (a, b) and case (b, a) we take left and right sorted by name and source
    val (left, right) = a.name.compareTo(b.name) match {
      case 0 =>
        val sourceA = attributeSource(a).getOrElse("")
        val sourceB = attributeSource(b).getOrElse("")
        if (sourceA.compareTo(sourceB) <= 0) (a, b) else (b, a)
      case n if n < 0 => (a, b)
      case _ => (b, a)
    }

    (attributeQualifiedName(left), attributeQualifiedName(right)) match {
      case (("repositories", "id"), ("references", "repository_id")) => true
      case (("references", "name"), ("commits", "reference_name")) => true
      case (("tree_entries", "commit_hash"), ("commits", "hash")) => true
      case (("tree_entries", "blob"), ("blobs", "blob_id")) => true
      // source does not matter in these cases
      case ((_, "repository_id"), (_, "repository_id")) => true
      case ((_, "reference_name"), (_, "reference_name")) => true
      case ((_, "commit_hash"), (_, "commit_hash")) => true
      case _ => false
    }
  }

  def attributeSource(a: AttributeReference): Option[String] =
    if (a.metadata.contains(Sources.SourceKey)) {
      Some(a.metadata.getString(Sources.SourceKey))
    } else {
      None
    }

  def attributeQualifiedName(a: AttributeReference): (String, String) =
    (attributeSource(a).getOrElse(""), a.name)

}
