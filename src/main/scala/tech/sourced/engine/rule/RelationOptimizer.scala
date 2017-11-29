package tech.sourced.engine.rule

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.{Inner, JoinType}
import org.apache.spark.sql.catalyst.plans.logical.Join
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.types.{StructField, StructType}

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
    val orderedNames = Seq(a.name, b.name).sorted
    // TODO: compare with the source of the attribute as well
    // both reference and commit have a "hash" column
    (orderedNames.head, orderedNames(1)) match {
      case ("id", "repository_id") => true
      case ("repository_id", "repository_id") => true
      case ("name", "reference_name") => true
      case ("commit_hash", "hash") => true
      case ("commit_hash", "commit_hash") => true
      case ("blob", "blob_id") => true
      case _ => false
    }
  }

}
