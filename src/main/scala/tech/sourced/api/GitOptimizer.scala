package tech.sourced.api

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.expressions.{And, Attribute, AttributeReference, EqualTo, Expression, Literal, NamedExpression, Or}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.plans.{Inner, JoinType}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.types.{BooleanType, StructField, StructType}

class SquashGitRelationJoin extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transformUp {
    // Joins are only applicable per repository, so we can push down completely the Join into the datasource
    case q@Join(_, _, _, _) =>
      val jd = GitOptimizer.canBeOptimized(q)
      if (!jd.valid) {
        return q
      }

      jd match {
        case JoinData(feo, jc, pe, attributes, Some(sqlc), _) =>
          val fe = feo.getOrElse(EqualTo(Literal(false, BooleanType), Literal(true, BooleanType)))
          Project(pe,
            Filter(fe,
              LogicalRelation(
                GitRelation(sqlc, GitOptimizer.attributesToSchema(attributes), jc), attributes, None)))
        case _ => q
      }
  }
}

case class JoinData(
                     filterExpression: Option[Expression] = None,
                     joinCondition: Option[Expression] = None,
                     projectExpressions: Seq[NamedExpression] = Nil,
                     attributes: Seq[AttributeReference] = Nil,
                     sqlContext: Option[SQLContext] = None,
                     valid: Boolean = false)

object GitOptimizer extends Logging {
  private val supportedJoinTypes: Seq[JoinType] = Inner :: Nil

  def canBeOptimized(j: Join): JoinData = {
    // check if the Join type is supported
    val supportedJoin = supportedJoinTypes.contains(j.joinType)

    // left and right ends in a GitRelation
    val leftGitRelOpt = gitRelationOpt(j.left)
    val rightGitRelOpt = gitRelationOpt(j.right)

    // Not a valid Join to optimize GitRelations
    if (leftGitRelOpt.isEmpty || rightGitRelOpt.isEmpty || !supportedJoin) {
      logWarning("Join cannot be optimized. It doesn't have GitRelations in both sides, or the Join type is not supported.")
      return JoinData()
    }

    // Check Join conditions. They must be all conditions related with GitRelations
    val leftReferences = leftGitRelOpt.get.references.baseSet
    val rightReferences = rightGitRelOpt.get.references.baseSet
    val joinReferences = j.references.baseSet

    val unsupportedConditions = joinReferences -- leftReferences -- rightReferences

    if (unsupportedConditions.nonEmpty) {
      logWarning(s"Join cannot be optimized. Obtained unsupported conditions: $unsupportedConditions")
      return JoinData()
    }

    // Check if the Join contains all valid Nodes
    val jd: Seq[JoinData] = j.map {
      case jm@Join(_, _, _, condition) =>
        if (jm == j) {
          JoinData(valid = true, joinCondition = condition)
        } else {
          logWarning(s"Join cannot be optimized. Invalid node: $jm")
          JoinData()
        }
      case Filter(cond, _) =>
        JoinData(Some(cond), valid = true)
      case Project(namedExpressions, _) =>
        JoinData(None, projectExpressions = namedExpressions, valid = true)
      case LogicalRelation(GitRelation(sqlc, schema, joinCondition), out, _) =>
        JoinData(None, valid = true, joinCondition = joinCondition, attributes = out, sqlContext = Some(sqlc))
      case other =>
        logWarning(s"Join cannot be optimized. Invalid node: $other")
        JoinData()
    }

    // Reduce all filter expressions into one
    jd.reduce((jd1, jd2) => {
      // get all filter expressions
      val exprOpt = (jd1.filterExpression, jd2.filterExpression) match {
        case (Some(expr1), Some(expr2)) => Some(Or(expr1, expr2))
        case (None, None) => None
        case (le, None) => le
        case (None, re) => re
      }

      // get all join conditions
      val joinConditionOpt = (jd1.joinCondition, jd2.joinCondition) match {
        case (Some(expr1), Some(expr2)) => Some(And(expr1, expr2))
        case (None, None) => None
        case (le, None) => le
        case (None, re) => re
      }

      // get just one sqlContext if any
      val sqlcOpt = (jd1.sqlContext, jd2.sqlContext) match {
        case (Some(l), _) => Some(l)
        case (_, Some(r)) => Some(r)
        case _ => None
      }

      JoinData(
        exprOpt,
        joinConditionOpt,
        jd1.projectExpressions ++ jd2.projectExpressions,
        jd1.attributes ++ jd2.attributes,
        sqlcOpt,
        jd1.valid && jd2.valid
      )
    })

  }

  def gitRelationOpt(lp: LogicalPlan): Option[LogicalRelation] =
    lp.find {
      case LogicalRelation(GitRelation(_, _, _), _, _) => true
      case _ => false
    } map (_.asInstanceOf[LogicalRelation])

  def attributesToSchema(attributes: Seq[AttributeReference]): StructType =
    StructType(attributes.map((a: Attribute) => StructField(a.name, a.dataType, a.nullable, a.metadata)) toArray)
}
