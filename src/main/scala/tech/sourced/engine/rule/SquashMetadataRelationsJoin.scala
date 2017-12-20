package tech.sourced.engine.rule

import org.apache.spark.SparkException
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.LogicalRelation
import tech.sourced.engine.{GitRelation, MetadataRelation}

object SquashMetadataRelationsJoin extends Rule[LogicalPlan] {
  /** @inheritdoc */
  def apply(plan: LogicalPlan): LogicalPlan = plan transformUp {
    case q@Join(_, _, _, _) =>
      val jd = MetadataOptimizer.getMetadataJoinData(q)
      if (!jd.valid) {
        return q
      }

      jd match {
        case MetadataJoinData(
        filters,
        joinConditions,
        projectExprs,
        attributes,
        Some(session),
        Some(dbPath),
        _
        ) =>
          val relation = LogicalRelation(
            MetadataRelation(
              session,
              RelationOptimizer.attributesToSchema(attributes),
              dbPath,
              joinConditions
            ),
            attributes,
            None
          )

          val node = RelationOptimizer.joinConditionsToFilters(joinConditions) match {
            case Some(condition) => Filter(condition, relation)
            case None => relation
          }

          val filteredNode = filters match {
            case Some(filter) => Filter(filter, node)
            case None => relation
          }

          // If the projection is empty, just return the filter
          if (projectExprs.nonEmpty) {
            Project(projectExprs, filteredNode)
          } else {
            filteredNode
          }
        case _ => q
      }

    // Remove two consecutive projects and replace it with the outermost one.
    case Project(list, Project(_, child)) =>
      Project(list, child)
  }
}

private[rule] case class MetadataJoinData(filterExpression: Option[Expression] = None,
                            joinCondition: Option[Expression] = None,
                            projectExpressions: Seq[NamedExpression] = Nil,
                            attributes: Seq[AttributeReference] = Nil,
                            session: Option[SparkSession] = None,
                            dbPath: Option[String] = None,
                            valid: Boolean = false)

private[rule] object MetadataOptimizer extends Logging {

  private def canRelationsBeJoined(left: LogicalRelation, right: LogicalRelation): Boolean =
    (left.relation, right.relation) match {
      case (_: MetadataRelation, _: MetadataRelation) => true
      case (_: MetadataRelation, GitRelation(_, _, _, Some("blobs"))) => true
      case (GitRelation(_, _, _, Some("blobs")), _: MetadataRelation) => true
      case _ => false
    }

  private[engine] def getMetadataJoinData(j: Join): MetadataJoinData = {
    // left and right ends in a GitRelation
    val leftRel = getRelation(j.left)
    val rightRel = getRelation(j.right)

    // Not a valid Join to optimize GitRelations
    if (leftRel.isEmpty
      || rightRel.isEmpty
      || !RelationOptimizer.isJoinSupported(j)
      || !canRelationsBeJoined(leftRel.get, rightRel.get)) {
      logWarning("Join cannot be optimized. It doesn't have MetadataRelations in both sides, " +
        "MetadataRelation in one side and blobs GitRelation in the other, " +
        "or the Join type is not supported.")
      return MetadataJoinData()
    }

    // Check Join conditions. They must be all conditions related with GitRelations
    val unsupportedConditions = RelationOptimizer.getUnsupportedConditions(
      j,
      leftRel.get,
      rightRel.get
    )
    if (unsupportedConditions.nonEmpty) {
      logWarning(s"Join cannot be optimized. Obtained unsupported " +
        s"conditions: $unsupportedConditions")
      return MetadataJoinData()
    }

    // Check if the Join contains all valid Nodes
    val jd: Seq[MetadataJoinData] = j.map {
      case jm@Join(_, _, _, condition) =>
        if (jm == j) {
          MetadataJoinData(valid = true, joinCondition = condition)
        } else {
          throw new SparkException(s"Join cannot be optimized. Invalid node: $jm")
        }
      case Filter(cond, _) =>
        MetadataJoinData(Some(cond), valid = true)
      case Project(namedExpressions, _) =>
        MetadataJoinData(None, projectExpressions = namedExpressions, valid = true)
      case LogicalRelation(MetadataRelation(session, _, dbPath, joinCondition, _), out, _) =>
        MetadataJoinData(
          None,
          valid = true,
          joinCondition = joinCondition,
          attributes = out,
          session = Some(session),
          dbPath = Some(dbPath)
        )
      case LogicalRelation(GitRelation(session, _, joinCondition, _), out, _) =>
        MetadataJoinData(
          None,
          valid = true,
          joinCondition = joinCondition,
          attributes = out,
          session = Some(session)
        )
      case other =>
        throw new SparkException(s"Join cannot be optimized. Invalid node: $other")
    }

    mergeMetadataJoinData(jd)
  }

  private def mergeMetadataJoinData(data: Seq[MetadataJoinData]): MetadataJoinData = {
    data.reduce((jd1, jd2) => {
      // get all filter expressions
      val exprOpt: Option[Expression] = RelationOptimizer.mixExpressions(
        jd1.filterExpression,
        jd2.filterExpression,
        And
      )
      // get all join conditions
      val joinConditionOpt: Option[Expression] = RelationOptimizer.mixExpressions(
        jd1.joinCondition,
        jd2.joinCondition,
        And
      )

      def getAny[T](left: Option[T], right: Option[T]) =
        (left, right) match {
          case (l@Some(_), _) => l
          case (_, r@Some(_)) => r
          case _ => None
        }

      MetadataJoinData(
        exprOpt,
        joinConditionOpt,
        jd1.projectExpressions ++ jd2.projectExpressions,
        jd1.attributes ++ jd2.attributes,
        getAny(jd1.session, jd2.session),
        getAny(jd1.dbPath, jd2.dbPath),
        jd1.valid && jd2.valid
      )
    })
  }

  def getRelation(lp: LogicalPlan): Option[LogicalRelation] =
    lp.find {
      case LogicalRelation(_: MetadataRelation, _, _) => true
      case LogicalRelation(GitRelation(_, _, _, Some("blobs")), _, _) => true
      case _ => false
    } map (_.asInstanceOf[LogicalRelation])

}
