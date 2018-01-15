package tech.sourced.engine.rule

import org.apache.spark.SparkException
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.LogicalRelation
import tech.sourced.engine.GitRelation

/**
  * Logical plan rule to transform joins of [[GitRelation]]s into a single [[GitRelation]]
  * that will use chainable iterators for better performance. Rather than obtaining all the
  * data from each table in isolation, it will reuse already filtered data from the previous
  * iterator.
  */
object SquashGitRelationsJoin extends Rule[LogicalPlan] {
  /** @inheritdoc*/
  def apply(plan: LogicalPlan): LogicalPlan = plan transformUp {
    // Joins are only applicable per repository, so we can push down completely
    // the join into the data source
    case q@Join(_, _, _, _) =>
      val jd = GitOptimizer.getJoinData(q)
      if (!jd.valid) {
        return q
      }

      jd match {
        case JoinData(filters, joinConditions, projectExprs, attributes, Some(session), _) =>
          val relation = LogicalRelation(
            GitRelation(
              session,
              RelationOptimizer.attributesToSchema(attributes),
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
            case None => node
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

/**
  * Contains all the data gathered from a join node in the logical plan.
  *
  * @param filterExpression   any expression filters mixed with ANDs below the join
  * @param joinCondition      all the join conditions mixed with ANDs
  * @param projectExpressions expressions for the projection
  * @param attributes         list of attributes
  * @param session            SparkSession
  * @param valid              if the data is valid or not
  */
private[rule] case class JoinData(filterExpression: Option[Expression] = None,
                                  joinCondition: Option[Expression] = None,
                                  projectExpressions: Seq[NamedExpression] = Nil,
                                  attributes: Seq[AttributeReference] = Nil,
                                  session: Option[SparkSession] = None,
                                  valid: Boolean = false)

/**
  * Support methods for optimizing [[GitRelation]]s.
  */
private[rule] object GitOptimizer extends Logging {

  /**
    * Returns the data about a join to perform optimizations on it.
    *
    * @param j join to get the data from
    * @return join data
    */
  private[engine] def getJoinData(j: Join): JoinData = {
    // left and right ends in a GitRelation
    val leftRel = getGitRelation(j.left)
    val rightRel = getGitRelation(j.right)

    // Not a valid Join to optimize GitRelations
    if (leftRel.isEmpty || rightRel.isEmpty || !RelationOptimizer.isJoinSupported(j)) {
      logUnableToOptimize("It doesn't have GitRelations in both sides, " +
        "or the Join type is not supported.")
      return JoinData()
    }

    // Check Join conditions. They must be all conditions related with GitRelations
    val unsupportedConditions = RelationOptimizer.getUnsupportedConditions(
      j,
      leftRel.get,
      rightRel.get
    )
    if (unsupportedConditions.nonEmpty) {
      logUnableToOptimize(s"Obtained unsupported conditions: $unsupportedConditions")
      return JoinData()
    }

    // Check if the Join contains all valid Nodes
    val jd: Seq[JoinData] = j.map {
      case jm@Join(_, _, _, condition) =>
        if (jm == j) {
          JoinData(valid = true, joinCondition = condition)
        } else {
          logUnableToOptimize(s"Invalid node: $jm")
          JoinData()
        }
      case Filter(cond, _) =>
        JoinData(Some(cond), valid = true)
      case Project(namedExpressions, _) =>
        JoinData(None, projectExpressions = namedExpressions, valid = true)
      case LogicalRelation(GitRelation(session, _, joinCondition, _), out, _) =>
        JoinData(
          None,
          valid = true,
          joinCondition = joinCondition,
          attributes = out,
          session = Some(session)
        )
      case other =>
        logUnableToOptimize(s"Invalid node: $other")
        JoinData()
    }

    mergeJoinData(jd)
  }

  /**
    * Reduce all join data into one single join data.
    *
    * @param data sequence of join data to be merged
    * @return merged join data
    */
  private def mergeJoinData(data: Seq[JoinData]): JoinData = {
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

      // get just one SparkSession if any
      val sessionOpt = (jd1.session, jd2.session) match {
        case (Some(l), _) => Some(l)
        case (_, Some(r)) => Some(r)
        case _ => None
      }

      JoinData(
        exprOpt,
        joinConditionOpt,
        jd1.projectExpressions ++ jd2.projectExpressions,
        jd1.attributes ++ jd2.attributes,
        sessionOpt,
        jd1.valid && jd2.valid
      )
    })
  }

  /**
    * Returns the first git relation found in the given logical plan, if any.
    *
    * @param lp logical plan
    * @return git relation, or none if there is no such relation
    */
  def getGitRelation(lp: LogicalPlan): Option[LogicalRelation] =
    lp.find {
      case LogicalRelation(GitRelation(_, _, _, _), _, _) => true
      case _ => false
    } map (_.asInstanceOf[LogicalRelation])

  private def logUnableToOptimize(msg: String = ""): Unit = {
    logError("*" * 80)
    logError("* This Join could not be optimized. This might severely impact the performance *")
    logError("* of your query. This happened because there is an unexpected node between the *")
    logError("* two relations of a Join, such as Limit or another kind of unknown relation.  *")
    logError("* Note that this will not stop your query or make it fail, only make it slow.  *")
    logError("*" * 80)
    if (msg.nonEmpty) {
      def split(str: String): Seq[String] = {
        if (str.length > 76) {
          Seq(str.substring(0, 76)) ++ split(str.substring(76))
        } else {
          Seq(str)
        }
      }
      logError(s"* Reason:${" " * 70}*")
      msg.lines.flatMap(split)
        .map(line => s"* $line${" " * (76 - line.length)} *")
        .foreach(logError(_))
      logError("*" * 80)
    }
  }

}
