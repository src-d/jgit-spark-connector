package tech.sourced.engine

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.plans.{Inner, JoinType}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.types._

/**
  * Rule to assign to an [[AttributeReference]] metadata to identify the table it belongs to.
  */
object AddSourceToAttributes extends Rule[LogicalPlan] {

  /**
    * SOURCE is the key used for attach metadata to [[AttributeReference]]s.
    */
  val SOURCE = "source"

  /** @inheritdoc */
  def apply(plan: LogicalPlan): LogicalPlan = plan transformUp {
    case LogicalRelation(gitRelation@GitRelation(_, _, _, schemaSource), out, catalogTable) =>
      val processedOut = schemaSource match {
        case Some(table) => out.map(
          _.withMetadata(new MetadataBuilder().putString(SOURCE, table).build()
          ).asInstanceOf[AttributeReference]
        )
        case None => out
      }

      LogicalRelation(gitRelation, processedOut, catalogTable)
  }
}

/**
  * Logical plan rule to transform joins of [[GitRelation]]s into a single [[GitRelation]]
  * that will use chainable iterators for better performance. Rather than obtaining all the
  * data from each table in isolation, it will reuse already filtered data from the previous
  * iterator.
  */
object SquashGitRelationJoin extends Rule[LogicalPlan] {
  /** @inheritdoc */
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
              GitOptimizer.attributesToSchema(attributes), joinConditions
            ),
            attributes,
            None
          )

          val node = GitOptimizer.joinConditionsToFilters(joinConditions) match {
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
case class JoinData(filterExpression: Option[Expression] = None,
                    joinCondition: Option[Expression] = None,
                    projectExpressions: Seq[NamedExpression] = Nil,
                    attributes: Seq[AttributeReference] = Nil,
                    session: Option[SparkSession] = None,
                    valid: Boolean = false)

/**
  * Support methods for optimizing [[GitRelation]]s.
  */
object GitOptimizer extends Logging {
  private val supportedJoinTypes: Seq[JoinType] = Inner :: Nil

  /**
    * Reports whether the given join is supported.
    *
    * @param j join
    * @return is supported or not
    */
  private def isJoinSupported(j: Join): Boolean = supportedJoinTypes.contains(j.joinType)

  /**
    * Retrieves all the unsupported conditions in the join.
    *
    * @param join  Join
    * @param left  left relation
    * @param right right relation
    * @return unsupported conditions
    */
  private def getUnsupportedConditions(join: Join,
                                       left: LogicalRelation,
                                       right: LogicalRelation) = {
    val leftReferences = left.references.baseSet
    val rightReferences = right.references.baseSet
    val joinReferences = join.references.baseSet
    joinReferences -- leftReferences -- rightReferences
  }

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
    if (leftRel.isEmpty || rightRel.isEmpty || !isJoinSupported(j)) {
      logWarning("Join cannot be optimized. It doesn't have GitRelations in both sides, " +
        "or the Join type is not supported.")
      return JoinData()
    }

    // Check Join conditions. They must be all conditions related with GitRelations
    val unsupportedConditions = getUnsupportedConditions(j, leftRel.get, rightRel.get)
    if (unsupportedConditions.nonEmpty) {
      logWarning(s"Join cannot be optimized. Obtained unsupported " +
        s"conditions: $unsupportedConditions")
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
      case LogicalRelation(GitRelation(session, _, joinCondition, _), out, _) =>
        JoinData(
          None,
          valid = true,
          joinCondition = joinCondition,
          attributes = out,
          session = Some(session)
        )
      case other =>
        logWarning(s"Join cannot be optimized. Invalid node: $other")
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
      val exprOpt: Option[Expression] = mixExpressions(
        jd1.filterExpression,
        jd2.filterExpression,
        And
      )
      // get all join conditions
      val joinConditionOpt: Option[Expression] = mixExpressions(
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
    * Mixes the two given expressions with the given join function if both exist
    * or returns the one that exists otherwise.
    *
    * @param l            left expression
    * @param r            right expression
    * @param joinFunction function used to join them
    * @return an optional expression
    */
  private def mixExpressions(l: Option[Expression],
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
