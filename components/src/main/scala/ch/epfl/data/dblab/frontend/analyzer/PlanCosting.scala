package ch.epfl.data
package dblab
package frontend
package analyzer

import schema._
import parser.OperatorAST._
import ch.epfl.data.dblab.frontend.parser.SQLAST._

import scala.math._

class PlanCosting(schema: Schema, queryPlan: QueryPlanTree) {
  val tau = 0.2
  val lambda = 2
  val blockSize = 1000

  val tableFilters = new scala.collection.mutable.HashMap[String, List[Expression]]()
  val generalFilters = new scala.collection.mutable.ArrayBuffer[Expression]()
  val tableJoins = new scala.collection.mutable.HashMap[String, List[Expression]]()
  val aggFilter = new scala.collection.mutable.ArrayBuffer[Expression]()
  getExpressions()
  val filterExprs: scala.collection.mutable.HashMap[String, Expression] = tableFilters.map(t => t._1 -> t._2.tail.foldLeft(t._2.head)((a, b) => And(a, b)))

  val costMap = new scala.collection.mutable.HashMap[OperatorNode, Double]
  val sizeMap = new scala.collection.mutable.HashMap[OperatorNode, Double]
  val fullSize = sizing(queryPlan.rootNode)
  val fullCost = costing(queryPlan.rootNode)

  val tables: Set[String] = removeSubquery(queryPlan.rootNode).toList().collect { case s: ScanOpNode => s }.map(_.table.name).toSet
  System.out.println("Tables at beginning are: " + tables)
  val views: Set[String] = queryPlan.views.map(_.name).toSet

  def size(node: OperatorNode = queryPlan.rootNode): Double = sizeMap.getOrElse(node, sizing(node))
  def size(plan: QueryPlanTree): Double = sizeMap.getOrElse(plan.rootNode, sizing(plan.rootNode))
  def cost(node: OperatorNode): Double = costMap.getOrElse(node, costing(node))
  def cost(plan: QueryPlanTree = queryPlan): Double = cost(queryPlan.rootNode) + queryPlan.views.map(v => cost(v)).sum

  def costing(node: OperatorNode = queryPlan.rootNode): Double = {
    node match {
      case ScanOpNode(_, _, _) =>
        val result = size(node)
        costMap.put(node, result)
        result
      case SelectOpNode(parent, _, _) =>
        val result = tau * size(parent) + cost(parent)
        costMap.put(node, result)
        result
      case UnionAllOpNode(top, bottom) =>
        val result = cost(top) + cost(bottom) + size(top) * size(bottom)
        costMap.put(node, result)
        result
      case JoinOpNode(left, right, _, joinType, _, _) =>
        joinType match {
          case IndexNestedLoopJoin | NestedLoopJoin =>
            val result = cost(left) + lambda * size(left) * max(size(node) / size(left), 1)
            costMap.put(node, result)
            result
          case HashJoin =>
            val result = size(node) + cost(left) + cost(right)
            costMap.put(node, result)
            result
          case _ =>
            val result = size(left) * size(right)
            costMap.put(node, result)
            result
        }
      case AggOpNode(parent, _, _, _) =>
        val result = size(parent) + cost(parent)
        costMap.put(node, result)
        result
      case MapOpNode(parent, _) =>
        val result = size(parent) + cost(parent)
        costMap.put(node, result)
        result
      case OrderByNode(parent, _) => {
        val parent_size = size(parent)
        val result = parent_size * log10(parent_size) / log10(2) + cost(parent)
        costMap.put(node, result)
        result
      }
      case PrintOpNode(parent, _, _) =>
        val result = size(parent) + cost(parent)
        costMap.put(node, result)
        result
      case SubqueryNode(parent) =>
        val result = costing(parent)
        costMap.put(node, result)
        result
      case SubquerySingleResultNode(parent) =>
        val result = costing(parent)
        costMap.put(node, result)
        result
      case ProjectOpNode(parent, _, _) =>
        val result = size(parent) + costing(parent)
        costMap.put(node, result)
        result
      case ViewOpNode(parent, _, _) =>
        val result = costing(parent)
        costMap.put(node, result)
        result
    }
  }

  def sizing(node: OperatorNode): Double = node match {
    case ScanOpNode(table, _, _) =>
      val cardinality = schema.stats.getCardinality(table.name)
      sizeMap.put(node, cardinality)
      cardinality
    case SelectOpNode(parent, e, _) =>
      val cardinality = schema.stats.getFilterSelectivity(e) * size(parent)
      sizeMap.put(node, cardinality)
      cardinality
    case UnionAllOpNode(top, bottom) =>
      val cardinality = sizing(top) * sizing(bottom)
      sizeMap.put(node, cardinality)
      cardinality
    case JoinOpNode(left, right, condition, joinType, _, _) =>
      if (condition.equals(Equals(IntLiteral(1), IntLiteral(1)))) {
        val cardinality = size(left) * size(right)
        sizeMap.put(node, cardinality)
        cardinality
      } else {
        val cardinality = joinType match {
          case AntiJoin                             => sizing(left)
          case HashJoin                             => max(sizing(left), sizing(right))
          case InnerJoin | NaturalJoin              => sizing(left) * sizing(right)
          case LeftSemiJoin                         => sizing(left)
          case LeftOuterJoin                        => sizing(left)
          case RightOuterJoin                       => sizing(right)
          case FullOuterJoin                        => sizing(left) * sizing(right)
          case NestedLoopJoin | IndexNestedLoopJoin => sizing(left)
        }
        sizeMap.put(node, cardinality)
        cardinality
      }
    case AggOpNode(parent, aggs, gb, aggNames) =>
      val cardinality = if (gb.isEmpty) 1.0 else sizing(parent)
      sizeMap.put(node, cardinality)
      cardinality
    case MapOpNode(parent, _) =>
      val cardinality = sizing(parent)
      sizeMap.put(node, cardinality)
      cardinality
    case OrderByNode(parent, _) =>
      val cardinality = sizing(parent)
      sizeMap.put(node, cardinality)
      cardinality
    case PrintOpNode(parent, _, _) =>
      val cardinality = sizing(parent)
      sizeMap.put(node, cardinality)
      cardinality
    case SubqueryNode(parent) =>
      val cardinality = sizing(parent)
      sizeMap.put(node, cardinality)
      cardinality
    case SubquerySingleResultNode(parent) =>
      val cardinality = sizing(parent)
      sizeMap.put(node, cardinality)
      cardinality
    case ProjectOpNode(parent, _, _) =>
      val cardinality = sizing(parent)
      sizeMap.put(node, cardinality)
      cardinality
    case ViewOpNode(parent, _, _) =>
      val cardinality = sizing(parent)
      sizeMap.put(node, cardinality)
      cardinality
  }

  def registerExpression(e: Expression, fi: FieldIdent, map: scala.collection.mutable.HashMap[String, List[Expression]]): Unit = {
    val tableName = fi.qualifier match {
      case Some(v) => v
      case None    => fi.name
    }
    map.put(tableName, map.get(tableName) match {
      case Some(v) => if (v.contains(e)) v else e :: v
      case None    => List(e)
    })
  }

  def sameTables(fi1: FieldIdent, fi2: FieldIdent): Boolean = {
    val tableName1 = fi1.qualifier match {
      case Some(v) => v
      case None    => fi1.name
    }
    val tableName2 = fi2.qualifier match {
      case Some(v) => v
      case None    => fi2.name
    }
    tableName1.equals(tableName2)
  }

  def getFieldIdents(e: Expression, res: List[FieldIdent] = Nil): List[FieldIdent] = e match {
    case e1: FieldIdent            => e1 :: res
    case Or(e1, e2)                => getFieldIdents(e1, res) ::: getFieldIdents(e2)
    case And(e1, e2)               => getFieldIdents(e1, res) ::: getFieldIdents(e2)
    case Equals(e1, e2)            => getFieldIdents(e1, res) ::: getFieldIdents(e2)
    case NotEquals(e1, e2)         => getFieldIdents(e1, res) ::: getFieldIdents(e2)
    case LessOrEqual(e1, e2)       => getFieldIdents(e1, res) ::: getFieldIdents(e2)
    case LessThan(e1, e2)          => getFieldIdents(e1, res) ::: getFieldIdents(e2)
    case GreaterOrEqual(e1, e2)    => getFieldIdents(e1, res) ::: getFieldIdents(e2)
    case GreaterThan(e1, e2)       => getFieldIdents(e1, res) ::: getFieldIdents(e2)
    case Like(e1, e2)              => getFieldIdents(e1, res) ::: getFieldIdents(e2)
    case Add(e1, e2)               => getFieldIdents(e1, res) ::: getFieldIdents(e2)
    case Subtract(e1, e2)          => getFieldIdents(e1, res) ::: getFieldIdents(e2)
    case Multiply(e1, e2)          => getFieldIdents(e1, res) ::: getFieldIdents(e2)
    case Divide(e1, e2)            => getFieldIdents(e1, res) ::: getFieldIdents(e2)
    case StringConcat(e1, e2)      => getFieldIdents(e1, res) ::: getFieldIdents(e2)
    case CountExpr(e1)             => getFieldIdents(e1, res)
    case Sum(e1)                   => getFieldIdents(e1, res)
    case Avg(e1)                   => getFieldIdents(e1, res)
    case Min(e1)                   => getFieldIdents(e1, res)
    case Max(e1)                   => getFieldIdents(e1, res)
    case Year(e1)                  => getFieldIdents(e1, res)
    case Upper(e1)                 => getFieldIdents(e1, res)
    case Distinct(e1)              => getFieldIdents(e1, res)
    case AllExp(e1)                => getFieldIdents(e1, res)
    case SomeExp(e1)               => getFieldIdents(e1, res)
    case Not(e1)                   => getFieldIdents(e1, res)
    case Abs(e1)                   => getFieldIdents(e1, res)
    case UnaryPlus(e1)             => getFieldIdents(e1, res)
    case UnaryMinus(e1)            => getFieldIdents(e1, res)
    case Exists(e1)                => getFieldIdents(e1, res)
    case Case(e1, e2, e3)          => getFieldIdents(e1, res) ::: getFieldIdents(e2) ::: getFieldIdents(e3)
    case In(e1, l2)                => getFieldIdents(e1, res)
    case InList(e1, l2)            => getFieldIdents(e1, res)
    case FunctionExp(name, inputs) => res
    case Substring(e1, e2, e3)     => getFieldIdents(e1, res) ::: getFieldIdents(e2) ::: getFieldIdents(e3)
    case _                         => res
  }

  def decideExpression(e: Expression, fi1: FieldIdent, fi2: FieldIdent): Unit = {
    if (sameTables(fi1, fi2)) {
      registerExpression(e, fi1, tableFilters)
      registerExpression(e, fi2, tableFilters)
    } else {
      val tableName1 = fi1.qualifier.getOrElse(fi1.name)
      val tableName2 = fi2.qualifier.getOrElse(fi2.name)
      val allTables = removeSubquery(queryPlan.rootNode).toList().collect { case s: ScanOpNode => s }.map(_.table.name).toSet
      if (!Set(tableName1, tableName2).subsetOf(allTables)) {
        aggFilter.append(e)
      } else {
        registerExpression(e, fi1, tableJoins)
        registerExpression(e, fi2, tableJoins)
      }
    }
  }

  def parseExpression(e: Expression) {
    e match {
      case And(e1, e2) => {
        parseExpression(e1)
        parseExpression(e2)
      }
      case Equals(fi1: FieldIdent, fi2: FieldIdent) => decideExpression(e, fi1, fi2)
      case Equals(fi: FieldIdent, lit: LiteralExpression) =>
        registerExpression(e, fi, tableFilters)
      case NotEquals(fi: FieldIdent, lit: LiteralExpression)      => registerExpression(e, fi, tableFilters)
      case NotEquals(fi1: FieldIdent, fi2: FieldIdent)            => decideExpression(e, fi1, fi2)
      case GreaterThan(fi: FieldIdent, lit: LiteralExpression)    => registerExpression(e, fi, tableFilters)
      case GreaterThan(fi1: FieldIdent, fi2: FieldIdent)          => decideExpression(e, fi1, fi2)
      case GreaterOrEqual(fi: FieldIdent, lit: LiteralExpression) => registerExpression(e, fi, tableFilters)
      case GreaterOrEqual(fi1: FieldIdent, fi2: FieldIdent)       => decideExpression(e, fi1, fi2)
      case LessThan(fi: FieldIdent, lit: LiteralExpression)       => registerExpression(e, fi, tableFilters)
      case LessThan(fi1: FieldIdent, fi2: FieldIdent)             => decideExpression(e, fi1, fi2)
      case LessOrEqual(fi: FieldIdent, lit: LiteralExpression)    => registerExpression(e, fi, tableFilters)
      case LessOrEqual(fi1: FieldIdent, fi2: FieldIdent)          => decideExpression(e, fi1, fi2)
      case Like(fi: FieldIdent, lit: LiteralExpression)           => registerExpression(e, fi, tableFilters)
      case In(fi: FieldIdent, _)                                  => //registerScanOpCond(fi, cond)
      case a => {
        val fields = getFieldIdents(e)
        if (fields.size == 1) {
          registerExpression(e, fields.head, tableFilters)
        } else {
          generalFilters.append(a)
        }
      }

    }
  }

  def getExpressions(node: OperatorNode = queryPlan.rootNode) {
    node match {
      case SelectOpNode(parent, expression, _) => {
        parseExpression(expression)
        getExpressions(parent)
      }
      case UnionAllOpNode(top, bottom) => {
        getExpressions(top)
        getExpressions(bottom)
      }
      case JoinOpNode(left, right, clause, _, _, _) => {
        parseExpression(clause)
        getExpressions(left)
        getExpressions(right)
      }
      case AggOpNode(parent, _, _, _) => getExpressions(parent)
      case MapOpNode(parent, _) => getExpressions(parent)
      case OrderByNode(parent, _) => getExpressions(parent)
      case PrintOpNode(parent, _, _) => getExpressions(parent)
      case ProjectOpNode(parent, _, _) => getExpressions(parent)
      case ViewOpNode(parent, _, _) => getExpressions(parent)
      case SubqueryNode(_) | SubquerySingleResultNode(_) | ScanOpNode(_, _, _) =>
    }
  }

  def getAggregation(node: OperatorNode = queryPlan.rootNode): Option[OperatorNode] = removeSubquery(node).toList.collectFirst { case a: AggOpNode => a }
  def getOrder(node: OperatorNode = queryPlan.rootNode): Option[OperatorNode] = removeSubquery(node).toList.collectFirst { case o: OrderByNode => o }
  def getMap(node: OperatorNode = queryPlan.rootNode): Option[OperatorNode] = removeSubquery(node).toList.collectFirst { case m: MapOpNode => m }
  def getProjection(node: OperatorNode = queryPlan.rootNode): Option[OperatorNode] = removeSubquery(node).toList.collectFirst { case p: ProjectOpNode => p }
  def getPrint(node: OperatorNode = queryPlan.rootNode): Option[OperatorNode] = removeSubquery(node).toList.collectFirst { case p: PrintOpNode => p }

  def getSubquery(node: OperatorNode = queryPlan.rootNode): Option[OperatorNode] = node match {
    case SelectOpNode(parent, _, _) => getSubquery(parent)
    case ScanOpNode(_, _, _)        => None
    case UnionAllOpNode(top, bottom) => {
      getSubquery(top) match {
        case None => getSubquery(bottom) match {
          case Some(v) => Some(v)
          case None    => None
        }
        case Some(a) => Some(a)
      }
    }
    case JoinOpNode(left, right, _, _, _, _) => {
      getSubquery(right) match {
        case None => getSubquery(left) match {
          case Some(v) => Some(v)
          case None    => None
        }
        case Some(a) => Some(a)
      }
    }
    case AggOpNode(parent, _, _, _)       => getSubquery(parent)
    case MapOpNode(parent, _)             => getSubquery(parent)
    case OrderByNode(parent, _)           => getSubquery(parent)
    case PrintOpNode(parent, _, _)        => getSubquery(parent)
    case ProjectOpNode(parent, _, _)      => getSubquery(parent)
    case ViewOpNode(parent, _, _)         => getSubquery(parent)
    case SubqueryNode(parent)             => Some(parent)
    case SubquerySingleResultNode(parent) => Some(parent)
  }

  def isPrimaryKey(column: String, tableName: String): Boolean = schema.findTable(tableName) match {
    case Some(table) => {
      table.primaryKey match {
        case Some(v) => {
          var result = false
          for (attribute <- v.attributes) {
            if (attribute.name.equals(column)) {
              result = true
            }
          }
          result
        }
        case None => false
      }
    }
    case None => false
  }

  def removeSubquery(node: OperatorNode = queryPlan.rootNode): OperatorNode = node match {
    case SelectOpNode(parent, cond, isHavingClause) => SelectOpNode(removeSubquery(parent), cond, isHavingClause)
    case ScanOpNode(_, _, _)                        => node
    case UnionAllOpNode(top, bottom) => {
      UnionAllOpNode(removeSubquery(top), removeSubquery(bottom))
    }
    case JoinOpNode(left, right, clause, joinType, leftAlias, rightAlias) => {
      JoinOpNode(removeSubquery(left), removeSubquery(right), clause, joinType, leftAlias, rightAlias)
    }
    case AggOpNode(parent, aggs, gb, aggNames)            => AggOpNode(removeSubquery(parent), aggs, gb, aggNames)
    case MapOpNode(parent, mapIndices)                    => MapOpNode(removeSubquery(parent), mapIndices)
    case OrderByNode(parent, orderBy)                     => OrderByNode(removeSubquery(parent), orderBy)
    case PrintOpNode(parent, projNames, limit)            => PrintOpNode(removeSubquery(parent), projNames, limit)
    case ProjectOpNode(parent, projNames, origFieldNames) => ProjectOpNode(removeSubquery(parent), projNames, origFieldNames)
    case ViewOpNode(parent, projNames, name)              => ViewOpNode(removeSubquery(parent), projNames, name)
    case SubqueryNode(_)                                  => ScanOpNode(new Table("TMP_VIEW", List(), new scala.collection.mutable.ArrayBuffer[Constraint], ""), "TMP_VIEW", None)
    case SubquerySingleResultNode(_)                      => ScanOpNode(new Table("TMP_VIEW", List(), new scala.collection.mutable.ArrayBuffer[Constraint], ""), "TMP_VIEW", None)
  }

  //assumes two tables are joined only on one attribute
  def getJoinColumns(condition: Expression, tableName1: String, tableName2: String): Option[(String, String)] = {
    condition match {
      case Equals(e1: IntLiteral, e2: IntLiteral) => if (e1.v == 1 && e2.v == 1) Some(("1", "1")) else None
      case Equals(e1: FieldIdent, e2: FieldIdent) => (e1.qualifier, e2.qualifier) match {
        case (Some(`tableName1`), Some(`tableName2`))                   => Some((e1.name, e2.name))
        case (Some(`tableName2`), Some(`tableName1`))                   => Some((e2.name, e1.name))
        case (Some(_), Some(`tableName1`)) | (Some(`tableName1`), None) => Some((e1.name, ""))
        case (Some(_), Some(`tableName2`)) | (Some(`tableName2`), None) => Some(("", e2.name))
      }
      case And(e1, e2) => (getJoinColumns(e1, tableName1, tableName2), getJoinColumns(e2, tableName1, tableName2)) match {
        case (Some(v1), Some(v2)) => Some(v1) //fix this
        case (Some(v), None)      => Some(v)
        case (None, Some(v))      => Some(v)
        case _                    => None
      }
      case LessThan(e1: FieldIdent, e2: FieldIdent) => (e1.qualifier, e2.qualifier) match {
        case (Some(`tableName1`), Some(`tableName2`))                   => Some((e1.name, e2.name))
        case (Some(`tableName2`), Some(`tableName1`))                   => Some((e2.name, e1.name))
        case (Some(_), Some(`tableName1`)) | (Some(`tableName1`), None) => Some((e1.name, ""))
        case (Some(_), Some(`tableName2`)) | (Some(`tableName2`), None) => Some(("", e2.name))
      }
      case NotEquals(e1: FieldIdent, e2: FieldIdent) => (e1.qualifier, e2.qualifier) match {
        case (Some(`tableName1`), Some(`tableName2`))                   => Some((e1.name, e2.name))
        case (Some(`tableName2`), Some(`tableName1`))                   => Some((e2.name, e1.name))
        case (Some(_), Some(`tableName1`)) | (Some(`tableName1`), None) => Some((e1.name, ""))
        case (Some(_), Some(`tableName2`)) | (Some(`tableName2`), None) => Some(("", e2.name))
      }
    }
  }
}