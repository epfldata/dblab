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
  val tableFilters = new scala.collection.mutable.HashMap[String, List[Expression]]()
  val generalFilters = new scala.collection.mutable.ArrayBuffer[Expression]()
  val tableJoins = new scala.collection.mutable.HashMap[String, List[Expression]]()
  getExpressions()
  val filterExprs: scala.collection.mutable.HashMap[String, Expression] = tableFilters.map(t => t._1 -> t._2.tail.foldLeft(t._2.head)((a, b) => And(a, b)))

  def cost(node: OperatorNode = queryPlan.rootNode): Double = {
    node match {
      case ScanOpNode(_, _, _)         => size(node)
      case SelectOpNode(parent, _, _)  => tau * size(parent) + cost(parent)
      case UnionAllOpNode(top, bottom) => cost(top) + cost(bottom) + size(top) * size(bottom)
      case JoinOpNode(left, right, _, joinType, _, _) =>
        joinType match {
          case IndexNestedLoopJoin | NestedLoopJoin =>
            val result = cost(left) + lambda * size(left) * max(size(node) / size(left), 1)
            //System.out.println("Cost: " + cost(left) + " + " + lambda * size(left) + " * " + size(node) + " / " + size(left) + " = " + result)
            result
          case HashJoin => size(node) + cost(left) + cost(right)
          case _        => size(left) * size(right)
        }
      case AggOpNode(parent, _, _, _) => size(parent) + cost(parent)
      case MapOpNode(parent, _)       => size(parent) + cost(parent)
      case OrderByNode(parent, _) => {
        val parent_size = size(parent)
        parent_size * log10(parent_size) / log10(2) + cost(parent)
      }
      case PrintOpNode(parent, _, _)        => size(parent) + cost(parent)
      case SubqueryNode(parent)             => cost(parent)
      case SubquerySingleResultNode(parent) => cost(parent)
      case ProjectOpNode(parent, _, _)      => size(parent) + cost(parent)
      case ViewOpNode(parent, _, _)         => cost(parent)
    }
  }

  def size(node: OperatorNode): Double = node match {
    case ScanOpNode(table, _, _)     => schema.stats.getCardinality(table.name)
    case SelectOpNode(parent, e, _)  => schema.stats.getFilterSelectivity(e) * size(parent)
    case UnionAllOpNode(top, bottom) => size(top) * size(bottom)
    case JoinOpNode(left, right, condition, joinType, _, _) => joinType match {
      //getJoinOutputEstimation(condition, size(left).toInt, rightAlias) //fix this
      case AntiJoin                             => size(left)
      case HashJoin                             => max(size(left), size(right))
      case InnerJoin | NaturalJoin              => size(left) * size(right)
      case LeftSemiJoin                         => size(left)
      case LeftOuterJoin                        => size(left)
      case RightOuterJoin                       => size(right)
      case FullOuterJoin                        => size(left) * size(right)
      case NestedLoopJoin | IndexNestedLoopJoin => size(left)

    }
    case AggOpNode(parent, _, _, _)       => size(parent)
    case MapOpNode(parent, _)             => size(parent)
    case OrderByNode(parent, _)           => size(parent)
    case PrintOpNode(parent, _, _)        => size(parent)
    case SubqueryNode(parent)             => size(parent)
    case SubquerySingleResultNode(parent) => size(parent)
    case ProjectOpNode(parent, _, _)      => size(parent)
    case ViewOpNode(parent, _, _)         => size(parent)
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

  def parseExpression(e: Expression) {
    e match {
      case And(e1, e2) => {
        parseExpression(e1)
        parseExpression(e2)
      }
      case Equals(fi1: FieldIdent, fi2: FieldIdent) => {
        if (sameTables(fi1, fi2)) {
          registerExpression(e, fi1, tableFilters)
          registerExpression(e, fi2, tableFilters)
        } else {
          registerExpression(e, fi1, tableJoins)
          registerExpression(e, fi2, tableJoins)
        }
      }
      case Equals(fi: FieldIdent, lit: LiteralExpression) =>
        registerExpression(e, fi, tableFilters)
      case NotEquals(fi: FieldIdent, lit: LiteralExpression)      => registerExpression(e, fi, tableFilters)
      case GreaterThan(fi: FieldIdent, lit: LiteralExpression)    => registerExpression(e, fi, tableFilters)
      case GreaterOrEqual(fi: FieldIdent, lit: LiteralExpression) => registerExpression(e, fi, tableFilters)
      case LessThan(fi: FieldIdent, lit: LiteralExpression)       => registerExpression(e, fi, tableFilters)
      case LessThan(fi1: FieldIdent, fi2: FieldIdent) => {
        if (sameTables(fi1, fi2)) {
          registerExpression(e, fi1, tableFilters)
        } else {
          registerExpression(e, fi1, tableJoins)
          registerExpression(e, fi2, tableJoins)
        }
      }
      case LessOrEqual(fi: FieldIdent, lit: LiteralExpression) => registerExpression(e, fi, tableFilters)
      case LessOrEqual(fi1: FieldIdent, fi2: FieldIdent) => {
        if (sameTables(fi1, fi2)) {
          registerExpression(e, fi1, tableFilters)
          registerExpression(e, fi2, tableFilters)
        } else {
          registerExpression(e, fi1, tableJoins)
          registerExpression(e, fi2, tableJoins)
        }
      }
      case Like(fi: FieldIdent, lit: LiteralExpression) => registerExpression(e, fi, tableFilters)
      case In(fi: FieldIdent, _)                        => //registerScanOpCond(fi, cond)
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

  //assumes only one aggregation per query (not counting subqueries)
  def getAggregation(node: OperatorNode = queryPlan.rootNode): Option[OperatorNode] = node match {
    case SelectOpNode(parent, _, _) => getAggregation(parent)
    case ScanOpNode(_, _, _)        => None
    case UnionAllOpNode(top, bottom) => {
      getAggregation(top) match {
        case None => getAggregation(bottom) match {
          case Some(v) => Some(v)
          case None    => None
        }
        case Some(a) => Some(a)
      }
    }
    case JoinOpNode(left, right, _, _, _, _) => {
      getAggregation(right) match {
        case None => getAggregation(left) match {
          case Some(v) => Some(v)
          case None    => None
        }
        case Some(a) => Some(a)
      }
    }
    case AggOpNode(parent, _, _, _)                    => Some(node)
    case MapOpNode(parent, _)                          => getAggregation(parent)
    case OrderByNode(parent, _)                        => getAggregation(parent)
    case PrintOpNode(parent, _, _)                     => getAggregation(parent)
    case ProjectOpNode(parent, _, _)                   => getAggregation(parent)
    case ViewOpNode(parent, _, _)                      => getAggregation(parent)
    case SubqueryNode(_) | SubquerySingleResultNode(_) => None
  }

  def getOrder(node: OperatorNode = queryPlan.rootNode): Option[OperatorNode] = node match {
    case SelectOpNode(parent, _, _) => getOrder(parent)
    case ScanOpNode(_, _, _)        => None
    case UnionAllOpNode(top, bottom) => {
      getOrder(top) match {
        case None => getOrder(bottom) match {
          case Some(v) => Some(v)
          case None    => None
        }
        case Some(a) => Some(a)
      }
    }
    case JoinOpNode(left, right, _, _, _, _) => {
      getOrder(right) match {
        case None => getOrder(left) match {
          case Some(v) => Some(v)
          case None    => None
        }
        case Some(a) => Some(a)
      }
    }
    case AggOpNode(parent, _, _, _)                    => None
    case MapOpNode(parent, _)                          => getOrder(parent)
    case OrderByNode(parent, _)                        => Some(node)
    case PrintOpNode(parent, _, _)                     => getOrder(parent)
    case ProjectOpNode(parent, _, _)                   => getOrder(parent)
    case ViewOpNode(parent, _, _)                      => getOrder(parent)
    case SubqueryNode(_) | SubquerySingleResultNode(_) => None
  }

  def getMap(node: OperatorNode = queryPlan.rootNode): Option[OperatorNode] = node match {
    case SelectOpNode(parent, _, _) => getMap(parent)
    case ScanOpNode(_, _, _)        => None
    case UnionAllOpNode(top, bottom) => {
      getMap(top) match {
        case None => getMap(bottom) match {
          case Some(v) => Some(v)
          case None    => None
        }
        case Some(a) => Some(a)
      }
    }
    case JoinOpNode(left, right, _, _, _, _) => {
      getMap(right) match {
        case None => getMap(left) match {
          case Some(v) => Some(v)
          case None    => None
        }
        case Some(a) => Some(a)
      }
    }
    case AggOpNode(parent, _, _, _)       => getMap(parent)
    case MapOpNode(parent, _)             => Some(node)
    case OrderByNode(parent, _)           => getMap(parent)
    case PrintOpNode(parent, _, _)        => getMap(parent)
    case ProjectOpNode(parent, _, _)      => getMap(parent)
    case ViewOpNode(parent, _, _)         => getMap(parent)
    case SubqueryNode(parent)             => None
    case SubquerySingleResultNode(parent) => None
  }

  def getProjection(node: OperatorNode = queryPlan.rootNode): Option[OperatorNode] = node match {
    case SelectOpNode(parent, _, _) => getProjection(parent)
    case ScanOpNode(_, _, _)        => None
    case UnionAllOpNode(top, bottom) => {
      getProjection(top) match {
        case None => getProjection(bottom) match {
          case Some(v) => Some(v)
          case None    => None
        }
        case Some(a) => Some(a)
      }
    }
    case JoinOpNode(left, right, _, _, _, _) => {
      getProjection(right) match {
        case None => getProjection(left) match {
          case Some(v) => Some(v)
          case None    => None
        }
        case Some(a) => Some(a)
      }
    }
    case AggOpNode(parent, _, _, _)       => getProjection(parent)
    case MapOpNode(parent, _)             => getProjection(parent)
    case OrderByNode(parent, _)           => getProjection(parent)
    case PrintOpNode(parent, _, _)        => getProjection(parent)
    case ProjectOpNode(parent, _, _)      => Some(node)
    case ViewOpNode(parent, _, _)         => getProjection(parent)
    case SubqueryNode(parent)             => None
    case SubquerySingleResultNode(parent) => None
  }

  def getPrint(node: OperatorNode = queryPlan.rootNode): Option[OperatorNode] = node match {
    case SelectOpNode(parent, _, _) => getPrint(parent)
    case ScanOpNode(_, _, _)        => None
    case UnionAllOpNode(top, bottom) => {
      getPrint(top) match {
        case None => getPrint(bottom) match {
          case Some(v) => Some(v)
          case None    => None
        }
        case Some(a) => Some(a)
      }
    }
    case JoinOpNode(left, right, _, _, _, _) => {
      getPrint(right) match {
        case None => getPrint(left) match {
          case Some(v) => Some(v)
          case None    => None
        }
        case Some(a) => Some(a)
      }
    }
    case AggOpNode(parent, _, _, _)       => getPrint(parent)
    case MapOpNode(parent, _)             => getPrint(parent)
    case OrderByNode(parent, _)           => getPrint(parent)
    case PrintOpNode(parent, _, _)        => Some(node)
    case ProjectOpNode(parent, _, _)      => getPrint(parent)
    case ViewOpNode(parent, _, _)         => getPrint(parent)
    case SubqueryNode(parent)             => None
    case SubquerySingleResultNode(parent) => None
  }

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