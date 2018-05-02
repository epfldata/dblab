package ch.epfl.data
package dblab
package frontend
package analyzer

import parser.CalcAST._
import sc.pardis.search.CostingContext
import sc.pardis.ast.Node
import schema._
import frontend.parser._
import parser.OperatorAST._
import frontend.optimizer._
import frontend.analyzer._
import ch.epfl.data.dblab.frontend.parser.SQLAST._
import scala.math._
import collection.mutable.HashMap

class PlanCosting(schema: Schema, queryPlan: QueryPlanTree) {
  val tau = 0.2
  val lambda = 2

  def cost(node: OperatorNode = queryPlan.rootNode): Double = {
    node match {
      case ScanOpNode(table, _, _)     => tau * size(node)
      case SelectOpNode(parent, _, _)  => tau * size(parent) + cost(parent)
      case UnionAllOpNode(top, bottom) => cost(top) + cost(bottom) + size(top) * size(bottom)
      case JoinOpNode(left, right, _, joinType, _, _) =>
        joinType match {
          case IndexNestedLoopJoin | NestedLoopJoin => cost(left) + lambda * size(left) * max(size(node) / size(left), 1)
          case HashJoin                             => size(node) + cost(left) + cost(right)
          case _                                    => size(left) * size(right)
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
    case SelectOpNode(parent, _, _)  => tau * size(parent)
    case UnionAllOpNode(top, bottom) => size(top) * size(bottom)
    case JoinOpNode(left, right, condition, joinType, _, rightAlias) => joinType match {
      //getJoinOutputEstimation(condition, size(left).toInt, rightAlias) //fix this
      case AntiJoin                             => size(left)
      case HashJoin                             => max(size(left), size(right))
      case InnerJoin | NaturalJoin              => size(left) * size(right)
      case LeftSemiJoin                         => size(left) * size(right)
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

  def getJoinTableList(node: OperatorNode, tableNames: List[String] = Nil): List[String] = {
    node match {
      case ScanOpNode(table, _, _)    => table.name :: tableNames
      case SelectOpNode(parent, _, _) => getJoinTableList(parent, tableNames)
      case AggOpNode(parent, _, _, _) => getJoinTableList(parent, tableNames)
      case JoinOpNode(left, right, _, _, leftAlias, rightAlias) => {
        (leftAlias, rightAlias) match {
          case ("", "") => getJoinTableList(left, tableNames) ::: getJoinTableList(right)
          case (_, "")  => getJoinTableList(right, leftAlias :: tableNames)
          case ("", _)  => getJoinTableList(left, rightAlias :: tableNames)
          case (_, _)   => leftAlias :: rightAlias :: tableNames
        }
      }
      case SubqueryNode(parent)             => getJoinTableList(parent, tableNames)
      case SubquerySingleResultNode(parent) => getJoinTableList(parent, tableNames)
    }
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

  def getJoinOutputEstimation(condition: Expression, sizeLeft: Int, table: String): Int = condition match {
    case Equals(e1: FieldIdent, e2: FieldIdent) => {
      val tableName1 = e1.qualifier match {
        case Some(v) => v
        case None    => throw new Exception("Can't find table ")
      }
      val tableName2 = e2.qualifier match {
        case Some(v) => v
        case None    => throw new Exception("Can't find table")
      }

      if (isPrimaryKey(e1.name, tableName1)) {
        if (tableName2 == table) {
          schema.stats.getCardinality(tableName2).toInt
        } else {
          sizeLeft
        }

      } else if (isPrimaryKey(e2.name, tableName2)) {
        if (tableName1 == table) {
          schema.stats.getCardinality(tableName1).toInt
        } else {
          sizeLeft
        }
      } else {
        //very general for now, returning size of bigger table in case of join that doesn't include a primary key
        val cardinality1 = schema.stats.getCardinality(tableName1)
        val cardinality2 = schema.stats.getCardinality(tableName2)
        max(max(cardinality1, cardinality2), sizeLeft).toInt
      }
    }
    case And(e1, e2)      => (schema.stats.getFilterSelectivity(e1) * getJoinOutputEstimation(e2, sizeLeft, table)).toInt
    case LessThan(e1, e2) => (0.5 * sizeLeft).toInt //TODO
  }

  def getJoinList(node: OperatorNode, result: List[(String, String, Int, Int, Expression, JoinType)] = Nil): List[(String, String, Int, Int, Expression, JoinType)] = {
    node match {
      case ScanOpNode(table, _, _) => result
      case JoinOpNode(left, _, clause, joinType, _, _) => {
        val (tableName1, tableName2, size1, size2) = getTablesAndSizes(clause)
        val thisJoin = (tableName1, tableName2, size1, size2, clause, joinType)
        getJoinList(left, thisJoin :: result)
      }
      case SelectOpNode(parent, _, _)       => getJoinList(parent, result)
      case AggOpNode(parent, _, _, _)       => getJoinList(parent, result)
      case MapOpNode(parent, _)             => getJoinList(parent, result)
      case OrderByNode(parent, _)           => getJoinList(parent, result)
      case PrintOpNode(parent, _, _)        => getJoinList(parent, result)
      case SubqueryNode(parent)             => result
      case SubquerySingleResultNode(parent) => result
      case ProjectOpNode(parent, _, _)      => getJoinList(parent, result)
      case ViewOpNode(parent, _, _)         => getJoinList(parent, result)
      case UnionAllOpNode(top, bottom)      => getJoinList(top) ::: getJoinList(bottom, result)
    }
  }

  def getSubquery(node: OperatorNode): Option[OperatorNode] = node match {
    case SubqueryNode(parent) => Some(parent)
    case ScanOpNode(_, _, _)  => None
    case JoinOpNode(left, right, clause, joinType, _, _) => getSubquery(left) match {
      case Some(v) => Some(v)
      case None => getSubquery(right) match {
        case Some(a) => Some(a)
        case None    => None
      }
    }
    case SelectOpNode(parent, _, _)       => getSubquery(parent)
    case AggOpNode(parent, _, _, _)       => getSubquery(parent)
    case MapOpNode(parent, _)             => getSubquery(parent)
    case OrderByNode(parent, _)           => getSubquery(parent)
    case PrintOpNode(parent, _, _)        => getSubquery(parent)
    case SubquerySingleResultNode(parent) => getSubquery(parent)
    case ProjectOpNode(parent, _, _)      => getSubquery(parent)
    case ViewOpNode(parent, _, _)         => getSubquery(parent)
    case UnionAllOpNode(top, bottom) => getSubquery(top) match {
      case Some(v) => Some(v)
      case None => getSubquery(bottom) match {
        case Some(a) => Some(a)
        case None    => None
      }
    }
  }

  //assumes two tables are joined only on one attribute
  def getJoinColumns(condition: Expression, tableName1: String, tableName2: String): Option[(String, String)] = {
    condition match {
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
      case LessThan(e1, e2) => None
    }
  }

  def getTablesAndSizes(expression: Expression): (String, String, Int, Int) = expression match {
    case And(e1, e2) => getTablesAndSizes(e1)
    case Equals(e1: FieldIdent, e2: FieldIdent) =>
      val tableName1 = e1.qualifier match {
        case Some(v) => v
        case None    => throw new Exception("Can't find table " + e1.qualifier)
      }
      val tableName2 = e2.qualifier match {
        case Some(v) => v
        case None    => throw new Exception("Can't find table " + e2.qualifier)
      }
      (tableName1, tableName2, schema.stats.getCardinality(tableName1).toInt, schema.stats.getCardinality(tableName1).toInt)
  }
}