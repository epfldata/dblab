package ch.epfl.data
package dblab
package frontend
package analyzer

import ch.epfl.data.dblab.frontend.parser.SQLAST
import parser.CalcAST._
import sc.pardis.search.CostingContext
import sc.pardis.ast.Node
import schema._
import frontend.parser._
import parser.OperatorAST._
import frontend.optimizer._
import frontend.analyzer._
import SQLAST._
import scala.math._

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
          case _                                    => size(node) + cost(left) + cost(right)
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

  def size(node: OperatorNode): Double = {
    node match {
      case ScanOpNode(table, _, _)                    => schema.stats.getCardinality(table.name)
      case SelectOpNode(parent, _, _)                 => tau * size(parent)
      case UnionAllOpNode(top, bottom)                => size(top) * size(bottom)
      case JoinOpNode(left, right, _, joinType, _, _) => schema.stats.getJoinOutputEstimation(getJoinTableList(node))
      case AggOpNode(parent, _, _, _)                 => size(parent)
      case MapOpNode(parent, _)                       => size(parent)
      case OrderByNode(parent, _)                     => size(parent)
      case PrintOpNode(parent, _, _)                  => size(parent)
      case SubqueryNode(parent)                       => size(parent)
      case SubquerySingleResultNode(parent)           => size(parent)
      case ProjectOpNode(parent, _, _)                => size(parent)
      case ViewOpNode(parent, _, _)                   => size(parent)
    }
  }

  def getJoinTableList(node: OperatorNode, tableNames: List[String] = Nil): List[String] = {
    node match {
      case ScanOpNode(table, _, _)    => table.name :: tableNames
      case SelectOpNode(parent, _, _) => getJoinTableList(parent)
      case AggOpNode(parent, _, _, _) => getJoinTableList(parent) ::: tableNames
      case JoinOpNode(left, right, _, _, leftAlias, rightAlias) => {
        (leftAlias, rightAlias) match {
          case ("", "") => getJoinTableList(left, tableNames) ::: getJoinTableList(right)
          case (_, "")  => getJoinTableList(right, leftAlias :: tableNames)
          case ("", _)  => getJoinTableList(left, rightAlias :: tableNames)
          case (_, _)   => leftAlias :: rightAlias :: tableNames
        }
      }
      case SubqueryNode(parent)             => getJoinTableList(parent) ::: tableNames
      case SubquerySingleResultNode(parent) => getJoinTableList(parent) ::: tableNames
    }
  }

}