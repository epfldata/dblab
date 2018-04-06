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
      case ScanOpNode(table, _, _)     => schema.stats.getCardinality(table.name)
      case SelectOpNode(parent, _, _)  => tau * size(parent)
      case UnionAllOpNode(top, bottom) => size(top) * size(bottom)
      case JoinOpNode(left, right, condition, joinType, _, _) => {
        val selinger = optimizeJoins(getJoinList(node))
        /*System.out.println("##############")
        System.out.println("Cost: " + selinger._1)
        System.out.println("Size: " + selinger._2)
        System.out.println("Order (last to first): " + selinger._3)
        System.out.println("##############")*/
        //val tableList = getJoinTableList(node)
        //System.out.print(tableList)
        //schema.stats.getJoinOutputEstimation(tableList)
        getJoinOutputEstimation(condition) //correct this
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
  // is it really needed?
  /*def simplifyJoinTree(queryPlan: QueryPlanTree): QueryPlanTree = {
    def getJoins(node: OperatorNode): OperatorNode = node match {
      case ScanOpNode(table, _, _)          => node
      case SelectOpNode(parent, _, _)       => getJoins(parent)
      case JoinOpNode(left, _)              => JoinOpNode(getJoins(left), _)
      case AggOpNode(parent, _, _, _)       => getJoins(parent)
      case MapOpNode(parent, _)             => getJoins(parent)
      case OrderByNode(parent, _)           => getJoins(parent)
      case PrintOpNode(parent, _, _)        => getJoins(parent)
      case SubqueryNode(parent)             => getJoins(parent)
      case SubquerySingleResultNode(parent) => size(parent)
      case ProjectOpNode(parent, _, _)      => getJoins(parent)
      case ViewOpNode(parent, _, _)         => ViewOpNode(getJoins(parent), _, _)
      case UnionAllOpNode(top, bottom)      => getJoins(top)
    }
    return ???
  }*/

  def getJoinOutputEstimation(condition: Expression): Int = condition match {
    case Equals(e1: FieldIdent, e2: FieldIdent) => {
      val tableName1 = e1.qualifier match {
        case Some(v) => v
        case None    => throw new Exception("Can't find table")
      }
      val tableName2 = e2.qualifier match {
        case Some(v) => v
        case None    => throw new Exception("Can't find table")
      }

      val table1 = schema.findTable(tableName1) match {
        case Some(v) => v
        case None    => throw new Exception("Can't find table: " + tableName1)
      }
      val table2 = schema.findTable(tableName2) match {
        case Some(v) => v
        case None    => throw new Exception("Can't find table: " + tableName2)
      }

      if (table1.primaryKey.toString == e1.name) {
        schema.stats.getCardinality(tableName2).toInt
      } else if (table2.primaryKey.toString == e2.name) {
        schema.stats.getCardinality(tableName1).toInt
      } else {
        val cardinality1 = schema.stats.getCardinality(tableName1)
        val cardinality2 = schema.stats.getCardinality(tableName2)
        max(cardinality1, cardinality2).toInt
      }
    }
    case And(e1, e2) => (schema.stats.getFilterSelectivity(e1) * getJoinOutputEstimation(e2)).toInt
  }

  def getJoinList(node: OperatorNode, result: List[(String, String, Int, Int, Expression, JoinType)] = Nil): List[(String, String, Int, Int, Expression, JoinType)] = {
    node match {
      case ScanOpNode(table, _, _)    => result
      case SelectOpNode(parent, _, _) => getJoinList(parent, result)
      case AggOpNode(parent, _, _, _) => getJoinList(parent, result)
      case JoinOpNode(left, _, clause, joinType, _, _) => {
        val (tableName1, tableName2, size1, size2) = getTablesAndSizes(clause)
        val thisJoin = (tableName1, tableName2, size1, size2, clause, joinType)
        getJoinList(left, thisJoin :: result)
      }
      case SubqueryNode(parent)             => Nil
      case SubquerySingleResultNode(parent) => Nil
    }
  }

  def getTablesAndSizes(expression: Expression): (String, String, Int, Int) = expression match {
    case And(e1, e2) => getTablesAndSizes(e1)
    case Equals(e1: FieldIdent, e2: FieldIdent) =>
      val tableName1 = e1.qualifier match {
        case Some(v) => v
        case None    => throw new Exception("Can't find table")
      }
      val tableName2 = e2.qualifier match {
        case Some(v) => v
        case None    => throw new Exception("Can't find table")
      }
      (tableName1, tableName2, schema.stats.getCardinality(tableName1).toInt, schema.stats.getCardinality(tableName1).toInt)
  }
  def isMainRelation(table: String, clause: Expression) {

  }

  def optimizeJoins(tableList: List[(String, String, Int, Int, Expression, JoinType)]): (Int, Int, List[String]) = {
    val tables = tableList.map(join => List(join._1, join._2)).flatten.toSet
    val subsets = tables.subsets.toList.groupBy(_.size).filter(_._1 != 0)
    val joinWays = tableList.map(join => List((join._1, join._2, join._5, join._6), (join._2, join._1, join._5, join._6))).flatten.groupBy(_._1)
    val joinCosts = new scala.collection.mutable.HashMap[List[String], (Int, Int, List[String])](); //maps list of tables to best cost, size and joinPlan

    /*for (v <- subsets.values) {
      for (subset <- v) {
        joinCosts.put(subset.toList.sorted, (Integer.MAX_VALUE, Nil))
      }
    }*/

    for (i <- 1 to tables.size) {
      for (subset <- subsets(i)) {
        //when a subset is only one table
        if (subset.size == 1) {

          var size = schema.stats.getCardinality(subset.head).toInt
          //is cost == size for one table?
          joinCosts.put(subset.toList.sorted, (size, size, subset.toList.sorted))
        }
        var bestPlan = (Integer.MAX_VALUE, Integer.MAX_VALUE, Nil: List[String])
        for (table <- subset) {
          var subsetToConsider = subset - table
          for (join <- joinWays(table)) {
            if (subsetToConsider.contains(join._2)) {
              var costAndPlan1 = joinCosts.get(subsetToConsider.toList.sorted) match {
                case Some(v) => v
                case None    => throw new Exception("No key: " + subsetToConsider.toList.sorted + " exists in the joinCosts HashMap")
              }
              var costAndPlan2 = joinCosts.get(List(table)) match {
                case Some(v) => v
                case None    => throw new Exception("No key: " + table.toList + " exists in the joinCosts HashMap")
              }
              var size = 0
              var cost = join._4 match {
                case IndexNestedLoopJoin | NestedLoopJoin =>
                  size = max(getJoinOutputEstimation(join._3), costAndPlan1._2)
                  costAndPlan1._1 + lambda * costAndPlan1._2 * max(costAndPlan2._2 / costAndPlan1._2, 1)
                case _ =>
                  size = max(getJoinOutputEstimation(join._3), costAndPlan1._2)
                  max(costAndPlan2._2, costAndPlan1._2) + costAndPlan1._1 + costAndPlan2._1
              }
              if (cost < bestPlan._1) {
                bestPlan = (cost, size, table :: costAndPlan1._3)
              }
            }
          }
        }
        joinCosts.put(subset.toList.sorted, bestPlan)
      }
    }
    joinCosts.get(subsets(tables.size).head.toList.sorted) match {
      case Some(v) => v
      case None    => throw new Exception("Error while loading best join plan")
    }
  }
}