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
  def selingerAsString(node: OperatorNode): String = node match {
    case ScanOpNode(table, _, _)    => ""
    case SelectOpNode(parent, _, _) => selingerAsString(parent)
    case UnionAllOpNode(top, bottom) => {
      selingerAsString(top)
      selingerAsString(bottom)
    }
    case JoinOpNode(left, right, condition, joinType, _, rightAlias) => {
      val selinger = optimizeJoins(getJoinList(node))
      "##############\nCost: " + selinger._1 + "\nSize: " + selinger._2 + "\nOrder: " + selinger._3 + "\n###################"
    }
    case AggOpNode(parent, _, _, _)       => selingerAsString(parent)
    case MapOpNode(parent, _)             => selingerAsString(parent)
    case OrderByNode(parent, _)           => selingerAsString(parent)
    case PrintOpNode(parent, _, _)        => selingerAsString(parent)
    case SubqueryNode(parent)             => selingerAsString(parent)
    case SubquerySingleResultNode(parent) => selingerAsString(parent)
    case ProjectOpNode(parent, _, _)      => selingerAsString(parent)
    case ViewOpNode(parent, _, _)         => selingerAsString(parent)
  }

  def size(node: OperatorNode): Double = node match {
    case ScanOpNode(table, _, _)     => schema.stats.getCardinality(table.name)
    case SelectOpNode(parent, _, _)  => tau * size(parent)
    case UnionAllOpNode(top, bottom) => size(top) * size(bottom)
    case JoinOpNode(left, right, condition, joinType, _, rightAlias) => {
      //val selinger = optimizeJoins(getJoinList(node))
      /*System.out.println("##############")
        System.out.println("Cost: " + selinger._1)
        System.out.println("Size: " + selinger._2)
        System.out.println("Order: " + selinger._3)
        System.out.println("##############")*/
      //val tableList = getJoinTableList(node)
      //System.out.print(tableList)
      //schema.stats.getJoinOutputEstimation(tableList)
      getJoinOutputEstimation(condition, size(left).toInt, rightAlias) //correct this
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

  def getJoinOutputEstimation(condition: Expression, sizeLeft: Int, table: String): Int = condition match {
    case Equals(e1: FieldIdent, e2: FieldIdent) => {
      val tableName1 = e1.qualifier
      val tableName2 = e2.qualifier

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
      case SubqueryNode(parent)             => getJoinList(parent, result)
      case SubquerySingleResultNode(parent) => getJoinList(parent, result)
      case ProjectOpNode(parent, _, _)      => getJoinList(parent, result)
      case ViewOpNode(parent, _, _)         => getJoinList(parent, result)
      case UnionAllOpNode(top, bottom)      => getJoinList(top) ::: getJoinList(bottom, result)
    }
  }

  def getSubquery(node: OperatorNode): Option[OperatorNode] = node match {
    case SubqueryNode(parent) => Some(node)
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
        //TODO case (Some(v1), Some(v2)) =>
        case (Some(v), None) => Some(v)
        case (None, Some(v)) => Some(v)
        case _               => None
      }
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

  def optimizeJoins(tableList: List[(String, String, Int, Int, Expression, JoinType)]): (Long, Long, OperatorNode) = {
    if (tableList.size == 0) {
      throw new Exception("No joins to optimize!")
    }

    val tables = tableList.map(join => List(join._1, join._2)).flatten.toSet //list of tables to join
    val subsets = tables.subsets.toList.groupBy(_.size).filter(_._1 != 0) //map of subset.size -> list(subsets_of_this_size)
    val joinWays = tableList.map(join => List((join._1, join._2, join._5, join._6), (join._2, join._1, join._5, join._6))).flatten.groupBy(_._1) //map of table -> list(table1, table2, expression, joinType)
    val joinCosts = new scala.collection.mutable.HashMap[List[String], (Long, Long, OperatorNode)](); //maps of table_list -> (best cost, size, joinPlan)

    for (i <- 1 to tables.size) {
      for (subset <- subsets(i)) {
        //when a subset is only one table
        if (i == 1) {
          var tableName = subset.head
          schema.findTable(tableName) match {
            case Some(v) => {
              var table = v
              var size = schema.stats.getCardinality(tableName).toLong
              joinCosts.put(subset.toList.sorted, (size, size.toInt, new ScanOpNode(table, tableName, None)))
            }
            case None => getSubquery(queryPlan.rootNode) match {
              case Some(v) => {
                if (tableName.equals("TMP_VIEW")) joinCosts.put(subset.toList.sorted, (cost(v).toLong, size(v).toLong, v)) else None
              }
              case None => throw new Exception(s"Can't find table: $tableName")
            }
          }
          //System.out.println(s"Best plan for $tableName is cost: $size and size: $size")
        } else {
          var dummyTableName = subset.head
          var dummyTable = schema.findTable(dummyTableName) match {
            case Some(v) => v
            case None    => throw new Exception("Can't find table: " + dummyTableName)
          }
          var bestPlan = (Long.MaxValue, Long.MaxValue, new ScanOpNode(dummyTable, subset.head + "_dummy", None): OperatorNode)

          for (table <- subset) {
            var subsetToConsider = subset - table
            for (join <- joinWays(table)) {
              if (subsetToConsider.contains(join._2)) {

                //get best plan and cost for one table
                val costAndPlan1 = joinCosts.get(List(table)) match {
                  case Some(v) => v
                  case None    => throw new Exception(s"No key: $table exists in the joinCosts HashMap")
                }

                //get best plan and cost for subset to join to
                val costAndPlan2 = joinCosts.get(subsetToConsider.toList.sorted) match {
                  case Some(v) => v
                  case None    => throw new Exception(s"No key: " + subsetToConsider.toList.sorted + " exists in the joinCosts HashMap")
                }

                var size = Long.MaxValue
                var cost = Long.MaxValue
                var joinType = join._4

                if (costAndPlan2._1 != Long.MaxValue) {

                  val (column1, column2) = getJoinColumns(join._3, table, join._2) match {
                    case Some(v) => v
                    case _       => throw new Exception("Expression " + join._3 + " contains no column names")
                  }

                  if (isPrimaryKey(column1, table)) {
                    joinType = NestedLoopJoin
                    size = costAndPlan2._2
                    cost = costAndPlan2._1 + lambda * costAndPlan2._2 //* max(size / costAndPlan2._2, 1)
                  } else if (isPrimaryKey(column2, join._2)) {
                    joinType = NestedLoopJoin
                    size = costAndPlan1._2
                    cost = costAndPlan1._1 + lambda * costAndPlan1._2 //* max(size / costAndPlan1._2, 1)
                  } else {
                    joinType = HashJoin
                    size = max(costAndPlan1._2, costAndPlan2._2)
                    cost = size + costAndPlan1._1 + costAndPlan2._1
                  }
                }
                if (cost < bestPlan._1) {
                  bestPlan = (cost.toInt, size.toInt, new JoinOpNode(costAndPlan2._3, costAndPlan1._3, join._3, joinType, costAndPlan2._3 match {
                    case ScanOpNode(_, name, _) => name
                    case _                      => ""
                  }, table))
                }
              }
            }
          }
          joinCosts.put(subset.toList.sorted, bestPlan)
          //System.out.println(subset.toList.sorted + "--> cost: " + bestPlan._1 + " size: " + bestPlan._2)
        }
      }
    }

    joinCosts.get(subsets(tables.size).head.toList.sorted) match {
      case Some(v) => {
        v
      }
      case None => throw new Exception("Error while loading best join plan")
    }
  }
}