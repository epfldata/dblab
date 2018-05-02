package ch.epfl.data
package dblab
package frontend
package optimizer

import parser.OperatorAST._
import frontend.analyzer._
import schema._
import ch.epfl.data.dblab.frontend.parser.SQLAST._
import scala.math._

/**
 * Selinger optimizer performs the Selinger algorithm on the joins of
 * provided query plan. It returns a tuple (cost, size, operator tree) of reordered joins and scans only,
 * dropping all the other nodes.
 *
 * @author Michal Pleskowicz
 */
class SelingerOptimizer(schema: Schema) extends QueryPlanOptimizer {

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
      case SubqueryNode(parent)             => result //getJoinList(parent, result)
      case SubquerySingleResultNode(parent) => result //getJoinList(parent, result)
      case ProjectOpNode(parent, _, _)      => getJoinList(parent, result)
      case ViewOpNode(parent, _, _)         => getJoinList(parent, result)
      case UnionAllOpNode(top, bottom)      => getJoinList(top) ::: getJoinList(bottom, result)
    }
  }

  def getTablesAndSizes(expression: Expression): (String, String, Int, Int) = expression match {
    /*case And(e1, e2) => (getTablesAndSizes(e1), getTablesAndSizes(e2)) match {
      case (Some(v1), Some(v2)) => v1
      case (Some(v), None)      => v
      case (None, Some(v))      => v
      case _                    => throw new Exception("Expression " + expression + " doesn't contain any tables")
    }*/
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

  def simplifyJoinTree(node: OperatorNode): OperatorNode = node match {
    case ScanOpNode(table, _, _) => node
    case SelectOpNode(parent, _, _) => simplifyJoinTree(parent)
    case JoinOpNode(left, right, clause, joinType, leftAlias, rightAlias) => JoinOpNode(simplifyJoinTree(left), simplifyJoinTree(right), clause, joinType, leftAlias, rightAlias)
    case AggOpNode(parent, _, _, _) => simplifyJoinTree(parent)
    case MapOpNode(parent, _) => simplifyJoinTree(parent)
    case OrderByNode(parent, _) => simplifyJoinTree(parent)
    case PrintOpNode(parent, _, _) => simplifyJoinTree(parent)
    case SubqueryNode(parent) => SubqueryNode(simplifyJoinTree(parent))
    case SubquerySingleResultNode(parent) => SubquerySingleResultNode(simplifyJoinTree(parent))
    case ProjectOpNode(parent, _, _) => simplifyJoinTree(parent)
    case ViewOpNode(parent, projNames, name) => ViewOpNode(simplifyJoinTree(parent), projNames, name)
    case UnionAllOpNode(top, bottom) => UnionAllOpNode(simplifyJoinTree(top), simplifyJoinTree(bottom))
  }

  def selinger(queryNode: OperatorNode, costingPlan: PlanCosting): OperatorNode = {

    val tableList = getJoinList(queryNode)

    if (tableList.isEmpty) {
      System.out.println("No joins to optimize!")
      return queryNode
    }

    val tables = tableList.flatMap(join => List(join._1, join._2)).toSet //list of tables to join
    val subsets = tables.subsets.toList.groupBy(_.size).filter(_._1 != 0) //map of subset.size -> list(subsets_of_this_size)
    val joinWays = tableList.flatMap(join => List((join._1, join._2, join._5, join._6), (join._2, join._1, join._5, join._6))).groupBy(_._1) //map of table -> list(table, table2, expression, joinType)
    val joinCosts = new scala.collection.mutable.HashMap[List[String], (Long, Long, OperatorNode)](); //maps of table_list -> (best cost, size, joinPlan)

    for (i <- 1 to tables.size) {
      for (subset <- subsets(i)) {
        //when a subset is only one table
        if (i == 1) {
          val tableName = subset.head
          schema.findTable(tableName) match {
            case Some(v) => {
              val table = v
              val size = schema.stats.getCardinality(tableName).toLong
              joinCosts.put(subset.toList.sorted, (size.toInt, size.toInt, new ScanOpNode(table, tableName, None)))
            }
            case None => costingPlan.getSubquery(queryNode) match {
              case Some(v) => {
                if (tableName.equals("TMP_VIEW")) joinCosts.put(subset.toList.sorted, (costingPlan.cost(v).toLong, costingPlan.size(v).toLong, SubqueryNode(selinger(v, costingPlan)))) else None
              }
              case None => throw new Exception(s"Can't find table: $tableName")
            }
          }
          //System.out.println(s"Best plan for $tableName is cost: $size and size: $size")
        } else {
          var dummyTableName = subset.head
          if (dummyTableName.equals("TMP_VIEW")) dummyTableName = (subset - dummyTableName).head
          val dummyTable = schema.findTable(dummyTableName) match {
            case Some(v) => v
            case None    => throw new Exception("Can't find table: " + dummyTableName)
          }
          var bestPlan = (Long.MaxValue, Long.MaxValue, new ScanOpNode(dummyTable, subset.head + "_dummy", None): OperatorNode)

          for (table <- subset) {
            val subsetToConsider = subset - table

            //get best plan and cost for subset to join to
            val costAndPlan2 = joinCosts.get(subsetToConsider.toList.sorted) match {
              case Some(v) => v
              case None    => throw new Exception(s"No key: " + subsetToConsider.toList.sorted + " exists in the joinCosts HashMap")
            }
            if (costAndPlan2._1 != Long.MaxValue) {
              for (join <- joinWays(table)) {
                if (subsetToConsider.contains(join._2)) {

                  //get best plan and cost for one table
                  val costAndPlan1 = joinCosts.get(List(table)) match {
                    case Some(v) => v
                    case None    => throw new Exception(s"No key: $table exists in the joinCosts HashMap")
                  }

                  var joinType: JoinType = HashJoin
                  var size = max(costAndPlan1._2, costAndPlan2._2)
                  var cost = size + costAndPlan1._1 + costAndPlan2._1
                  var plan = new JoinOpNode(costAndPlan2._3, costAndPlan1._3, join._3, joinType, costAndPlan2._3 match {
                    case ScanOpNode(_, name, _) => name
                    case _                      => ""
                  }, table)

                  if (costAndPlan2._1 != Long.MaxValue) {

                    val (column1, column2) = costingPlan.getJoinColumns(join._3, table, join._2) match {
                      case Some(v) => v
                      case _       => throw new Exception("Expression " + join._3 + " contains no column names")
                    }

                    if (costingPlan.isPrimaryKey(column1, table)) {
                      System.out.println(column1 + " is a primary key (1st if) of " + table)
                      val cost1 = costAndPlan2._1 + costingPlan.lambda * costAndPlan2._2 //* max(size / costAndPlan2._2, 1)
                      System.out.println("Hash cost: " + cost + " INL cost: " + cost1)
                      size = costAndPlan2._2
                      if (cost1 < cost) {
                        joinType = NestedLoopJoin
                        cost = cost1
                        plan = new JoinOpNode(costAndPlan2._3, costAndPlan1._3, join._3, joinType, costAndPlan2._3 match {
                          case ScanOpNode(_, name, _) => name
                          case _                      => ""
                        }, table)
                      }
                    }
                    if (costingPlan.isPrimaryKey(column2, join._2)) {
                      System.out.println(column2 + " is a primary key (2nd if) of " + join._2)
                      val cost2 = costAndPlan1._1 + costingPlan.lambda * costAndPlan1._2 //* max(size / costAndPlan1._2, 1)
                      System.out.println("Hash cost: " + cost + " INL cost: " + cost2)
                      size = costAndPlan1._2
                      if (cost2 < cost) {
                        joinType = NestedLoopJoin
                        cost = cost2
                        plan = new JoinOpNode(costAndPlan1._3, costAndPlan2._3, join._3, joinType, costAndPlan2._3 match {
                          case ScanOpNode(_, name, _) => name
                          case _                      => ""
                        }, table)
                      }
                    }
                    if (costingPlan.isPrimaryKey(column1, join._2)) {
                      System.out.println(column2 + " is a primary key (3rd if) of " + join._2)
                    }
                    if (costingPlan.isPrimaryKey(column2, table)) {
                      System.out.println(column2 + " is a primary key (4th if) of " + join._2)
                    }

                  }
                  if (cost < bestPlan._1) {
                    bestPlan = (cost.toInt, size.toInt, plan)
                  }
                }
              }
            }
          }
          joinCosts.put(subset.toList.sorted, bestPlan)
        }
      }
    }

    joinCosts.get(subsets(tables.size).head.toList.sorted) match {
      case Some(v) => v._3
      case None    => throw new Exception("Error while loading best join plan")
    }
  }

  def optimize(queryPlan: QueryPlanTree): QueryPlanTree = {
    val planCosting = new PlanCosting(schema, queryPlan)
    val resultTree = selinger(queryPlan.rootNode, planCosting)
    val result = QueryPlanTree(resultTree, queryPlan.views map (x => new ViewOpNode(selinger(x, planCosting), x.projNames, x.name)))
    System.out.println(result)
    result
  }

}