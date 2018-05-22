package ch.epfl.data
package dblab
package frontend
package optimizer

import java.util.function.BinaryOperator

import parser.OperatorAST._
import frontend.analyzer._
import schema._
import ch.epfl.data.dblab.frontend.parser.SQLAST._

import scala.math._
import scala.collection.mutable.ArrayBuffer

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
      case SelectOpNode(parent, _, _) => getJoinList(parent, result)
      case AggOpNode(parent, _, _, _) => getJoinList(parent, result)
      case MapOpNode(parent, _) => getJoinList(parent, result)
      case OrderByNode(parent, _) => getJoinList(parent, result)
      case PrintOpNode(parent, _, _) => getJoinList(parent, result)
      case SubqueryNode(parent) => result //getJoinList(parent, result)
      case SubquerySingleResultNode(parent) => result //getJoinList(parent, result)
      case ProjectOpNode(parent, _, _) => getJoinList(parent, result)
      case ViewOpNode(parent, _, _) => getJoinList(parent, result)
      case UnionAllOpNode(top, bottom) => getJoinList(top) ::: getJoinList(bottom, result)
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
        case None => throw new Exception("Can't find table " + e1.qualifier)
      }
      val tableName2 = e2.qualifier match {
        case Some(v) => v
        case None => throw new Exception("Can't find table " + e2.qualifier)
      }
      (tableName1, tableName2, schema.stats.getCardinality(tableName1).toInt, schema.stats.getCardinality(tableName2).toInt)

    case NotEquals(e1: FieldIdent, e2: FieldIdent) =>
      val tableName1 = e1.qualifier match {
        case Some(v) => v
        case None => throw new Exception("Can't find table " + e1.qualifier)
      }
      val tableName2 = e2.qualifier match {
        case Some(v) => v
        case None => throw new Exception("Can't find table " + e2.qualifier)
      }
      (tableName1, tableName2, schema.stats.getCardinality(tableName1).toInt, schema.stats.getCardinality(tableName2).toInt)

    case Equals(e1: FieldIdent, e2: FieldIdent) =>
      val tableName1 = e1.qualifier match {
        case Some(v) => v
        case None => throw new Exception("Can't find table " + e1.qualifier)
      }
      val tableName2 = e2.qualifier match {
        case Some(v) => v
        case None => throw new Exception("Can't find table " + e2.qualifier)
      }
      (tableName1, tableName2, schema.stats.getCardinality(tableName1).toInt, schema.stats.getCardinality(tableName2).toInt)
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

  def getAliases(node: OperatorNode, result: List[(String, String)] = Nil): List[(String, String)] = node match {
    case ScanOpNode(table, _, qualifier) => qualifier match {
      case Some(v) => {
        val res = (v, table.name)
        List(res)
      }
      case None => {
        val res = (table.name, table.name)
        List(res)
      }
    }
    case SelectOpNode(parent, _, _) => getAliases(parent, result)
    case JoinOpNode(left, right, _, _, _, _) => getAliases(left, result) ::: getAliases(right)
    case AggOpNode(parent, _, _, _) => getAliases(parent, result)
    case MapOpNode(parent, _) => getAliases(parent, result)
    case OrderByNode(parent, _) => getAliases(parent, result)
    case PrintOpNode(parent, _, _) => getAliases(parent, result)
    case SubqueryNode(_) => List(("TMP_VIEW", "TMP_VIEW"))
    case SubquerySingleResultNode(parent) => getAliases(parent, result)
    case ProjectOpNode(parent, _, _) => getAliases(parent, result)
    case ViewOpNode(parent, _, _) => getAliases(parent, result)
    case UnionAllOpNode(top, bottom) => getAliases(top, result) ::: getAliases(bottom)
  }

  def attachSelections(node: OperatorNode, selections: scala.collection.mutable.HashMap[String, Expression]): OperatorNode = node match {
    case ScanOpNode(table, _, _) => selections.get(table.name) match {
      case Some(expr) => SelectOpNode(node, expr, false)
      case None => node
    }
    case JoinOpNode(left, right, clause, joinType, leftAlias, rightAlias) => JoinOpNode(attachSelections(left, selections), attachSelections(right, selections), clause, joinType, leftAlias, rightAlias)
    case SelectOpNode(parent, cond, isHavingClause) => SelectOpNode(attachSelections(parent, selections), cond, isHavingClause)
    case AggOpNode(parent, aggs, gb, aggNames) => {
      val filter = new scala.collection.mutable.HashMap[String, Expression]
      aggNames.map(a => selections.get(a) match {
        case Some(expr: Expression) => filter.put(a, expr)
        case None =>
      })
      if (filter.isEmpty) AggOpNode(attachSelections(parent, selections), aggs, gb, aggNames)
      else {
        val condition = filter.values.tail.foldLeft(filter.values.head)((a, b) => And(a, b))
        SelectOpNode(AggOpNode(attachSelections(parent, selections), aggs, gb, aggNames), condition, false)
      }
    }

    case MapOpNode(parent, mapIndices) => MapOpNode(attachSelections(parent, selections), mapIndices)
    case OrderByNode(parent, orderBy) => OrderByNode(attachSelections(parent, selections), orderBy)
    case PrintOpNode(parent, projNames, limit) => PrintOpNode(attachSelections(parent, selections), projNames, limit)
    case SubqueryNode(_) => selections.get("TMP_VIEW") match {
      case Some(expr) => SelectOpNode(node, expr, false)
      case None => node
    }
    case SubquerySingleResultNode(_) => node
    case ProjectOpNode(parent, projNames, origFieldNames) => ProjectOpNode(attachSelections(parent, selections), projNames, origFieldNames)
    case ViewOpNode(parent, projNames, name) => ViewOpNode(attachSelections(parent, selections), projNames, name)
    case UnionAllOpNode(top, bottom) => UnionAllOpNode(attachSelections(top, selections), attachSelections(bottom, selections))
  }

  def attachAggregation(node: OperatorNode, aggregation: AggOpNode, tables: Set[String]): OperatorNode = node match {
    case ScanOpNode(_, _, _) => AggOpNode(node, aggregation.aggs, aggregation.gb, aggregation.aggNames)
    case JoinOpNode(left, right, clause, joinType, leftAlias, rightAlias) => {
      val tablesLeft = left.toList().collect {
        case scan: ScanOpNode => scan
      }.map(_.table.name).toSet
      val tableRight = {
        val nodeList = right.toList()
        if (nodeList.collectFirst {
          case ss: SubquerySingleResultNode => ss
          case s: SubqueryNode => s
        }.isDefined) {
          "TMP_VIEW"
        } else {
          nodeList.collectFirst {
            case scan: ScanOpNode => scan
          } match {
            case Some(v) => v.table.name
            case None => throw new Exception("Join contains no tables on the right!")
          }
        }
      }
      //System.out.println("Left tables: " + tablesLeft + " right table: " + tableRight)
      if (tables.subsetOf(tablesLeft) && !tables.contains(tableRight)) JoinOpNode(attachAggregation(left, aggregation, tables), right, clause, joinType, leftAlias, rightAlias)
      else AggOpNode(node, aggregation.aggs, aggregation.gb, aggregation.aggNames)
    }
    case SelectOpNode(_, _, _) => AggOpNode(node, aggregation.aggs, aggregation.gb, aggregation.aggNames)
    case AggOpNode(parent, _, _, _) => attachAggregation(parent, aggregation, tables)
    case MapOpNode(parent, mapIndices) => MapOpNode(attachAggregation(parent, aggregation, tables), mapIndices)
    case OrderByNode(parent, _) => attachAggregation(parent, aggregation, tables)
    case PrintOpNode(parent, projNames, limit) => PrintOpNode(attachAggregation(parent, aggregation, tables), projNames, limit)
    case SubqueryNode(_) => node
    case SubquerySingleResultNode(_) => node
    case ProjectOpNode(parent, projNames, origFieldNames) => ProjectOpNode(attachAggregation(parent, aggregation, tables), projNames, origFieldNames)
    case ViewOpNode(parent, projNames, name) => ViewOpNode(attachAggregation(parent, aggregation, tables), projNames, name)
    case UnionAllOpNode(_, _) => AggOpNode(node, aggregation.aggs, aggregation.gb, aggregation.aggNames)
  }

  def attachOrder(tree: OperatorNode, orderNode: OrderByNode): OperatorNode = {
    OrderByNode(tree, orderNode.orderBy)
  }

  def attachMap(tree: OperatorNode, mapNode: MapOpNode): OperatorNode = {
    MapOpNode(tree, mapNode.mapIndices)
  }

  def attachProjection(tree: OperatorNode, projectionNode: ProjectOpNode): OperatorNode = {
    ProjectOpNode(tree, projectionNode.projNames, projectionNode.origFieldNames)
  }

  //fix the fieldIdent cases
  def getAggTables(node: AggOpNode): Set[String] = {
    def getFieldIdents(e: Expression, res: List[FieldIdent] = Nil): List[FieldIdent] = e match {
      case e1: FieldIdent => e1 :: res
      case Or(e1, e2) => getFieldIdents(e1, res) ::: getFieldIdents(e2)
      case And(e1, e2) => getFieldIdents(e1, res) ::: getFieldIdents(e2)
      case Equals(e1, e2) => getFieldIdents(e1, res) ::: getFieldIdents(e2)
      case NotEquals(e1, e2) => getFieldIdents(e1, res) ::: getFieldIdents(e2)
      case LessOrEqual(e1, e2) => getFieldIdents(e1, res) ::: getFieldIdents(e2)
      case LessThan(e1, e2) => getFieldIdents(e1, res) ::: getFieldIdents(e2)
      case GreaterOrEqual(e1, e2) => getFieldIdents(e1, res) ::: getFieldIdents(e2)
      case GreaterThan(e1, e2) => getFieldIdents(e1, res) ::: getFieldIdents(e2)
      case Like(e1, e2) => getFieldIdents(e1, res) ::: getFieldIdents(e2)
      case Add(e1, e2) => getFieldIdents(e1, res) ::: getFieldIdents(e2)
      case Subtract(e1, e2) => getFieldIdents(e1, res) ::: getFieldIdents(e2)
      case Multiply(e1, e2) => getFieldIdents(e1, res) ::: getFieldIdents(e2)
      case Divide(e1, e2) => getFieldIdents(e1, res) ::: getFieldIdents(e2)
      case StringConcat(e1, e2) => getFieldIdents(e1, res) ::: getFieldIdents(e2)
      case CountExpr(e1) => getFieldIdents(e1, res)
      case Sum(e1) => getFieldIdents(e1, res)
      case Avg(e1) => getFieldIdents(e1, res)
      case Min(e1) => getFieldIdents(e1, res)
      case Max(e1) => getFieldIdents(e1, res)
      case Year(e1) => getFieldIdents(e1, res)
      case Upper(e1) => getFieldIdents(e1, res)
      case Distinct(e1) => getFieldIdents(e1, res)
      case AllExp(e1) => getFieldIdents(e1, res)
      case SomeExp(e1) => getFieldIdents(e1, res)
      case Not(e1) => getFieldIdents(e1, res)
      case Abs(e1) => getFieldIdents(e1, res)
      case UnaryPlus(e1) => getFieldIdents(e1, res)
      case UnaryMinus(e1) => getFieldIdents(e1, res)
      case Exists(e1) => getFieldIdents(e1, res)
      case Case(e1, e2, e3) => getFieldIdents(e1, res) ::: getFieldIdents(e2) ::: getFieldIdents(e3)
      case In(e1, l2) => getFieldIdents(e1, res)
      case InList(e1, l2) => getFieldIdents(e1, res)
      case FunctionExp(name, inputs) => res
      case Substring(e1, e2, e3) => getFieldIdents(e1, res) ::: getFieldIdents(e2) ::: getFieldIdents(e3)
      case _ => res
    }
    {
      node.aggs.map(a => getFieldIdents(a)).flatten ++ node.gb.map(_._1)
    }.collect {
      case f: FieldIdent => f.qualifier match {
        case Some(v) => v
        case None => f.name
      }
    }.toSet
  }

  def selinger(queryNode: OperatorNode, costingPlan: PlanCosting): OperatorNode = {
    queryNode.toList().collect[JoinOpNode].map

    /*
    System.out.println("Filters: " + costingPlan.tableFilters)
    System.out.println("Joins: " + costingPlan.tableJoins)
    System.out.println("Filters in one expression: " + costingPlan.filterExprs)*/

    val tableList = getJoinList(queryNode)
    var plan = queryNode

    if (tableList.isEmpty) {
      costingPlan.getSubquery(queryNode) match {
        case Some(v) => {
          val costing = new PlanCosting(schema, QueryPlanTree(v, new ArrayBuffer()))
          SubqueryNode(selinger(v, costing))
        }
        case None => {
          //Think whether to return a full subquery or not
          plan = simplifyJoinTree(queryNode)
        }
      }
    } else {

      val tables = tableList.flatMap(join => List(join._1, join._2)).toSet //list of tables in the join
      val subsets = tables.subsets.toList.groupBy(_.size).filter(_._1 != 0) //map of subset.size -> list(subsets_of_this_size)
      val joinWays = tableList.flatMap(join => List((join._1, join._2, join._5, join._6), (join._2, join._1, join._5, join._6))).groupBy(_._1) //map of table -> list(table, table2, expression, joinType)
      val joinCosts = new scala.collection.mutable.HashMap[List[String], (Long, Long, OperatorNode)](); //maps of table_list -> (best cost, size, joinPlan)
      val aliases = getAliases(queryNode).toMap

      for (i <- 1 to tables.size) {
        for (subset <- subsets(i)) {
          //when a subset is only one table
          if (i == 1) {
            val tableName = aliases.get(subset.head) match {
              case Some(v) => v
              case None => throw new Exception(subset.head + " table doesn't exist!")
            }
            schema.findTable(tableName) match {
              case Some(v) => {
                val table = v
                val size = schema.stats.getCardinality(tableName)
                val tableSelectivity = costingPlan.filterExprs.get(v.name) match {
                  case Some(a) =>
                    schema.stats.getFilterSelectivity(a)
                  case None => 1
                }
                joinCosts.put(subset.toList.sorted, ((size * tableSelectivity).toInt, (size * tableSelectivity).toInt, new ScanOpNode(table, subset.head, None)))
              }
              case None => costingPlan.getSubquery(queryNode) match {
                case Some(v) => {
                  if (tableName.equals("TMP_VIEW")) {
                    val costing = new PlanCosting(schema, QueryPlanTree(v, new ArrayBuffer()))
                    val subquery = selinger(v, costing)
                    joinCosts.put(subset.toList.sorted, (costing.cost(subquery).toLong, costing.size(subquery).toLong, SubqueryNode(subquery)))
                  } else None
                }
                case None => throw new Exception(s"Can't find table: $tableName")
              }
            }
            //System.out.println(s"Best plan for $tableName is cost: $size and size: $size")
          } else {
            var dummyTableName = aliases.get(subset.head) match {
              case Some(v) => v
              case None => throw new Exception(subset.head + " table doesn't exist!")
            }
            if (dummyTableName.equals("TMP_VIEW")) dummyTableName = (subset - dummyTableName).head
            val dummyTable = schema.findTable(dummyTableName) match {
              case Some(v) => v
              case None => throw new Exception("Can't find table: " + dummyTableName)
            }
            var bestPlan = (Long.MaxValue, Long.MaxValue, new ScanOpNode(dummyTable, subset.head + "_dummy", None): OperatorNode)

            for (table <- subset) {
              val subsetToConsider = subset - table

              //get best plan and cost for subset to join to
              val costAndPlan2 = joinCosts.get(subsetToConsider.toList.sorted) match {
                case Some(v) => v
                case None => throw new Exception(s"No key: " + subsetToConsider.toList.sorted + " exists in the joinCosts HashMap")
              }
              if (costAndPlan2._1 != Long.MaxValue) {
                for (join <- joinWays(table)) {
                  if (subsetToConsider.contains(join._2)) {

                    //get best plan and cost for one table
                    val costAndPlan1 = joinCosts.get(List(table)) match {
                      case Some(v) => v
                      case None => throw new Exception(s"No key: $table exists in the joinCosts HashMap")
                    }

                    var joinType: JoinType = HashJoin
                    var size = max(costAndPlan1._2, costAndPlan2._2)
                    var cost = size + costAndPlan1._1 + costAndPlan2._1
                    var plan = new JoinOpNode(costAndPlan2._3, costAndPlan1._3, join._3, joinType, costAndPlan2._3 match {
                      case ScanOpNode(_, name, _) => name
                      case _ => ""
                    }, table)

                    if (costAndPlan2._1 != Long.MaxValue) {

                      val (column1, column2) = costingPlan.getJoinColumns(join._3, table, join._2) match {
                        case Some(v) => v
                        case _ => throw new Exception("Expression " + join._3 + " contains no column names")
                      }

                      if (!table.equals("TMP_VIEW") && costingPlan.isPrimaryKey(column1, table)) {
                        size = costAndPlan2._2
                        val cost1 = costAndPlan2._1 + costingPlan.lambda * costAndPlan2._2 * max(size / costAndPlan2._2, 1)

                        //System.out.println(column1 + " is a primary key (1st if) of " + table)
                        //System.out.println("Hash cost: " + cost + " INL cost: " + cost1)
                        if (cost1 < cost) {
                          joinType = IndexNestedLoopJoin
                          cost = cost1
                          plan = new JoinOpNode(costAndPlan2._3, costAndPlan1._3, join._3, joinType, costAndPlan2._3 match {
                            case ScanOpNode(_, name, _) => name
                            case _ => ""
                          }, table)
                        }
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
      plan = joinCosts.get(subsets(tables.size).head.toList.sorted) match {
        case Some(v) => v._3
        case None => throw new Exception("Error while loading best join plan")
      }
    }
    restoreQuery(queryNode, costingPlan, plan)
  }

  def restoreQuery(modelPlan: OperatorNode, costingPlan: PlanCosting, optimizedPlan: OperatorNode): OperatorNode = {
    val aggregation = costingPlan.getAggregation()
    val order = costingPlan.getOrder()
    val map = costingPlan.getMap()
    val projection = costingPlan.getProjection()
    val selections = costingPlan.filterExprs
    val resultWithAgg = aggregation match {
      case Some(v) => {
        val ag = v.asInstanceOf[AggOpNode]
        val tabs = getAggTables(ag)
        System.out.println("Aggregation aggNames: " + ag.aggNames + " gb: " + ag.gb + " aggs " + ag.aggs + " tables: " + tabs)
        attachAggregation(optimizedPlan, ag, tabs)
      }
      case None => optimizedPlan
    }
    val resultWithOrder = order match {
      case Some(v) => attachOrder(resultWithAgg, v.asInstanceOf[OrderByNode])
      case None => resultWithAgg
    }
    val resultWithMap = map match {
      case Some(v) => attachMap(resultWithOrder, v.asInstanceOf[MapOpNode])
      case None => resultWithOrder
    }
    val resultWithProjection = projection match {
      case Some(v) => attachProjection(resultWithMap, v.asInstanceOf[ProjectOpNode])
      case None => resultWithMap
    }
    val resultWithSelections = attachSelections(resultWithProjection, selections)
    resultWithSelections
  }



  def optimize(queryPlan: QueryPlanTree): QueryPlanTree = {
    val planCosting = new PlanCosting(schema, queryPlan)
    val result = selinger(queryPlan.rootNode, planCosting)
    val resultTree = QueryPlanTree(result, queryPlan.views map (x => new ViewOpNode(selinger(x, planCosting), x.projNames, x.name)))
    //System.out.println(result)
    resultTree
  }

}