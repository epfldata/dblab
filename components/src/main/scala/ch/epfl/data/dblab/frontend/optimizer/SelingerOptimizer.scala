package ch.epfl.data
package dblab
package frontend
package optimizer

import parser.OperatorAST._
import frontend.analyzer._
import schema._
import ch.epfl.data.dblab.frontend.parser.SQLAST._

import scala.math._
import scala.collection.mutable.ArrayBuffer

/**
 * Selinger optimizer performs the Selinger algorithm on the joins of
 * provided query plan. It reorders joins, pushes up selections and aggregations.
 *
 * @author Michal Pleskowicz
 */
class SelingerOptimizer(schema: Schema) extends QueryPlanOptimizer {

  case class JoinPlan(cost: Long, size: Long, plan: OperatorNode)

  def getJoinNodes(node: OperatorNode, result: List[(String, String, OperatorNode)] = Nil): List[(String, String, OperatorNode)] = {
    node match {
      case ScanOpNode(_, _, _) => result
      case JoinOpNode(left, right, clause, _, _, _) => {
        val names = getFieldIdents(clause)
        System.out.println("Names: " + names)
        val tableNames = if (names.isEmpty) {
          val leftNode = left.toList().collectFirst { case s: ScanOpNode => s } match {
            case Some(v) => v.asInstanceOf[ScanOpNode]
            case None    => throw new Exception("No ScanOpNode found")
          }
          val rightNode = right.toList().collectFirst { case s: ScanOpNode => s } match {
            case Some(v) => v.asInstanceOf[ScanOpNode]
            case None    => throw new Exception("No ScanOpNode found")
          }
          List(leftNode.qualifier match {
            case Some(v) => v
            case None    => leftNode.scanOpName
          }, rightNode.qualifier match {
            case Some(v) => v
            case None    => leftNode.scanOpName
          })
        } else getTableNames(names)
        System.out.println("tableNames: " + tableNames)
        val thisJoin = (tableNames(0), tableNames(1), node)
        getJoinNodes(left, thisJoin :: result)
      }
      case SelectOpNode(parent, _, _)  => getJoinNodes(parent, result)
      case AggOpNode(parent, _, _, _)  => getJoinNodes(parent, result)
      case MapOpNode(parent, _)        => getJoinNodes(parent, result)
      case OrderByNode(parent, _)      => getJoinNodes(parent, result)
      case PrintOpNode(parent, _, _)   => getJoinNodes(parent, result)
      case SubqueryNode(_)             => result //getJoinList(parent, result)
      case SubquerySingleResultNode(_) => result //getJoinList(parent, result)
      case ProjectOpNode(parent, _, _) => getJoinNodes(parent, result)
      case ViewOpNode(parent, _, _)    => getJoinNodes(parent, result)
      case UnionAllOpNode(top, bottom) => getJoinNodes(top) ::: getJoinNodes(bottom, result)
    }
  }

  def getTableNames(fieldIdents: Seq[FieldIdent]): Seq[String] = {
    fieldIdents.map { f =>
      f.qualifier match {
        case Some(v) => v
        case None    => throw new Exception(s"%f doesn't contain a table name")
      }
    }
  }

  def getTableNames(expression: Expression): Seq[String] = expression match {
    case And(e1, e2) => getTableNames(e1)
    case Equals(e1: FieldIdent, e2: FieldIdent) => getTableNames(Seq(e1, e2))
    case NotEquals(e1: FieldIdent, e2: FieldIdent) => getTableNames(Seq(e1, e2))
    case LessThan(e1: FieldIdent, e2: FieldIdent) => getTableNames(Seq(e1, e2))
    case LessOrEqual(e1: FieldIdent, e2: FieldIdent) => getTableNames(Seq(e1, e2))
    case GreaterOrEqual(e1: FieldIdent, e2: FieldIdent) => getTableNames(Seq(e1, e2))
    case GreaterThan(e1: FieldIdent, e2: FieldIdent) => getTableNames(Seq(e1, e2))
    case _ => Seq()
  }

  def reorderTables(expression: Expression): Expression = expression match {
    case And(e1, e2)                                    => And(reorderTables(e1), reorderTables(e2))
    case Equals(e1: FieldIdent, e2: FieldIdent)         => Equals(e2, e1)
    case NotEquals(e1: FieldIdent, e2: FieldIdent)      => NotEquals(e2, e1)
    case LessThan(e1: FieldIdent, e2: FieldIdent)       => GreaterOrEqual(e2, e1)
    case LessOrEqual(e1: FieldIdent, e2: FieldIdent)    => GreaterThan(e2, e1)
    case GreaterOrEqual(e1: FieldIdent, e2: FieldIdent) => LessThan(e2, e1)
    case GreaterThan(e1: FieldIdent, e2: FieldIdent)    => LessOrEqual(e2, e1)
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

  def getTableAliases(node: OperatorNode, result: List[(String, String)] = Nil): List[(String, String)] = node match {
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
    case SelectOpNode(parent, _, _)          => getTableAliases(parent, result)
    case JoinOpNode(left, right, _, _, _, _) => getTableAliases(left, result) ::: getTableAliases(right)
    case AggOpNode(parent, _, _, _)          => getTableAliases(parent, result)
    case MapOpNode(parent, _)                => getTableAliases(parent, result)
    case OrderByNode(parent, _)              => getTableAliases(parent, result)
    case PrintOpNode(parent, _, _)           => getTableAliases(parent, result)
    case SubqueryNode(_)                     => List(("TMP_VIEW", "TMP_VIEW"))
    case SubquerySingleResultNode(parent)    => getTableAliases(parent, result)
    case ProjectOpNode(parent, _, _)         => getTableAliases(parent, result)
    case ViewOpNode(parent, _, _)            => getTableAliases(parent, result)
    case UnionAllOpNode(top, bottom)         => getTableAliases(top, result) ::: getTableAliases(bottom)
  }

  def attachSelections(node: OperatorNode, selections: scala.collection.mutable.HashMap[String, Expression]): OperatorNode = node match {
    case ScanOpNode(table, _, _) => selections.get(table.name) match {
      case Some(expr) => SelectOpNode(node, expr, false)
      case None       => node
    }
    case JoinOpNode(left, right, clause, joinType, leftAlias, rightAlias) => JoinOpNode(attachSelections(left, selections), attachSelections(right, selections), clause, joinType, leftAlias, rightAlias)
    case SelectOpNode(parent, cond, isHavingClause)                       => SelectOpNode(attachSelections(parent, selections), cond, isHavingClause)
    case AggOpNode(parent, aggs, gb, aggNames) => {
      val filter = new scala.collection.mutable.HashMap[String, Expression]
      aggNames.map(a => selections.get(a) match {
        case Some(expr: Expression) => filter.put(a, expr)
        case None                   =>
      })
      if (filter.isEmpty) AggOpNode(attachSelections(parent, selections), aggs, gb, aggNames)
      else {
        val condition = filter.values.tail.foldLeft(filter.values.head)((a, b) => And(a, b))
        SelectOpNode(AggOpNode(attachSelections(parent, selections), aggs, gb, aggNames), condition, false)
      }
    }

    case MapOpNode(parent, mapIndices)         => MapOpNode(attachSelections(parent, selections), mapIndices)
    case OrderByNode(parent, orderBy)          => OrderByNode(attachSelections(parent, selections), orderBy)
    case PrintOpNode(parent, projNames, limit) => PrintOpNode(attachSelections(parent, selections), projNames, limit)
    case SubqueryNode(_) => selections.get("TMP_VIEW") match {
      case Some(expr) => SelectOpNode(node, expr, false)
      case None       => node
    }
    case SubquerySingleResultNode(_)                      => node
    case ProjectOpNode(parent, projNames, origFieldNames) => ProjectOpNode(attachSelections(parent, selections), projNames, origFieldNames)
    case ViewOpNode(parent, projNames, name)              => ViewOpNode(attachSelections(parent, selections), projNames, name)
    case UnionAllOpNode(top, bottom)                      => UnionAllOpNode(attachSelections(top, selections), attachSelections(bottom, selections))
  }

  def attachAggregationNode(node: OperatorNode, aggregation: AggOpNode, tables: Set[String]): OperatorNode = node match {
    case ScanOpNode(_, _, _) => AggOpNode(node, aggregation.aggs, aggregation.gb, aggregation.aggNames)
    case JoinOpNode(left, right, clause, joinType, leftAlias, rightAlias) => {
      /*val tablesLeft = left.toList().collect {
        case scan: ScanOpNode => scan
      }.map(_.table.name).toSet
      val tableRight = {
        val nodeList = right.toList()
        if (nodeList.collectFirst {
          case ss: SubquerySingleResultNode => ss
          case s: SubqueryNode              => s
        }.isDefined) {
          "TMP_VIEW"
        } else {
          nodeList.collectFirst {
            case scan: ScanOpNode => scan
          } match {
            case Some(v) => v.table.name
            case None    => throw new Exception("Join contains no tables on the right!")
          }
        }
      }*/
      //System.out.println("Left tables: " + tablesLeft + " right table: " + tableRight)
      //if (tables.subsetOf(tablesLeft) && !tables.contains(tableRight)) JoinOpNode(attachAggregationNode(left, aggregation, tables), right, clause, joinType, leftAlias, rightAlias)
      //else AggOpNode(node, aggregation.aggs, aggregation.gb, aggregation.aggNames)
      //Check whether the expression contains any of the tabless
      val tablesInClause = getTableNames(clause).filter(tables.contains(_))
      if (tablesInClause.size > 0) AggOpNode(node, aggregation.aggs, aggregation.gb, aggregation.aggNames)
      else JoinOpNode(attachAggregationNode(left, aggregation, tables), right, clause, joinType, leftAlias, rightAlias)
    }
    case SelectOpNode(_, _, _)                            => AggOpNode(node, aggregation.aggs, aggregation.gb, aggregation.aggNames)
    case AggOpNode(parent, _, _, _)                       => attachAggregationNode(parent, aggregation, tables)
    case MapOpNode(parent, mapIndices)                    => MapOpNode(attachAggregationNode(parent, aggregation, tables), mapIndices)
    case OrderByNode(parent, _)                           => attachAggregationNode(parent, aggregation, tables)
    case PrintOpNode(parent, projNames, limit)            => PrintOpNode(attachAggregationNode(parent, aggregation, tables), projNames, limit)
    case SubqueryNode(_)                                  => AggOpNode(node, aggregation.aggs, aggregation.gb, aggregation.aggNames)
    case SubquerySingleResultNode(_)                      => AggOpNode(node, aggregation.aggs, aggregation.gb, aggregation.aggNames)
    case ProjectOpNode(parent, projNames, origFieldNames) => ProjectOpNode(attachAggregationNode(parent, aggregation, tables), projNames, origFieldNames)
    case ViewOpNode(parent, projNames, name)              => ViewOpNode(attachAggregationNode(parent, aggregation, tables), projNames, name)
    case UnionAllOpNode(_, _)                             => AggOpNode(node, aggregation.aggs, aggregation.gb, aggregation.aggNames)
  }

  def attachOrderNode(tree: OperatorNode, orderNode: OrderByNode): OperatorNode = {
    OrderByNode(tree, orderNode.orderBy)
  }

  def attachMapNode(tree: OperatorNode, mapNode: MapOpNode): OperatorNode = {
    MapOpNode(tree, mapNode.mapIndices)
  }

  def attachProjectionNode(tree: OperatorNode, projectionNode: ProjectOpNode): OperatorNode = {
    ProjectOpNode(tree, projectionNode.projNames, projectionNode.origFieldNames)
  }

  def attachPrintNode(tree: OperatorNode, printNode: PrintOpNode): OperatorNode = {
    PrintOpNode(tree, printNode.projNames, printNode.limit)
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

  //fix the fieldIdent cases
  def getAggregationTables(node: AggOpNode): Set[String] = {
    val fieldIdents = node.aggs.flatMap(a => getFieldIdents(a)) ++ node.gb.map(_._1).flatMap(e => getFieldIdents(e))
    getTableNames(fieldIdents.asInstanceOf[List[FieldIdent]]).toSet
  }

  def selinger(queryNode: OperatorNode, costingPlan: PlanCosting): OperatorNode = {
    var plan = queryNode
    val nodeList = getJoinNodes(queryNode)

    if (nodeList.isEmpty) {
      /*costingPlan.getSubquery(queryNode).fold(plan = simplifyJoinTree(queryNode))(v => {val costing = new PlanCosting(schema, QueryPlanTree(v, new ArrayBuffer()))
                                                                                          SubqueryNode(selinger(v, costing))})*/
      costingPlan.getSubquery(queryNode) match {
        case Some(v) => {
          System.out.println("Found a subquery!")
          val costing = new PlanCosting(schema, QueryPlanTree(v, new ArrayBuffer()))
          plan = SubqueryNode(selinger(v, costing))
        }
        case None => {
          plan = simplifyJoinTree(queryNode)
        }
      }
    } else {
      val tables = nodeList.flatMap(e => Seq(e._1, e._2)).toSet
      val subsets = tables.subsets.toList.groupBy(_.size).filter(_._1 != 0) //map of size -> List[subets of that size]
      val joinWays = nodeList.flatMap(join => List((join._1, join._2, join._3), (join._2, join._1, join._3))).groupBy(_._1) //map of tableName -> List of (tableName, tableName2, joinNode)

      val joinCosts = new scala.collection.mutable.HashMap[List[String], JoinPlan](); //maps of table_list -> (best cost, size, joinPlan)
      val aliases = getTableAliases(queryNode).toMap
      System.out.println("Tables: " + tables)
      for (i <- 1 to tables.size) {
        for (subset <- subsets(i)) {
          //when a subset is only one table
          if (i == 1) {
            val tableName = aliases.getOrElse(subset.head, throw new Exception(subset.head + " table doesn't exist!"))
            System.out.println("Table name is " + tableName)
            schema.findTable(tableName) match {
              case Some(v) => {
                val table = v
                val size = schema.stats.getCardinality(tableName)
                val tableSelectivity = costingPlan.filterExprs.get(v.name) match {
                  case Some(a) => schema.stats.getFilterSelectivity(a)
                  case None    => 1
                }
                joinCosts.put(subset.toList.sorted, new JoinPlan(size.toLong, (size * tableSelectivity).toLong, new ScanOpNode(table, subset.head, None)))
                System.out.println(s"Best plan for $tableName is cost: $size and size: $size")
              }
              case None =>
                if (tableName.equals("TMP_VIEW")) {
                  costingPlan.getSubquery(queryNode) match {
                    case Some(v) =>
                      val costing = new PlanCosting(schema, QueryPlanTree(v, new ArrayBuffer()))
                      val subquery = selinger(v, costing)
                      joinCosts.put(subset.toList, new JoinPlan(costing.cost(subquery).toLong, costing.size(subquery).toLong, SubqueryNode(subquery)))
                    case None => throw new Exception(s"Can't find table: $tableName")
                  }
                } else None
            }
          } else {
            var bestCost = Long.MaxValue
            for (table <- subset) {
              val subsetToConsider = subset - table
              joinCosts.get(subsetToConsider.toList.sorted) match {
                //get if a subset is possible to create with joins
                case Some(leftPlan) => {
                  for (join <- joinWays(table)) {
                    if (subsetToConsider.contains(join._2)) {
                      //get best plan and cost for one table
                      val rightPlan = joinCosts.getOrElse(List(table), throw new Exception(s"No key: $table exists in the joinCosts HashMap"))
                      val bestJoin = getBestJoinPlan(leftPlan, rightPlan, join, costingPlan)

                      if (bestJoin.cost < bestCost) {
                        bestCost = bestJoin.cost
                        joinCosts.put(subset.toList.sorted, JoinPlan(bestJoin.cost, bestJoin.size, bestJoin.plan))
                        System.out.println("Found a best plan for: " + subset)
                      }
                    }
                  }
                }
                case None =>
              }
            }
          }
        }
      }
      plan = joinCosts.getOrElse(subsets(tables.size).head.toList.sorted, throw new Exception("Error while loading best join plan")).plan
      System.out.println("Best plan is " + plan)
    }
    System.out.println("Best plan is " + plan)
    restoreQuery(queryNode, costingPlan, plan)
  }

  def getBestJoinPlan(left: JoinPlan, right: JoinPlan, join: (String, String, OperatorNode), costingPlan: PlanCosting, physical: Boolean = true): JoinPlan = {
    val table = join._1
    val joinNode = join._3.asInstanceOf[JoinOpNode]

    var joinType = if (physical) HashJoin else joinNode.joinType
    var size = max(right.size, left.size)
    var cost = size + right.cost + left.cost
    val (column1, column2) = costingPlan.getJoinColumns(joinNode.clause, table, join._2).getOrElse(throw new Exception("Expression " + join._3 + " contains no column names"))

    if (joinNode.clause.equals(Equals(IntLiteral(1), IntLiteral(1)))) {
      val plan = new JoinOpNode(left.plan, right.plan, joinNode.clause, joinType, left.plan match {
        case ScanOpNode(_, name, _) => name
        case _                      => ""
      }, table)
      cost = left.size * right.size
      size = left.size * right.size
      JoinPlan(cost, size, plan)
    } else {
      val tables = getTableNames(joinNode.clause)
      val clause = if (table.equals(tables(0))) reorderTables(joinNode.clause) else joinNode.clause

      var plan = new JoinOpNode(left.plan, right.plan, clause, joinType, left.plan match {
        case ScanOpNode(_, name, _) => name
        case _                      => ""
      }, table)

      if (!table.equals("TMP_VIEW") && costingPlan.isPrimaryKey(column1, table)) {
        size = left.size
        val cost1 = left.cost + costingPlan.lambda * left.size * max(size / left.cost, 1)
        //System.out.println(column1 + " is a primary key (1st if) of " + table)
        //System.out.println("Hash cost: " + cost + " INL cost: " + cost1)
        if (cost1 < cost) {
          joinType = IndexNestedLoopJoin
          cost = cost1
          plan = new JoinOpNode(left.plan, right.plan, clause, if (physical) IndexNestedLoopJoin else joinNode.joinType, left.plan match {
            case ScanOpNode(_, name, _) => name
            case _                      => ""
          }, table)
        }
      }
      JoinPlan(cost, size, plan)
    }
  }

  def restoreQuery(modelPlan: OperatorNode, costingPlan: PlanCosting, optimizedPlan: OperatorNode): OperatorNode = {
    val aggregation = costingPlan.getAggregation()
    val order = costingPlan.getOrder()
    val map = costingPlan.getMap()
    val projection = costingPlan.getProjection()
    val print = costingPlan.getPrint()
    val selections = costingPlan.filterExprs
    val generalSelection = costingPlan.generalFilters
    val plan = if (generalSelection.isEmpty) optimizedPlan else SelectOpNode(optimizedPlan, generalSelection.tail.foldLeft(generalSelection.head)((a, b) => And(a, b)), false)
    val resultWithAgg = aggregation match {
      case Some(v) => {
        val ag = v.asInstanceOf[AggOpNode]
        val tabs = getAggregationTables(ag)
        attachAggregationNode(plan, ag, tabs)
      }
      case None => optimizedPlan
    }
    val resultWithOrder = order.fold(resultWithAgg)(v => attachOrderNode(resultWithAgg, v.asInstanceOf[OrderByNode]))
    val resultWithMap = map.fold(resultWithOrder)(v => attachMapNode(resultWithOrder, v.asInstanceOf[MapOpNode]))
    val resultWithProjection = projection.fold(resultWithMap)(v => attachProjectionNode(resultWithMap, v.asInstanceOf[ProjectOpNode]))
    val resultWithPrint = print.fold(resultWithProjection)(v => attachPrintNode(resultWithProjection, v.asInstanceOf[PrintOpNode]))
    val resultWithSelections = attachSelections(resultWithPrint, selections)
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