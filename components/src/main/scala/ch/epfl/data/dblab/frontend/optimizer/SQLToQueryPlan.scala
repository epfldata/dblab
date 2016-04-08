package ch.epfl.data
package dblab
package frontend
package optimizer

import schema._
import scala.reflect._
import parser._
import parser.OperatorAST._
import scala.reflect.runtime.{ universe => ru }
import ru._
import queryengine.GenericEngine

/**
 *
 * @author Yannis Klonatos
 */
class SQLToQueryPlan(schema: Schema) {

  val temporaryViewMap = new scala.collection.mutable.ArrayBuffer[ViewOpNode]
  private def viewNameDefined(viewName: String) = temporaryViewMap.find(tv => tv.name == viewName).isDefined
  private def getViewWithName(viewName: String) = temporaryViewMap.find(tv => tv.name == viewName).get

  def createScanOperators(sqlTree: SelectStatement) = {
    sqlTree.extractRelations.map(r => r match {
      case t: SQLTable if viewNameDefined(t.name) =>
        val vw = getViewWithName(t.name)
        (t.name -> vw)
      case t: SQLTable if schema.findTable(t.name).nonEmpty =>
        val table = schema.findTable(t.name).get
        val scanOpName = table.name + t.alias.getOrElse("")
        (scanOpName -> ScanOpNode(table, scanOpName, t.alias))
      case vw: View => (vw.alias -> getViewWithName(vw.alias))
    })
  }

  def parseJoinAliases(leftParent: OperatorNode, rightParent: OperatorNode): (String, String) = {
    val leftAlias = leftParent match {
      case c if leftParent.isInstanceOf[ScanOpNode] => leftParent.asInstanceOf[ScanOpNode].qualifier
      case _                                        => Some("")
    }
    val rightAlias = rightParent match {
      case c if rightParent.isInstanceOf[ScanOpNode] => rightParent.asInstanceOf[ScanOpNode].qualifier
      case _                                         => Some("")
    }
    (leftAlias.getOrElse(""), rightAlias.getOrElse(""))
  }

  def parseJoinTree(e: Option[Relation], inputOps: Seq[(String, OperatorNode)]): OperatorNode = e match {
    case None =>
      if (inputOps.size > 1)
        throw new Exception("Error in query: There are multiple input relations (" + inputOps.mkString(",") + ") but no join! Cannot process such query operator!")
      else inputOps(0)._2
    case Some(joinTree) => joinTree match {
      case j: Join =>
        val leftOp = parseJoinTree(Some(j.left), inputOps)
        val rightOp = parseJoinTree(Some(j.right), inputOps)
        val (leftAlias, rightAlias) = parseJoinAliases(leftOp, rightOp)
        new JoinOpNode(leftOp, rightOp, j.clause, j.tpe, leftAlias, rightAlias)
      case r: SQLTable if viewNameDefined(r.name) => getViewWithName(r.name)
      case vw: View                               => getViewWithName(vw.alias)
      case r: SQLTable =>
        val tableName = r.name + r.alias.getOrElse("")
        inputOps.find(so => so._1 == tableName) match {
          case Some(t) => t._2
          case None    => throw new Exception("LegoBase Frontend BUG: Table " + tableName + " referenced in join but a Scan operator for this table was not constructed! (inputops = " + inputOps.mkString + ")")
        }
      case sq: Subquery => SubqueryNode(convertQuery(sq.subquery).topOperator)
    }
  }

  def parseWhereClauses(e: Option[Expression], parentOp: OperatorNode): OperatorNode = e match {
    case Some(expr) =>
      analyzeExprForSubquery(expr, parentOp, false);
    case None => parentOp
  }

  def getExpressionName(expr: Expression) = {
    expr match {
      case FieldIdent(qualifier, name, _) => qualifier.getOrElse("") + name
      case Year(_)                        => throw new Exception("When YEAR is used in group by it must be given an alias")
      case _                              => throw new Exception("Invalid Group by (Non-single attribute reference) expression " + expr + " found that does not appear in the select statement.")
    }
  }

  def parseGroupBy(gb: Option[GroupBy], proj: Seq[(Expression, Option[String])]) = gb match {
    case Some(GroupBy(exprList)) =>
      exprList.map(gbExpr => proj.find(p => p._1 == gbExpr) match {
        case Some(p) if p._2.isDefined  => (gbExpr, p._2.get)
        case Some(p) if !p._2.isDefined => (gbExpr, getExpressionName(gbExpr))
        case _ => proj.find(p => p._2 == Some(getExpressionName(gbExpr))) match {
          case Some(e) => (e._1, e._2.get)
          case None    => (gbExpr, getExpressionName(gbExpr))
        }
      })
    case None => Seq()
  }

  def parseAggregations(e: Projections, gb: Option[GroupBy], parentOp: OperatorNode): OperatorNode = e match {
    case ExpressionProjections(proj) => {

      val divisionIndexes = scala.collection.mutable.ArrayBuffer[(String, String)]()
      val hasDivide = proj.find(p => p._1.isInstanceOf[Divide]).isDefined

      val projsWithoutDiv = proj.map(p => (p._1 match {
        case Divide(e1, e2) =>
          val e1Name = p._2.getOrElse("")
          val e2Name = p._2.getOrElse("") + "_2"
          divisionIndexes += ((e1Name, e2Name))
          Seq((e1, Some(e1Name)), (e2, Some(e2Name)))
        case _ => Seq((p._1, p._2))
      })).flatten.asInstanceOf[Seq[(Expression, Option[String])]]

      var aggProjs = projsWithoutDiv.filter(p => p._1.isAggregateOpExpr)

      val hasAVG = aggProjs.exists(ap => ap._1.isInstanceOf[Avg])
      if (hasAVG && aggProjs.find(_._1.isInstanceOf[CountAll]) == None)
        aggProjs = aggProjs :+ (CountAll(), Some("__TOTAL_COUNT"))

      if (aggProjs == List()) parentOp
      else {
        val aggNames = aggProjs.map(agg => agg._2 match {
          case Some(al) => al
          case None     => throw new Exception("LegoBase Limitation: All aggregations must be given an alias (aggregation " + agg._1 + " was not)")
        })

        val aggOp = if (hasAVG) {
          val finalAggs = aggProjs.map(ag => ag._1 match {
            case Avg(e) => Sum(e)
            case _      => ag._1
          })

          //val countAllIdx = aggsAliases.indexOf(CountAll())
          val countAllName = aggProjs.find(_._1.isInstanceOf[CountAll]).get._2.get
          val mapIndices = aggProjs.filter(avg => avg._1.isInstanceOf[Avg]).map(avg => { (avg._2.get, countAllName) })
          MapOpNode(AggOpNode(parentOp, finalAggs, parseGroupBy(gb, proj), aggNames), mapIndices)
        } else AggOpNode(parentOp, aggProjs.map(_._1), parseGroupBy(gb, proj), aggNames)

        if (hasDivide) MapOpNode(aggOp, divisionIndexes)
        else aggOp
      }
    }
    case AllColumns() => parentOp
  }

  // TODO -- needs to be generalized and expanded with more cases
  def createSubquery(sq: SelectStatement) = {
    val rootOp = SubquerySingleResultNode(createMainOperatorTree(sq))
    val rhs = GetSingleResult(rootOp)
    rhs.setTp(typeTag[Double]) // FIXME
    rhs
  }

  def subqueryHasSingleTupleResult(sq: SelectStatement) = sq.groupBy == None

  def analyzeExprForSubquery(expr: Expression, parentOp: OperatorNode, isHaving: Boolean): SelectOpNode = expr match {
    case GreaterThan(e, (sq: SelectStatement)) if subqueryHasSingleTupleResult(sq) =>
      SelectOpNode(parentOp, GreaterThan(e, createSubquery(sq)), isHaving)
    case LessThan(e, (sq: SelectStatement)) if subqueryHasSingleTupleResult(sq) =>
      SelectOpNode(parentOp, LessThan(e, createSubquery(sq)), isHaving)
    case Equals(e, (sq: SelectStatement)) if subqueryHasSingleTupleResult(sq) =>
      SelectOpNode(parentOp, LessThan(e, createSubquery(sq)), isHaving)
    case And(e1, e2) =>
      // TODO -- Not the best of solutions, but OK for now (this func needs to break into two, one for analysis, one for construction of nodes)
      val left = analyzeExprForSubquery(e1, parentOp, isHaving)
      val right = analyzeExprForSubquery(e2, parentOp, isHaving)
      SelectOpNode(parentOp, And(left.cond, right.cond), isHaving)
    case dflt @ _ =>
      //System.out.println("Do not know how to handle " + dflt + "... Treating it as default..."); 
      SelectOpNode(parentOp, expr, isHaving)
  }

  def parseHaving(having: Option[Having], parentOp: OperatorNode): OperatorNode = {
    having match {
      case Some(Having(expr)) =>
        analyzeExprForSubquery(expr, parentOp, true);
      case None => parentOp
    }
  }

  def parseOrderBy(ob: Option[OrderBy], parentOp: OperatorNode) = ob match {
    case Some(OrderBy(listExpr)) => OrderByNode(parentOp, listExpr)
    case None                    => parentOp
  }

  def parseProjections(e: Projections, gb: Option[GroupBy], parentOp: OperatorNode): OperatorNode = e match {
    case ExpressionProjections(projs) =>
      //System.out.println("Trying to create projection operator for " + projs + " and groupbys " + gb + " which when parsed is " + parseGroupBy(gb, projs))
      // If there is a projection that is a) not an aggregation, b) not a group by and c) *does have an alias* then we need to project
      val projsThatNeedRenaming = projs.filter(p => {
        gb match {
          case Some(l) => !l.contains(p._1)
          case None    => true
        }
      } && !p._1.isAggregateOpExpr && p._2.isDefined)
      //System.out.println("projsThatNeedRenaming = " + projsThatNeedRenaming)
      if (projsThatNeedRenaming.size == 0) parentOp
      else ProjectOpNode(parentOp, projsThatNeedRenaming.map(_._2.get), projsThatNeedRenaming.map(_._1))
    case AllColumns() => parentOp
  }

  def createMainOperatorTree(sqlTree: SelectStatement): OperatorNode = {
    sqlTree.withs.foreach(w => {
      val qt = convertQuery(w.subquery)
      // TODO -- Handle all columns
      temporaryViewMap += ViewOpNode(qt.topOperator, qt.projections.asInstanceOf[ExpressionProjections].getNames, w.alias)
    })

    val inputOps = createScanOperators(sqlTree)
    /* We assume that a normalizer has created a single joinTree
     * from all the input relations specified in the select statement.
     * Thus we can just get the first element in this sequence.
     */
    val hashJoinOp = parseJoinTree(sqlTree.joinTree, inputOps.toSeq)
    val selectOp = parseWhereClauses(sqlTree.where, hashJoinOp)
    val aggOp = parseAggregations(sqlTree.projections, sqlTree.groupBy, selectOp)
    val orderByOp = parseOrderBy(sqlTree.orderBy, aggOp)
    val havingOp = parseHaving(sqlTree.having, orderByOp)
    val projOp = parseProjections(sqlTree.projections, sqlTree.groupBy, havingOp)
    projOp
  }

  case class QueryTree(topOperator: OperatorNode, projections: Projections, limit: Option[Limit])

  def convertQuery(node: Node): QueryTree = node match {
    case UnionIntersectSequence(top, bottom, connectionType) =>
      val topQueryTree = convertQuery(top)
      val bottomQueryTree = convertQuery(bottom)
      if (connectionType == UNION) throw new Exception("SQL UNION NOT YET HANDLED (ONLY UNION ALL)")
      if (connectionType == INTERSECT) throw new Exception("SQL INTERSECT NOT YET HANDLED")
      if (connectionType == SEQUENCE) throw new Exception("SQL SEQUENCE OF QUERIES NOT YET HANDLED")
      if (connectionType == EXCEPT) throw new Exception("SQL EXCEPT NOT YET HANDLED")

      val unionTree = UnionAllOpNode(topQueryTree.topOperator, bottomQueryTree.topOperator)
      // TODO: Check here that both parts of union all return the same schema. If not throw an exception
      // TODO: Handle limit properly... this assumes that both parts of union all have the same limit (not necessary)
      QueryTree(unionTree, topQueryTree.projections, topQueryTree.limit)
    case stmt: SelectStatement =>
      QueryTree(createMainOperatorTree(stmt), stmt.projections, stmt.limit)
  }

  def createPrintOperator(parent: OperatorNode, e: Projections, limit: Option[Limit]) = {
    val projs = e match {
      case ExpressionProjections(proj) => proj
      case AllColumns()                => Seq()
    }
    new PrintOpNode(parent, projs, limit match {
      case Some(Limit(num)) => num.toInt
      case None             => -1
    })
  }

  def convert(node: Node): QueryPlanTree = {
    val queryTree = convertQuery(node)
    val printOp = createPrintOperator(queryTree.topOperator, queryTree.projections, queryTree.limit)
    QueryPlanTree(printOp, temporaryViewMap)
  }
}