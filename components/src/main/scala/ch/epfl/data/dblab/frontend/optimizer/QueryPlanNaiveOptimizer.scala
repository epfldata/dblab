package ch.epfl.data
package dblab
package frontend
package optimizer

import schema._
import parser._
import parser.OperatorAST._
import parser.SQLAST._
import scala.collection.mutable.ArrayBuffer

/**
 * This optimizer is called naive, since it only pushes-up selection predicates to
 * scan and join operators, and performs no other technique from traditional query
 * optimization (e.g. join reordering).
 *
 * The tree received by this optimizer is the one built by the SQL parser, thus
 * _by construction_ has _a single_ select op that performs the where clause, and
 * which is executed after all the scans and joins and aggregations have been
 * executed. This optimizer pushes up parts of this where clause to the corresponding
 * scan and join operators operators.
 *
 * @author Yannis Klonatos
 */
class QueryPlanNaiveOptimizer(schema: Schema) extends QueryPlanOptimizer {

  var linkingOperatorIsAnd = true
  var operatorList: List[OperatorNode] = _

  // Join conditions to be pushed up
  val joinOpConds = new scala.collection.mutable.HashMap[OperatorNode, ArrayBuffer[Expression]]();
  // ScanOp conditions to be pushed up
  val scanOpConds = new scala.collection.mutable.HashMap[OperatorNode, ArrayBuffer[Expression]]();

  def containsField(op: OperatorNode, fld: FieldIdent): Boolean = op match {
    case JoinOpNode(leftParent, rightParent, joinCond, joinType, leftAlias, rightAlias) =>
      containsField(leftParent, fld) || containsField(rightParent, fld)
    case ViewOpNode(qpt, projNames, name) =>
      // System.out.println("projNames of ViewOp " + name + " = " + projNames + " and searching for " + fld.name)
      projNames.contains(fld.name)
    case AggOpNode(parent, aggs, gb, aggAlias) =>
      aggAlias.contains(fld.name) || gb.map(_._2).contains(fld.name)
    case SelectOpNode(parent, cond, isHaving) =>
      containsField(parent, fld)
    case so: ScanOpNode =>

      so.scanOpName == (schema.tables.find(t => t.findAttribute(fld.name).isDefined) match {
        case Some(tbl) => {
          // Apparently it is valid to use table name when referencing a field ident. So we 
          // check for this case here and handle it approprietly
          if (fld.qualifier.getOrElse("") != tbl.name) tbl.name + fld.qualifier.getOrElse("")
          else tbl.name
        }
        case None => ""
      })
    case MapOpNode(parent, mapIndices) =>
      containsField(parent, fld)
  }

  def registerScanOpCond(fi: FieldIdent, cond: Expression): Option[Expression] = {
    schema.tables.find(t => t.findAttribute(fi.name).isDefined) match {
      case None => Some(cond)
      case Some(tbl) =>
        val scanOperators = operatorList.filter(_.isInstanceOf[ScanOpNode]).map(_.asInstanceOf[ScanOpNode])

        val scanOp = scanOperators.find(so => (so.scanOpName == tbl.name) || (so.scanOpName == tbl.name + fi.qualifier.getOrElse(""))) match {
          case Some(op) => op
          case None     => throw new Exception("BUG: Scan op " + tbl.name + " referenced but no such operator exists!")
        }

        val buf = scanOpConds.getOrElseUpdate(scanOp, new ArrayBuffer[Expression]())
        buf += cond
        None
    }
  }

  def reorderPredicates(expr: Expression) = expr match {
    case And(lhs, rhs)    => And(rhs, lhs)
    case Equals(lhs, rhs) => Equals(rhs, lhs)
  }

  def registerJoinOpCond(fi1: FieldIdent, fi2: FieldIdent, cond: Expression) {
    val joinOperators = operatorList.filter(_.isInstanceOf[JoinOpNode]).map(_.asInstanceOf[JoinOpNode]).sortWith((a, b) => a.toList.length < b.toList.length)

    var reorder = false
    // System.out.println("Searching to find operator for join cond " + cond + "\n")
    joinOperators.find(jo => {
      val containsInExistingOrder = containsField(jo.left, fi1) && containsField(jo.right, fi2)
      val containsInReverseOrder = containsField(jo.right, fi1) && containsField(jo.left, fi2)
      // System.out.println(jo + "/" + containsInExistingOrder + "/" + containsInReverseOrder + "/(" + containsField(jo.left, fi1) + "/" + containsField(jo.right, fi2) + ")" + "(" + containsField(jo.right, fi1) + "/" + containsField(jo.left, fi2) + ")")
      if (containsInExistingOrder) true
      else if (containsInReverseOrder) {
        reorder = true
        true
      } else false
    }) match {
      case Some(jo) =>
        //System.out.println("Adding cond " + cond + " to operator " + jo);
        val buf = joinOpConds.getOrElseUpdate(jo, new ArrayBuffer[Expression]())
        buf += {
          if (reorder) reorderPredicates(cond)
          else cond
        }
      case None => throw new Exception("LegoBase Optimizer: Couldn't find join operator for condition " + cond)
    }
  }

  def analysePushingUpCondition(parent: OperatorNode, cond: Expression): Option[Expression] = {
    def isArithmeticBinaryOp(e: Expression) = e match {
      case Subtract(_, _) | Multiply(_, _) | Add(_, _) | Divide(_, _) => true
      case _ => false
    }

    // System.out.println("Analyzing condition " + cond)
    cond match {
      case And(lhs, rhs) =>
        val lhsPushed = analysePushingUpCondition(parent, lhs)
        val rhsPushed = analysePushingUpCondition(parent, rhs)
        (lhsPushed, rhsPushed) match {
          case (None, None)       => None
          case (Some(_), None)    => lhsPushed
          case (None, Some(_))    => rhsPushed
          case (Some(l), Some(r)) => Some(And(l, r))
        }
      // FIXME there's a bug here
      case Or(lhs, rhs) =>
        // val lhsPushed = analysePushingUpCondition(parent, lhs)
        // val rhsPushed = analysePushingUpCondition(parent, rhs)
        // (lhsPushed, rhsPushed) match {
        //   case (None, None)       => None
        //   case (Some(_), None)    => lhsPushed
        //   case (None, Some(_))    => rhsPushed
        //   case (Some(l), Some(r)) => Some(Or(l, r))
        // }
        Some(Or(lhs, rhs))

      case Equals(fi1: FieldIdent, fi2: FieldIdent) =>
        registerJoinOpCond(fi1, fi2, cond)
        None
      case Equals(fi: FieldIdent, lit: LiteralExpression) => registerScanOpCond(fi, cond)
      case Equals(fi: FieldIdent, e) if isArithmeticBinaryOp(e) => Some(cond)
      case Equals(_, _) => Some(cond)

      case NotEquals(fi: FieldIdent, lit: LiteralExpression) => registerScanOpCond(fi, cond)
      case GreaterThan(fi: FieldIdent, lit: LiteralExpression) => registerScanOpCond(fi, cond)
      case GreaterOrEqual(fi: FieldIdent, lit: LiteralExpression) => registerScanOpCond(fi, cond)
      case LessThan(fi: FieldIdent, lit: LiteralExpression) => registerScanOpCond(fi, cond)
      case LessOrEqual(fi: FieldIdent, lit: LiteralExpression) => registerScanOpCond(fi, cond)
      case Like(fi: FieldIdent, lit: LiteralExpression) => registerScanOpCond(fi, cond)
      case In(fi: FieldIdent, _) => registerScanOpCond(fi, cond)

      // TODO -- Can optimize further by looking at lhs and rhs recursively 
      case GreaterThan(_, _) => Some(cond)
      case LessThan(_, _) => Some(cond)
      case LessOrEqual(_, _) => Some(cond)
      case Not(_) => Some(cond) // TODO: can this be optimized?
    }
  }

  /** Connects the predicates using AND */
  private def connectPredicates(eq: Seq[Expression]): Expression = {
    eq.tail.foldLeft(eq.head.asInstanceOf[Expression])((elem, acc) => And(acc, elem))
  }

  def pushUpCondition(tree: OperatorNode): OperatorNode = tree match {
    case JoinOpNode(leftParent, rightParent, joinCond, joinType, leftAlias, rightAlias) =>
      val newCond = (joinOpConds.get(tree) match {
        case Some(expr) => connectPredicates(expr)
        case None       => joinCond
      }).asInstanceOf[Expression]
      JoinOpNode(pushUpCondition(leftParent), pushUpCondition(rightParent), newCond, joinType, leftAlias, rightAlias)
    case ScanOpNode(table, _, _) => scanOpConds.get(tree) match {
      case Some(expr) => SelectOpNode(tree, connectPredicates(expr), false)
      case None       => tree
    }
    case AggOpNode(parent, aggs, gb, aggNames) => AggOpNode(pushUpCondition(parent), aggs, gb, aggNames)
    case SubqueryNode(parent)                  => SubqueryNode(optimizeInContext(parent))
    case ViewOpNode(parent, _, name)           => tree
    case OrderByNode(parent, orderBy)          => OrderByNode(optimizeNode(parent), orderBy)
    case SelectOpNode(parent, cond, isHaving)  => SelectOpNode(optimizeNode(parent), cond, isHaving)
  }

  def optimizeInContext(node: OperatorNode): OperatorNode = {
    // Save old map, then start processing the given node with an empty map and finally
    // restore original map with proceeding with rest of original query. Return the result
    // of processing the given node. This method is useful for subqueries
    val oldJoinOpConds = joinOpConds.clone()
    val oldScanOpConds = scanOpConds.clone()
    joinOpConds.clear()
    scanOpConds.clear()
    val res = optimizeNode(node)
    joinOpConds.clear()
    scanOpConds.clear()
    joinOpConds ++= oldJoinOpConds
    scanOpConds ++= oldScanOpConds
    res
  }

  def optimizeNode(tree: OperatorNode): OperatorNode = tree match {
    case ProjectOpNode(parent, projNames, origFieldNames) => ProjectOpNode(optimizeNode(parent), projNames, origFieldNames)
    case ScanOpNode(_, _, _)                              => tree
    case SelectOpNode(parent, cond, isHaving) /*if isHaving == false*/ =>
      val remCond = analysePushingUpCondition(parent, cond)
      val newParentTree = pushUpCondition(parent)
      remCond match {
        case Some(expr) => SelectOpNode(newParentTree, expr, false)
        case None       => newParentTree
      }
    //case SelectOpNode(parent, cond, isHaving) if isHaving == true =>
    //  SelectOpNode(optimizeNode(parent), cond, isHaving)
    case JoinOpNode(leftParent, rightParent, joinCond, joinType, leftAlias, rightAlias) =>
      JoinOpNode(optimizeNode(leftParent), optimizeNode(rightParent), joinCond, joinType, leftAlias, rightAlias)
    case SubquerySingleResultNode(parent)      => SubquerySingleResultNode(optimizeNode(parent))
    case MapOpNode(parent, mapIndices)         => MapOpNode(optimizeNode(parent), mapIndices)
    case AggOpNode(parent, aggs, gb, aggAlias) => AggOpNode(optimizeNode(parent), aggs, gb, aggAlias)
    case OrderByNode(parent, ob)               => OrderByNode(optimizeNode(parent), ob)
    case PrintOpNode(parent, projNames, limit) => PrintOpNode(optimizeNode(parent), projNames, limit)
    case SubqueryNode(parent)                  => SubqueryNode(optimizeNode(parent))
    case UnionAllOpNode(top, bottom)           => UnionAllOpNode(optimizeNode(top), optimizeNode(bottom))
    case ViewOpNode(parent, projs, name)       => ViewOpNode(optimizeNode(parent), projs, name)
  }

  def optimizeRootNode(rn: OperatorNode): OperatorNode = {
    operatorList = rn.toList
    optimizeNode(rn)
  }

  def optimize(qp: QueryPlanTree): QueryPlanTree = {
    val optimizedViews = qp.views.map(v => ViewOpNode(optimizeRootNode(v.parent), v.projNames, v.name))
    QueryPlanTree(optimizeRootNode(qp.rootNode), optimizedViews)
  }
}
