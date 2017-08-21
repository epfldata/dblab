package ch.epfl.data
package dblab
package frontend
package parser

import schema._
import scala.reflect._
import scala.collection.mutable.ArrayBuffer
import SQLAST._

/**
 * A module containing AST of physical query operators.
 *
 * @author Yannis Klonatos
 */
object OperatorAST {

  case class QueryPlanTree(rootNode: OperatorNode, views: ArrayBuffer[ViewOpNode]) {
    override def toString() = {
      views.map(v => v.name + ":\n" + v.parent.toString + "\n").mkString("\n") + "\n" + rootNode.toString
    }
  }

  abstract class OperatorNode {
    def toList(): List[OperatorNode] = List(this) ++ (this match {
      case ScanOpNode(_, _, _)                             => List()
      case UnionAllOpNode(top, bottom)                     => top.toList ++ bottom.toList
      case SelectOpNode(parent, _, _)                      => parent.toList
      case JoinOpNode(leftParent, rightParent, _, _, _, _) => leftParent.toList ++ rightParent.toList
      case AggOpNode(parent, _, _, _)                      => parent.toList
      case MapOpNode(parent, _)                            => parent.toList
      case OrderByNode(parent, ob)                         => parent.toList
      case PrintOpNode(parent, _, _)                       => parent.toList
      case SubqueryNode(parent)                            => parent.toList
      case SubquerySingleResultNode(parent)                => parent.toList
      case ProjectOpNode(parent, _, _)                     => parent.toList
      case ViewOpNode(parent, _, name)                     => parent.toList
    })
  }

  private var printDepth = 0;
  private def printIdent = " " * (printDepth * 6)

  private def stringify(op: Any, preamble: String = "") = {
    printDepth += 1
    val res = "\n" + printIdent + "|--> " + preamble + op.toString
    printDepth -= 1
    res
  }

  /* Qualifier (if any) is included in the scanOpName */
  case class ScanOpNode(table: Table, scanOpName: String, qualifier: Option[String]) extends OperatorNode {
    override def toString = "ScanOp(" + scanOpName + ")"
  }

  case class SelectOpNode(parent: OperatorNode, cond: Expression, isHavingClause: Boolean) extends OperatorNode {
    override def toString = "SelectOp" + stringify(cond, "CONDITION: ") + stringify(parent)
  }

  case class JoinOpNode(left: OperatorNode, right: OperatorNode, clause: Expression, joinType: JoinType, leftAlias: String, rightAlias: String) extends OperatorNode {
    override def toString = joinType + stringify(left) + stringify(right) + stringify(clause, "CONDITION: ")
  }

  case class AggOpNode(parent: OperatorNode, aggs: Seq[Expression], gb: Seq[(Expression, String)], aggNames: Seq[String]) extends OperatorNode {
    override def toString = "AggregateOp" + stringify(aggs, "AGGREGATES: ") + stringify(gb.map(_._2), "GROUP BY: ") + stringify(parent)
  }

  // TODO -- Currently used by LegoBase only for calculating averages and divisions (thus the / below in toString), but can be generalized
  case class MapOpNode(parent: OperatorNode, mapIndices: Seq[(String, String)]) extends OperatorNode {
    override def toString = "MapOpNode" + stringify(mapIndices.map(mi => mi._1 + " = " + mi._1 + " / " + mi._2).mkString(", ")) + stringify(parent)
  }

  case class OrderByNode(parent: OperatorNode, orderBy: Seq[(Expression, OrderType)]) extends OperatorNode {
    override def toString = {
      val orderByFlds = orderBy.map(ob => ob._1 + " " + ob._2).mkString(", ")
      "SortOp" + stringify(orderByFlds, "ORDER BY: ") + stringify(parent)
    }
  }

  case class PrintOpNode(parent: OperatorNode, projNames: Seq[(Expression, Option[String])], limit: Int) extends OperatorNode {
    override def toString = "PrintOpNode" + stringify(projNames.map(p => p._2 match {
      case Some(al) => al
      case None     => p._1
    }).mkString(","), "PROJ: ") + {
      if (limit != -1) stringify(limit, "LIMIT: ")
      else ""
    } + stringify(parent)
  }

  case class UnionAllOpNode(top: OperatorNode, bottom: OperatorNode) extends OperatorNode {
    override def toString = "UnionAllOp" + stringify(top) + stringify(bottom)
  }

  case class ProjectOpNode(parent: OperatorNode, projNames: Seq[String], origFieldNames: Seq[Expression]) extends OperatorNode {
    override def toString = "ProjectOp" + stringify(projNames zip origFieldNames, "PROJ NAMES: ") + stringify(parent)
  }

  case class ViewOpNode(parent: OperatorNode, projNames: Seq[String], name: String) extends OperatorNode {
    override def toString = "View " + name
  }

  // Dummy node to know where a subquery begins and ends
  case class SubqueryNode(parent: OperatorNode) extends OperatorNode {
    override def toString = "Subquery" + stringify(parent)
  }

  case class SubquerySingleResultNode(parent: OperatorNode) extends OperatorNode {
    override def toString = "SubquerySingleResult" + stringify(parent)
  }
  // TODO why does it extend SQLAST.Expression ?
  case class GetSingleResult(parent: OperatorNode) extends Expression {
    override def toString() = "GetSingleResult(" + stringify(parent) + ")"
  }
}
