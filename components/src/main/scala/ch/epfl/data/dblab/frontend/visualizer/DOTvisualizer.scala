package ch.epfl.data.dblab.frontend.visualiser

import java.io.{File, PrintWriter}
import java.text.SimpleDateFormat
import java.util.Date

import ch.epfl.data.dblab.frontend.analyzer.PlanCosting
import ch.epfl.data.dblab.frontend.parser.OperatorAST._

object DOTvisualiser {
  def visualize(tree: QueryPlanTree, planCosting: PlanCosting, queryName: String, simple: Boolean = true, subquery: String = "") {

    def parseTree(tree: OperatorNode, suffix: String = ""): String = {

      def connectTwo(node1: OperatorNode, node2: OperatorNode): String = node1 match {
        case SubqueryNode(_) | SubquerySingleResultNode(_) => "\"" + getLabel(node1, suffix) + "\" -> \"" + getLabel(node2, suffix + "_sub") + "\" [label=\"" + planCosting.size(node2) + "\"];\n"
        case _ => "\"" + getLabel(node1, suffix) + "\" -> \"" + getLabel(node2, suffix) + "\" [label=\"" + planCosting.size(node2) + "\"];\n"
      }

      tree match {
        case ScanOpNode(_, _, _)                 => ""
        case UnionAllOpNode(top, bottom)         => connectTwo(tree, top) + connectTwo(tree, bottom) + parseTree(top, suffix) + parseTree(bottom, suffix)
        case SelectOpNode(parent, _, _)          => connectTwo(tree, parent) + parseTree(parent, suffix)
        case JoinOpNode(left, right, _, _, _, _) => connectTwo(tree, left) + connectTwo(tree, right) + parseTree(right, suffix) + parseTree(left, suffix)
        case AggOpNode(parent, _, _, _)          => connectTwo(tree, parent) + parseTree(parent, suffix)
        case MapOpNode(parent, _)                => connectTwo(tree, parent) + parseTree(parent, suffix)
        case OrderByNode(parent, ob)             => connectTwo(tree, parent) + parseTree(parent, suffix)
        case PrintOpNode(parent, _, _)           => connectTwo(tree, parent) + parseTree(parent, suffix)
        case SubqueryNode(parent)                => connectTwo(tree, parent) + parseTree(parent, suffix + "_sub")
        case SubquerySingleResultNode(parent)    => connectTwo(tree, parent) + parseTree(parent, suffix + "_sub")
        case ProjectOpNode(parent, _, _)         => connectTwo(tree, parent) + parseTree(parent, suffix)
        case ViewOpNode(parent, _, name)         => connectTwo(tree, parent) + parseTree(parent, suffix)
      }
    }

    def getLabel(node: OperatorNode, suffix: String = ""): String = {
      node match {
        case ScanOpNode(_, scanOpName, _)             => "SCAN " + scanOpName + suffix
        case UnionAllOpNode(_, _)                     => "U" + suffix
        case SelectOpNode(_, cond, _)                 => "Select " + cond.toString + suffix
        case JoinOpNode(_, _, clause, joinType, _, _) => joinType + " " + clause.toString + suffix
        case AggOpNode(_, aggs, gb, _)                => "Aggregate " + aggs.toString + " group by " + gb.map(_._2).toString + suffix
        case MapOpNode(_, mapIndices)                 => "Map " + mapIndices.map(mi => mi._1 + " = " + mi._1 + " / " + mi._2).mkString(", ").toString + suffix
        case OrderByNode(_, ob)                       => "Order by " + ob.map(ob => ob._1 + " " + ob._2).mkString(", ").toString + suffix
        case PrintOpNode(_, projNames, limit) => "Project " + projNames.map(p => p._2 match {
          case Some(al) => al
          case None     => p._1
        }).mkString(",").toString + {
          if (limit != -1) ", limit = " + limit.toString
          else ""
        } + suffix
        case SubqueryNode(parent)                        => "Subquery" + suffix
        case SubquerySingleResultNode(parent)            => "Single subquery" + suffix
        case ProjectOpNode(_, projNames, origFieldNames) => "Project " + (projNames zip origFieldNames).toString + suffix
        case ViewOpNode(_, _, name)                      => "View " + name + suffix
      }
    }

    def simplify(node: OperatorNode, suffix: String = ""): String = node match {
      case ScanOpNode(_, scanOpName, _)                                => "\"" + getLabel(node, suffix) + "\"" + " [label=\"" + getLabel(node, "") + "\"];\n"
      case UnionAllOpNode(top, bottom)                                 => "\"" + getLabel(node, suffix) + "\"" + " [label=\"∪\"];\n" + simplify(top, suffix) + simplify(bottom, suffix)
      case SelectOpNode(parent, _, _)                                  => "\"" + getLabel(node, suffix) + "\"" + " [label=\"σ\"];\n" + simplify(parent, suffix)
      case JoinOpNode(left, right, _, joinType, leftAlias, rightAlias) => "\"" + getLabel(node, suffix) + "\"" + " [label=\"⋈: " + joinType + "\"];\n" + simplify(left, suffix) + simplify(right, suffix)
      case AggOpNode(parent, _, _, _)                                  => "\"" + getLabel(node, suffix) + "\"" + " [label=\"Γ\"];\n" + simplify(parent, suffix)
      case MapOpNode(parent, _)                                        => "\"" + getLabel(node, suffix) + "\"" + " [label=\"X\"];\n" + simplify(parent, suffix)
      case OrderByNode(parent, ob)                                     => "\"" + getLabel(node, suffix) + "\"" + " [label=\"SORT\"];\n" + simplify(parent, suffix)
      case PrintOpNode(parent, _, _)                                   => "\"" + getLabel(node, suffix) + "\"" + " [label=\"Print\"];\n" + simplify(parent, suffix)
      case SubqueryNode(parent)                                        => "\"" + getLabel(node, suffix) + "\"" + " [label=\"Subquery\"];\n" + simplify(parent, suffix + "_sub")
      case SubquerySingleResultNode(parent)                            => "\"" + getLabel(node, suffix) + "\"" + " [label=\"Subquery single\"];\n" + simplify(parent, suffix + "_sub")
      case ProjectOpNode(parent, _, _)                                 => "\"" + getLabel(node, suffix) + "\"" + " [label=\"π\"];\n" + simplify(parent, suffix)
      case ViewOpNode(parent, _, name)                                 => "\"" + getLabel(node, suffix) + "\"" + " [label=\"View\"];\n" + simplify(parent, suffix)
    }

    val dateFormat = new SimpleDateFormat("MM-dd_HH-mm-ss");

    val date = new Date()

    val graphSuffix = if (simple) "_simple" else "_full"
    val fileName = queryName + "_" + dateFormat.format(date) + graphSuffix

    val pw = new PrintWriter(new File("/Users/michal/Desktop/SemesterProject/visualisations/" + fileName + ".gv"))

    pw.write("digraph G { \n	size=\"8,8\"\nrankdir = TB;\nedge[dir=back];\n")

    var result = ""

    //result += "\"Cost\" [label=\"Cost: " + planCosting.cost() + "\"];\n";
    if (simple) result += simplify(tree.rootNode)
    result += parseTree(tree.rootNode)
    pw.write(result)
    pw.write("}")
    pw.close
  }
}
