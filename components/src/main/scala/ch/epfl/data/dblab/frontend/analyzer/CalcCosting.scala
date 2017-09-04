package ch.epfl.data
package dblab
package frontend
package analyzer

import parser.CalcAST._
import sc.pardis.search.CostingContext
import sc.pardis.ast.Node

object CalcCosting extends CostingContext {
  def apply(node: Node): Double = cost(node.asInstanceOf[CalcExpr])

  // TODO add proper costs for different calc expressions
  // most probably an appropriate cost will be parameteric and dependent on 
  // the cardinality of relations.
  def cost(exp: CalcExpr): Double = exp match {
    case CalcProd(lst) => 100 + lst.map(cost).sum
    case CalcSum(lst)  => 10 + lst.map(cost).sum
    case _             => 10
  }
}
