package ch.epfl.data
package dblab
package frontend
package optimizer

import parser.CalcAST._
import parser.SQLAST._
import sc.pardis.ast._
import sc.pardis.rules._

object CalcRules {
  case object Agg0 extends Rule("Agg0") {
    def generate(node: Node): Option[Node] = node match {
      case AggSum(gbvars, CalcValue(ArithConst(IntLiteral(0)))) =>
        Some(CalcValue(ArithConst(IntLiteral(0))))
      case AggSum(gbvars, CalcValue(ArithConst(DoubleLiteral(0.0)))) =>
        Some(CalcValue(ArithConst(DoubleLiteral(0.0))))
      case _ => None
    }
  }
}