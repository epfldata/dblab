package ch.epfl.data
package dblab
package frontend
package parser

import sc.pardis.types.PardisType

/**
 * A module containing AST nodes for AGCA constructs.
 *
 * @author Mohsen Ferdosi
 */
object CalcAST {
  trait CalcExpr
  case class CalcSum(exprs: List[CalcExpr]) extends CalcExpr
  // TODO CalcProd
  case class CalcNeg(expr: CalcExpr) extends CalcExpr
  case class AggSum(vars: List[VarT], expr: CalcExpr) extends CalcExpr
  // TODO the rest
  case class CalcValue(v: ArithExpr) extends CalcExpr

  trait ArithExpr
  case class ArithSum(expr: List[ArithExpr]) extends ArithExpr
  // TODO ArithProd
  case class ArithNeg(expr: ArithExpr) extends ArithExpr
  case class ArithConst(lit: SQLAST.LiteralExpression) extends ArithExpr
  case class ArithVar(v: VarT) extends ArithExpr
  // TODO ArithFunc

  case class VarT(name: String, tp: PardisType[_])
}