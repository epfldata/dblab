package ch.epfl.data
package dblab
package frontend
package parser

import sc.pardis.types.{ PardisType, Tpe }

/**
 * A module containing AST nodes for AG
 * CA constructs.
 *
 * @author Mohsen Ferdosi
 */
object CalcAST {
  trait CalcExpr
  case class CalcQuery(name: String, expr: CalcExpr) extends CalcExpr
  case class CalcSum(exprs: List[CalcExpr]) extends CalcExpr // Why ?
  case class CalcProd(exprs: List[CalcExpr]) extends CalcExpr
  case class CalcNeg(expr: CalcExpr) extends CalcExpr
  case class AggSum(vars: List[VarT], expr: CalcExpr) extends CalcExpr
  case class Rel(tag: String, name: String, vars: List[VarT], rest: String) extends CalcExpr
  case class External(e: CalcExpr, et: External_t) extends CalcExpr
  case class Cmp(cmp: Cmp_t, first: ArithExpr, second: ArithExpr) extends CalcExpr
  case class CmpOrList(v: ArithExpr, consts: List[Const_t]) extends CalcExpr
  case class Lift(vr: VarT, expr: CalcExpr) extends CalcExpr
  case class Exists(term: CalcExpr) extends CalcExpr
  case class CalcValue(v: ArithExpr) extends CalcExpr

  trait ArithExpr
  case class ArithSum(expr: List[ArithExpr]) extends ArithExpr
  case class ArithProd(expr: List[ArithExpr]) extends ArithExpr
  case class ArithNeg(expr: ArithExpr) extends ArithExpr
  case class ArithConst(lit: SQLAST.LiteralExpression) extends ArithExpr
  case class ArithVar(v: VarT) extends ArithExpr
  case class ArithFunc(name: String, terms: List[ArithExpr], tp: Tpe) extends ArithExpr

  case class VarT(name: String, tp: Tpe)

  case class External_t(name: String, inps: List[VarT], outs: List[VarT], tp: Tpe, meta: Option[CalcExpr])

  trait Cmp_t
  case object Eq extends Cmp_t // Equals
  case object Lt extends Cmp_t // Less Than
  case object Lte extends Cmp_t // Less Than or equal
  case object Gt extends Cmp_t // Greater Than
  case object Gte extends Cmp_t // Greater Than or equal
  case object Neq extends Cmp_t // Not equals

  trait Const_t
  case class CBool(value: Boolean) extends Const_t
  case class CInt(value: Int) extends Const_t
  case class CFloat(value: Float) extends Const_t
  case class CString(value: String) extends Const_t
  case class CDate(y: Int, m: Int, d: Int) extends Const_t
  case class CInterval(it: Interval_t) extends Const_t

  trait Interval_t
  case class CYearMonth(y: Int, m: Int) extends Interval_t
  case class CDay(value: Int) extends Interval_t

}