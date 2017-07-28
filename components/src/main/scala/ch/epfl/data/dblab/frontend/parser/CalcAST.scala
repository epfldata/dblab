package ch.epfl.data
package dblab
package frontend
package parser

import ch.epfl.data.dblab.schema.{DateType, VarCharType}
import sc.pardis.types._

/**
 * A module containing AST nodes for AG
 * CA constructs.
 *
 * @author Mohsen Ferdosi
 */
object CalcAST {
  trait CalcExpr
  case class CalcQuery(name: String, expr: CalcExpr) extends CalcExpr
  case class CalcSum(exprs: List[CalcExpr]) extends CalcExpr
  case class CalcProd(exprs: List[CalcExpr]) extends CalcExpr
  case class CalcNeg(expr: CalcExpr) extends CalcExpr
  case class AggSum(vars: List[VarT], expr: CalcExpr) extends CalcExpr
  case class Rel(tag: String, name: String, vars: List[VarT], rest: String) extends CalcExpr
  case class External(e: CalcExpr, et: External_t) extends CalcExpr
  case class Cmp(cmp: CmpTag, first: ArithExpr, second: ArithExpr) extends CalcExpr
  case class CmpOrList(v: ArithExpr, consts: List[Const]) extends CalcExpr
  case class Lift(vr: VarT, expr: CalcExpr) extends CalcExpr
  case class Exists(term: CalcExpr) extends CalcExpr
  case class CalcValue(v: ArithExpr) extends CalcExpr
  case class In(first: ArithExpr, vars: List[ArithExpr]) extends CalcExpr

  //TODO add const one and zero

  trait ArithExpr
  case class ArithSum(expr: List[ArithExpr]) extends ArithExpr
  case class ArithProd(expr: List[ArithExpr]) extends ArithExpr
  case class ArithNeg(expr: ArithExpr) extends ArithExpr
  case class ArithConst(lit: SQLAST.LiteralExpression) extends ArithExpr
  case class ArithVar(v: VarT) extends ArithExpr
  case class ArithFunc(name: String, terms: List[ArithExpr], tp: Tpe) extends ArithExpr

  case class VarT(name: String, tp: Tpe)

  case class External_t(name: String, inps: List[VarT], outs: List[VarT], tp: Tpe, meta: Option[CalcExpr])

  trait CmpTag
  case object Eq extends CmpTag // Equals
  case object Lt extends CmpTag // Less Than
  case object Lte extends CmpTag // Less Than or equal
  case object Gt extends CmpTag // Greater Than
  case object Gte extends CmpTag // Greater Than or equal
  case object Neq extends CmpTag // Not equals

  trait Const
  case class CBool(value: Boolean) extends Const

  case class CInt(value: Int) extends Const
  case class CFloat(value: Float) extends Const
  case class CString(value: String) extends Const
  case class CDate(y: Int, m: Int, d: Int) extends Const
  case class CInterval(it: Interval) extends Const

  trait Interval
  case class CYearMonth(y: Int, m: Int) extends Interval
  case class CDay(value: Int) extends Interval

  def prettyprint(calcExpr: CalcExpr): String = {
    def rcr(c: CalcExpr): String = prettyprint(c)
    calcExpr match{
      case CalcQuery(id , expr) => s"${id}: ${rcr(expr)}"
      case CalcNeg(e) => s"-(${rcr(e)})"
      case CalcSum(hd :: tl) => tl.foldLeft(rcr(hd))((acc, cur) => s"$acc + ${rcr(cur)}")
      case CalcProd(hd :: tl) => tl.foldLeft(rcr(hd))((acc, cur) => s"$acc * ${rcr(cur)}")
      case AggSum(vars, expr) => vars.foldLeft("AGGSUM([")((acc, cur) => s"$acc + ${prettyprint(cur)}") + s"], ${rcr(expr)})"

    }
  }

  def prettyprint(vt: VarT): String = {
    vt.tp match{
      case IntType => s"${vt.name}: INT"
      case StringType => s"${vt.name}: STRING"
      case FloatType => s"${vt.name}: FLOAT"
      case VarCharType(mx) => s"${vt.name}: VARCHAR(${mx})"
      case DateType => s"${vt.name}: DATE"
    }
  }

}