package ch.epfl.data
package dblab
package frontend
package parser

import ch.epfl.data.dblab.schema.{ DateType, VarCharType }
import sc.pardis.types._

/**
 * A module containing AST nodes for AG
 * CA constructs.
 *
 * @author Mohsen Ferdosi
 * @author Parand Alizadeh
 *
 */
object CalcAST {

  var tmpVarNum = 0

  def getTmpVar(tpe: Tpe): VarT = {
    tmpVarNum = tmpVarNum + 1
    return VarT("tmpvar" + tmpVarNum, tpe)
  }

  trait CalcExpr
  case class CalcQuery(name: String, expr: CalcExpr) extends CalcExpr
  case class CalcSum(exprs: List[CalcExpr]) extends CalcExpr
  case class CalcProd(exprs: List[CalcExpr]) extends CalcExpr
  case class CalcNeg(expr: CalcExpr) extends CalcExpr
  case class AggSum(vars: List[VarT], expr: CalcExpr) extends CalcExpr
  case class Rel(tag: String, name: String, vars: List[VarT], rest: String) extends CalcExpr
  case class Cmp(cmp: CmpTag, first: ArithExpr, second: ArithExpr) extends CalcExpr
  case class External(name: String, inps: List[VarT], outs: List[VarT], tp: Tpe, meta: Option[CalcExpr]) extends CalcExpr
  case class CmpOrList(v: ArithExpr, consts: List[ArithConst]) extends CalcExpr
  case class Lift(vr: VarT, expr: CalcExpr) extends CalcExpr
  case class Exists(term: CalcExpr) extends CalcExpr
  case class CalcValue(v: ArithExpr) extends CalcExpr

  //TODO add const one and zero

  trait ArithExpr
  case class ArithSum(expr: List[ArithExpr]) extends ArithExpr
  case class ArithProd(expr: List[ArithExpr]) extends ArithExpr
  case class ArithNeg(expr: ArithExpr) extends ArithExpr
  case class ArithConst(lit: SQLAST.LiteralExpression) extends ArithExpr
  case class ArithVar(v: VarT) extends ArithExpr
  case class ArithFunc(name: String, terms: List[ArithExpr], tp: Tpe) extends ArithExpr

  case class VarT(name: String, tp: Tpe)

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
    calcExpr match {
      case CalcQuery(id, expr) => s"${id}:\n ${rcr(expr)}"
      case CalcNeg(e)          => s"-(${rcr(e)})"
      case CalcSum(list)       => s"(${list.map(rcr).mkString(" + ")})"
      case CalcProd(list)      => s"(${list.map(rcr).mkString(" * ")})"
      case AggSum(list, expr)  => s"AGGSUM([${list.map(pprint).mkString(", ")}], ${rcr(expr)})"
      case Rel(tag, name, vars, rest) => tag match {
        case "TABLE" | "STREAM" => s"CREATE ${tag} ${name} ( ${vars.map(pprint).mkString(", ")})" + (if (rest == "") s"" else s"FROM ${rest}")
        case "Rel"              => s"${name}(${vars.map(pprint).mkString(", ")})"
      }
      case Lift(vr, expr) => s"(${pprint(vr)} ^= ${rcr(expr)})"
      case Exists(expr)   => s"EXISTS (${rcr(expr)}) "
      case External(name, inps, outs, tp, meta) => meta match {
        case None           => s"${name}[${inps.map(pprint).mkString(", ")}][${outs.map(pprint).mkString(", ")}]"
        case Some(calcExpr) => s"${name}[${inps.map(pprint).mkString(", ")}][${outs.map(pprint).mkString(", ")}]:(${rcr(calcExpr)})"
      }
      case Cmp(cmp, first, second) => s"{${pprint(first)} ${pprint(cmp)} ${pprint(second)}}"
      case CalcValue(v)            => s"{${pprint(v)}}"
      case CmpOrList(first, list)  => s"{${pprint(first)} IN [${list.map(pprint).mkString(", ")}]}"
      case _                       => ""

    }
  }

  def pprint(vt: VarT): String = {
    vt.tp match {
      case IntType         => s"${vt.name}: INT"
      case StringType      => s"${vt.name}: STRING"
      case FloatType       => s"${vt.name}: FLOAT"
      case VarCharType(mx) => s"${vt.name}: VARCHAR(${mx})"
      case DateType        => s"${vt.name}: DATE"
      case null            => s"${vt.name}"
    }
  }

  def pprint(exp: ArithExpr): String = {
    exp match {
      case ArithSum(list)             => s"${list.map(pprint).mkString(" + ")}"
      case ArithProd(list)            => s"${list.map(pprint).mkString(" * ")}"
      case ArithNeg(expr)             => s"-(${pprint(expr)})"
      case ArithConst(lit)            => lit.toString
      case ArithVar(varT)             => pprint(varT)
      case ArithFunc(name, list, tpe) => s"[${name}: ${pprint(tpe)}](${list.map(pprint).mkString(", ")})"
    }
  }
  def pprint(cmp_t: CmpTag): String = {
    cmp_t match {
      case Eq  => "="
      case Neq => "!="
      case Lt  => "<"
      case Lte => "<="
      case Gt  => ">"
      case Gte => ">="
    }
  }
  def pprint(tpe: Tpe): String = {
    tpe match {
      case IntType          => s"INT"
      case DoubleType       => s"FLOAT"
      case DateType         => s"DATE"
      case StringType       => s"STRING"
      case VarCharType(num) => s"VARCHAR(${num})"
    }
  }

}