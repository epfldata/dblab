package ch.epfl.data
package dblab
package frontend
package parser

import ch.epfl.data.dblab.frontend.parser.SQLAST.IntLiteral
import ch.epfl.data.dblab.schema.{ DateType, VarCharType }
import sc.pardis.types._
import sc.pardis.ast._
import ch.epfl.data.dblab.schema._

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
    VarT("tmpvar" + tmpVarNum, tpe)
  }

  sealed trait CalcExpr extends Node {
    type NO = CalcExprOps
    def nodeOps: NO = new CalcExprOps(this)
  }
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

  case class DeltaRel(name: String, vars: List[VarT]) extends CalcExpr
  case class DomainDelta(term: CalcExpr) extends CalcExpr
  //TODO fix all previous utils for delta rel and domain delta

  val CalcOne = CalcValue(ArithConst(IntLiteral(1)))
  val CalcZero = CalcValue(ArithConst(IntLiteral(0)))

  case class Ds(dsname: CalcExpr, dsdef: CalcExpr)
  case class TodoT(depth: Int, ds: Ds, b: Boolean)
  trait EventT
  case class InsertEvent(tab: Table) extends EventT
  case class DeleteEvent(tab: Table) extends EventT
  case class BatchUpdate(str: String) extends EventT
  case class CorrectiveUpdate(str: String, l1: List[VarT], l2: List[VarT], v: VarT, e: EventT) extends EventT
  case class SystemInitializedEvent() extends EventT

  trait UpdateType
  case object UpdateStmt extends UpdateType
  case object ReplaceStmt extends UpdateType
  case class StmtT(targetMap: CalcExpr, updateType: UpdateType, updateExpr: CalcExpr)
  case class Trigger(event: EventT, stmt: StmtT)
  case class CompiledDs(description: Ds, triggers: List[Trigger])
  case class Plan(list: List[CompiledDs])

  sealed trait ArithExpr extends CalcExpr
  case class ArithSum(expr: List[ArithExpr]) extends ArithExpr
  case class ArithProd(expr: List[ArithExpr]) extends ArithExpr
  case class ArithNeg(expr: ArithExpr) extends ArithExpr
  case class ArithConst(lit: SQLAST.LiteralExpression) extends ArithExpr
  case class ArithVar(v: VarT) extends ArithExpr
  case class ArithFunc(name: String, terms: List[ArithExpr], tp: Tpe) extends ArithExpr

  case class VarT(name: String, tp: Tpe)
  case class Schema_t(scope: Option[List[VarT]], schema: Option[List[VarT]])
  case class SchemaT(scope: List[VarT], schema: List[VarT])

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
      case ae: ArithExpr           => pprint(ae)
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
      case DoubleType       => s"FLOAT" //TODO ok ?
      case FloatType        => s"FLOAT" //TODO added by Mohsen
      case DateType         => s"DATE"
      case StringType       => s"STRING"
      case VarCharType(num) => s"VARCHAR(${num})"
    }
  }

  class CalcExprOps(val node: CalcExpr) extends NodeOps {
    type N = CalcExpr
    def children: List[CalcExpr] = CalcExprShape.unapply(node).get._2.toList
    def rebuild: CalcExpr = RebuildCalcTransformer.transform(node)
    def recreate(children: List[Node]): CalcExpr = {
      val (fact, _) = CalcExprShape.unapply(node).get
      fact(children.map(_.asInstanceOf[CalcExpr]))
    }
  }

  object RebuildCalcTransformer extends optimizer.CalcTransformer {
    override def transform(n: CalcExpr): CalcExpr = {
      val newNode = super.transform(n)
      val newSymbol = NodeSymbol(newNode)
      if (n.symbol != null) {
        val oldProps = n.symbol.properties
        for ((pf, p) <- oldProps if pf.constant) {
          newSymbol.updateProperty(p)
        }
      }
      newNode.symbol = newSymbol
      newNode
    }
  }

  object CalcExprShape {
    type CalcExprFact = Seq[CalcExpr] => CalcExpr
    def unapply(expr: CalcExpr): Option[(CalcExprFact, Seq[CalcExpr])] = {
      def arithChild(l: Seq[CalcExpr], i: Int) = l(i).asInstanceOf[ArithExpr]
      expr match {
        case CalcQuery(name, e)          => Some(l => CalcQuery(name, l(0)), Seq(e))
        case CalcSum(es)                 => Some(l => CalcSum(l.toList), es)
        case CalcProd(es)                => Some(l => CalcProd(l.toList), es)
        case CalcNeg(e)                  => Some(l => CalcNeg(l(0)), Seq(e))
        case AggSum(vs, e)               => Some(l => AggSum(vs, l(0)), Seq(e))
        case Rel(t, n, vs, r)            => Some(l => Rel(t, n, vs, r), Seq())
        case Cmp(t, e1, e2)              => Some(l => Cmp(t, arithChild(l, 0), arithChild(l, 1)), Seq(e1, e2))
        case External(n, is, os, tp, me) => Some(l => External(n, is, os, tp, l.headOption), me.toSeq)
        case CmpOrList(v, cs) =>
          Some(l => CmpOrList(arithChild(l, 0), l.tail.toList.map(_.asInstanceOf[ArithConst])), Seq(v) ++ cs)
        case Lift(v, e)           => Some(l => Lift(v, l(0)), Seq(e))
        case Exists(e)            => Some(l => Exists(l(0)), Seq(e))
        case CalcValue(a)         => Some(l => CalcValue(arithChild(l, 0)), Seq(a))
        case ArithSum(es)         => Some(l => ArithSum(l.map(_.asInstanceOf[ArithExpr]).toList), es)
        case ArithProd(es)        => Some(l => ArithProd(l.map(_.asInstanceOf[ArithExpr]).toList), es)
        case ArithNeg(e)          => Some(l => ArithNeg(arithChild(l, 0)), Seq(e))
        case ArithConst(c)        => Some(l => ArithConst(c), Seq())
        case ArithVar(v)          => Some(l => ArithVar(v), Seq())
        case ArithFunc(n, ts, tp) => Some(l => ArithFunc(n, l.map(_.asInstanceOf[ArithExpr]).toList, tp), ts)
      }
    }
  }

}