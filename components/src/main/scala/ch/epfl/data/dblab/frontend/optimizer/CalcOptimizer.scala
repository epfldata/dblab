package ch.epfl.data.dblab.frontend.optimizer

import ch.epfl.data.dblab.frontend.parser.CalcAST._
import ch.epfl.data.dblab.frontend.parser.SQLAST.{ DoubleLiteral, IntLiteral }

/**
 * @author Parand Alizadeh
 */

object CalcOptimizer {

  // evaluating constants in product list and remove unnecessary calc expression (0 or 1 in product list)
  def Prod(calcProd: CalcProd): CalcExpr = {
    var consts: List[Int] = List()
    var nonconsts: List[CalcExpr] = List()

    consts = calcProd.exprs.foldLeft(consts) {
      case (acc1, cur) => cur match {
        case CalcValue(ArithConst(IntLiteral(t))) => (t :: acc1)
        //TODO
        //case CalcValue(ArithConst(DoubleLiteral(t))) => (t::consts, nonconsts)
        case _                                    => (acc1)
      }
    }

    nonconsts = calcProd.exprs.foldLeft(nonconsts) {
      case (acc1, cur) => cur match {
        case CalcValue(ArithConst(IntLiteral(t))) => (acc1)
        //TODO
        //case CalcValue(ArithConst(DoubleLiteral(t))) => (t::consts, nonconsts)
        case _                                    => (cur :: acc1)
      }
    }

    var res = 1
    if (consts.length > 0)
      res = consts.foldLeft(1)((acc, cur) => (acc * cur))

    if (res == 0)
      return CalcValue(ArithConst(IntLiteral(0)))
    else if (res == 1)
      return CalcProd(nonconsts)

    if (nonconsts.length > 0)
      return CalcProd(CalcValue(ArithConst(IntLiteral(res))) :: nonconsts)
    else
      return CalcValue(ArithConst(IntLiteral(res)))

  }

  def Sum(calcSum: CalcSum): CalcExpr = {
    var consts: List[Int] = List()
    var nonconsts: List[CalcExpr] = List()

    consts = calcSum.exprs.foldLeft(consts) {
      case (acc1, cur) => cur match {
        case CalcValue(ArithConst(IntLiteral(t))) => (t :: acc1)
        //TODO
        //case CalcValue(ArithConst(DoubleLiteral(t))) => (t::consts, nonconsts)
        case _                                    => (acc1)
      }
    }

    nonconsts = calcSum.exprs.foldLeft(nonconsts) {
      case (acc1, cur) => cur match {
        case CalcValue(ArithConst(IntLiteral(t))) => (acc1)
        //TODO
        //case CalcValue(ArithConst(DoubleLiteral(t))) => (t::consts, nonconsts)
        case _                                    => (cur :: acc1)
      }
    }

    var res = 0
    if (consts.length > 0)
      res = consts.foldLeft(0)((acc, cur) => (acc + cur))

    if (res == 0)
      return CalcSum(nonconsts)
    if (nonconsts.length > 0)
      return CalcSum(CalcValue(ArithConst(IntLiteral(res))) :: nonconsts)
    else
      return CalcValue(ArithConst(IntLiteral(res)))
  }

  def Neg(expr: CalcExpr): CalcExpr = {
    return expr match {
      case CalcProd(list) => Prod(CalcProd(CalcValue(ArithConst(IntLiteral(-1))) :: list))
      case _              => CalcProd(CalcValue(ArithConst(IntLiteral(-1))) :: List(expr))
    }
  }

  def Value(expr: CalcExpr): CalcExpr = {
    return expr
  }

  /* Normalize a given expression by replacing all Negs with {-1} and
    evaluating all constants in the product list. */
  def Normalize(expr: CalcExpr): CalcExpr = {
    return rewrite(expr, Sum, Prod, Neg, Value)
  }

  def rewrite(expression: CalcExpr, sumFunc: CalcSum => CalcExpr, prodFunc: CalcProd => CalcExpr, negFunc: CalcExpr => CalcExpr, leafFunc: CalcExpr => CalcExpr): CalcExpr = {
    return expression match {
      case CalcProd(list)    => prodFunc(CalcProd(list.foldLeft(List.empty[CalcExpr])((acc, cur) => rewrite(cur, sumFunc, prodFunc, negFunc, leafFunc) :: acc)))
      case CalcSum(list)     => sumFunc(CalcSum(list.foldLeft(List.empty[CalcExpr])((acc, cur) => rewrite(cur, sumFunc, prodFunc, negFunc, leafFunc) :: acc)))
      case CalcNeg(expr)     => rewrite(negFunc(expr), sumFunc, prodFunc, negFunc, leafFunc)
      case AggSum(t, expr)   => AggSum(t, rewrite(expr, sumFunc, prodFunc, negFunc, leafFunc))
      case External(expr, t) => External(rewrite(expr, sumFunc, prodFunc, negFunc, leafFunc), t)
      case Lift(t, expr)     => Lift(t, rewrite(expr, sumFunc, prodFunc, negFunc, leafFunc))
      case Exists(expr)      => Exists(rewrite(expr, sumFunc, prodFunc, negFunc, leafFunc))
      case _                 => expression
    }
  }

}
