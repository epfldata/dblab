package ch.epfl.data.dblab.frontend.optimizer

import ch.epfl.data.dblab.frontend.parser.CalcAST._
import ch.epfl.data.dblab.frontend.parser.SQLAST.{DoubleLiteral, IntLiteral}

/**
  * @author Parand Alizadeh
  */

object CalcOptimizer {




  // evaluating constants in product list and remove unnecessary calc expression (0 or 1 in product list)
  def Prod(calcProd: CalcProd): CalcExpr = {
    var consts: List[Int] = List()
    var nonconsts: List[CalcExpr] = List()


    calcProd.exprs.foldLeft(consts, nonconsts){
      case ((acc1, acc2), cur) => cur match {
        case CalcValue(ArithConst(IntLiteral(t))) => (t::consts, nonconsts)
          //TODO
        //case CalcValue(ArithConst(DoubleLiteral(t))) => (t::consts, nonconsts)
        case _ => (consts, cur :: nonconsts)
      }
    }

    var res = 1
    if (consts.length > 0)
      res = consts.foldLeft(1)((acc, cur) => (acc * cur))

    if (res == 0)
      return CalcValue(ArithConst(IntLiteral(0)))
    else if (res == 1)
      return CalcProd(nonconsts)

    return CalcProd(CalcValue(ArithConst(DoubleLiteral(res)))::nonconsts)
  }


  def Sum(calcSum: CalcSum): CalcExpr = {
    var consts: List[Int] = List()
    var nonconsts: List[CalcExpr] = List()


    calcSum.exprs.foldLeft(consts, nonconsts){
      case ((acc1, acc2), cur) => cur match {
        case CalcValue(ArithConst(IntLiteral(t))) => (t::consts, nonconsts)
        //TODO
        //case CalcValue(ArithConst(DoubleLiteral(t))) => (t::consts, nonconsts)
        case _ => (consts, cur :: nonconsts)
      }
    }

    var res = 0
    if (consts.length > 0)
      res = consts.foldLeft(0)((acc, cur) => (acc + cur))

    if (res == 0)
      return CalcSum(nonconsts)
    return CalcSum(CalcValue(ArithConst(DoubleLiteral(res)))::nonconsts)
  }

  def Neg(expr: CalcExpr): CalcExpr = {
    case CalcProd(list) => Prod(CalcProd(CalcValue(ArithConst(IntLiteral(1))) :: list))
    case _ => CalcProd(CalcValue(ArithConst(IntLiteral(1))) :: List(expr))
  }

  def Value(expr: CalcExpr): CalcExpr = {
    return expr
  }

  /* Normalize a given expression by replacing all Negs with {-1} and
    evaluating all constants in the product list. */
  def Normalize(expr: CalcExpr): CalcExpr = {
    return rewrite(expr,Sum, Prod, Neg, Value)
  }

  def rewrite(expr: CalcExpr, sumFunc: CalcSum => CalcExpr, prodFunc: CalcProd => CalcExpr, negFunc: CalcExpr => CalcExpr, leafFunc: CalcExpr => CalcExpr ): CalcExpr = {
    ???
  }

}
