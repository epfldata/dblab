package ch.epfl.data.dblab.frontend.optimizer

import ch.epfl.data.dblab.frontend.parser.CalcAST._
import ch.epfl.data.dblab.frontend.parser.SQLAST.{ DoubleLiteral, IntLiteral }
import ch.epfl.data.sc.pardis.types._

/**
 * @author Parand Alizadeh
 */

object CalcOptimizer {

  // evaluating constants in product list and remove unnecessary calc expression (0 or 1 in product list)
  def Prod(exprs: List[CalcExpr]): CalcExpr = {

    val (cs, ncs) = exprs.foldLeft[(List[Double], List[CalcExpr])]((Nil, Nil))({
      case ((acc1, acc2), cur) => cur match {
        case CalcValue(ArithConst(IntLiteral(t)))    => (t :: acc1, acc2)
        case CalcValue(ArithConst(DoubleLiteral(t))) => (t :: acc1, acc2)
        case _                                       => (acc1, cur :: acc2)
      }
    })

    var res = 1.0
    if (cs.length > 0)
      res = cs.foldLeft(1.0)((acc, cur) => (acc * cur))

    if (res == 0)
      return CalcValue(ArithConst(DoubleLiteral(0.0)))
    else if (res == 1.0)
      return CalcProd(ncs)

    if (ncs.length > 0)
      return CalcProd(CalcValue(ArithConst(DoubleLiteral(res))) :: ncs)
    else
      return CalcValue(ArithConst(DoubleLiteral(res)))

  }

  def Sum(exprs: List[CalcExpr]): CalcExpr = {
    val elems = exprs.foldLeft[(List[CalcExpr])]((Nil))({
      case (acc1, cur) => cur match {
        case CalcValue(ArithConst(IntLiteral(0)))      => (acc1)
        case CalcValue(ArithConst(DoubleLiteral(0.0))) => (acc1)
        case _                                         => (cur :: acc1)
      }
    })

    if (elems.length > 0)
      return CalcSum(elems)
    else
      return CalcSum(List(CalcValue(ArithConst(IntLiteral(0)))))

  }

  def Neg(expr: CalcExpr): CalcExpr = {
    return expr match {
      case CalcProd(list) => Prod(CalcValue(ArithConst(IntLiteral(-1))) :: list)
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

  def Fold(sumFun: List[(List[VarT], List[VarT])] => (List[VarT], List[VarT]), prodFun: List[(List[VarT], List[VarT])] => (List[VarT], List[VarT]), negFun: ((List[VarT], List[VarT])) => (List[VarT], List[VarT]), leafFun: CalcExpr => (List[VarT], List[VarT]), expr: CalcExpr): (List[VarT], List[VarT]) = {

    def rcr(expr: CalcExpr): (List[VarT], List[VarT]) = {
      return Fold(sumFun, prodFun, negFun, leafFun, expr)
    }
    return expr match {
      case CalcSum(list)  => sumFun(list.map(x => rcr(x)))
      case CalcProd(list) => prodFun(list.map(x => rcr(x)))
      case CalcNeg(e)     => Fold(sumFun, prodFun, negFun, leafFun, e)
      case _              => leafFun(expr)
    }
  }

  def FoldOfVars(sum: List[List[VarT]] => List[VarT], prod: List[List[VarT]] => List[VarT], neg: List[VarT] => List[VarT], leaf: ArithExpr => List[VarT], expr: ArithExpr): List[VarT] = {

    def rcr(expr: ArithExpr): List[VarT] = {
      return FoldOfVars(sum, prod, neg, leaf, expr)
    }

    return expr match {
      case ArithSum(list)  => sum(list.map(x => rcr(x)))
      case ArithProd(list) => prod(list.map(x => rcr(x)))
      case ArithNeg(e)     => neg(rcr(e))
      case _               => leaf(expr)
    }
  }

  def varsOfValue(expr: ArithExpr): List[VarT] = {
    def multiunion(list: List[List[VarT]]): List[VarT] = {
      return list.foldLeft(List.empty[VarT])((acc, cur) => acc.toSet.union(cur.toSet).toList)
    }
    def leaf(arithExpr: ArithExpr): List[VarT] = {
      return arithExpr match {
        case ArithConst(_)       => List()
        case ArithVar(v)         => List(v)
        case ArithFunc(_, tr, _) => multiunion(tr.map(x => varsOfValue(x)))
      }
    }
    return FoldOfVars(multiunion, multiunion, (x => x), leaf, expr)

  }
  def SchemaOfExpression(expr: CalcExpr): (List[VarT], List[VarT]) = {

    def sum(sumlist: List[(List[VarT], List[VarT])]): (List[VarT], List[VarT]) = {
      val (ivars, ovars) = sumlist.unzip
      val oldivars = ivars.foldLeft(List.empty[VarT])((acc, cur) => acc.toSet.union(cur.toSet).toList)
      val oldovars = ovars.foldLeft(List.empty[VarT])((acc, cur) => acc.toSet.union(cur.toSet).toList)
      val newivars = oldovars.toSet.diff(ovars.foldLeft(Set.empty[VarT])((acc, cur) => acc.intersect(cur.toSet))).toList

      return (oldivars.toSet.union(newivars.toSet).toList, oldovars.toSet.diff(newivars.toSet).toList)
    }

    def prod(prodList: List[(List[VarT], List[VarT])]): (List[VarT], List[VarT]) = {
      return prodList.foldLeft((List.empty[VarT], List.empty[VarT]))((oldvars, newvars) => (oldvars._1.toSet.union(newvars._1.toSet.diff(oldvars._2.toSet)).toList, oldvars._2.toSet.union(newvars._2.toSet).diff(oldvars._1.toSet).toList))

    }

    def negSch(varTs: (List[VarT], List[VarT])): (List[VarT], List[VarT]) = { return varTs }

    def leafSch(calcExpr: CalcExpr): (List[VarT], List[VarT]) = {

      def lift(target: VarT, expr: CalcExpr): (List[VarT], List[VarT]) = {
        val (ivars, ovars) = SchemaOfExpression(expr)
        return (ivars.toSet.union(ovars.toSet).toList, List(target))
      }

      def aggsum(gbvars: List[VarT], subexp: CalcExpr): (List[VarT], List[VarT]) = {
        val (ivars, ovars) = SchemaOfExpression(subexp)
        val trimmedGbVars = ovars.toSet.intersect(gbvars.toSet).toList
        if (!(trimmedGbVars.equals(gbvars)))
          throw new Exception
        else
          return (ivars, gbvars)
      }

      return calcExpr match {
        case CalcValue(v)                   => (varsOfValue(v), List())
        case External(_, eins, eouts, _, _) => (eins, eouts)
        case AggSum(gbvars, subexp)         => { aggsum(gbvars, subexp) }
        case Rel("Rel", _, rvars, _)        => (List.empty[VarT], rvars)
        case Cmp(_, v1, v2)                 => (varsOfValue(v1).toSet.union(varsOfValue(v2).toSet).toList, List())
        case CmpOrList(v, _)                => (varsOfValue(v), List())
        case Lift(target, subexpr)          => lift(target, subexpr)
        case Exists(expr)                   => SchemaOfExpression(expr)

      }
    }
    return Fold(sum, prod, negSch, leafSch, expr)

  }
  def rewrite(expression: CalcExpr, sumFunc: List[CalcExpr] => CalcExpr, prodFunc: List[CalcExpr] => CalcExpr, negFunc: CalcExpr => CalcExpr, leafFunc: CalcExpr => CalcExpr): CalcExpr = {
    return expression match {
      case CalcProd(list)  => prodFunc(list.foldLeft(List.empty[CalcExpr])((acc, cur) => rewrite(cur, sumFunc, prodFunc, negFunc, leafFunc) :: acc))
      case CalcSum(list)   => sumFunc(list.foldLeft(List.empty[CalcExpr])((acc, cur) => rewrite(cur, sumFunc, prodFunc, negFunc, leafFunc) :: acc))
      case CalcNeg(expr)   => rewrite(negFunc(expr), sumFunc, prodFunc, negFunc, leafFunc)
      case AggSum(t, expr) => AggSum(t, rewrite(expr, sumFunc, prodFunc, negFunc, leafFunc))
      //case External(expr, t) => External(rewrite(expr, sumFunc, prodFunc, negFunc, leafFunc), t)
      case Lift(t, expr)   => Lift(t, rewrite(expr, sumFunc, prodFunc, negFunc, leafFunc))
      case Exists(expr)    => Exists(rewrite(expr, sumFunc, prodFunc, negFunc, leafFunc))
      case _               => leafFunc(expression)
    }
  }

  def nestingRewrites(expr: CalcExpr): CalcExpr = {
    return expr match {
      case AggSum(vars, CalcValue(ArithConst(IntLiteral(0)))) => CalcValue(ArithConst(IntLiteral(0)))
      case AggSum(vars, CalcSum(list))                        => CalcSum(list.foldLeft(List.empty[CalcExpr])((acc, cur) => AggSum(vars, cur) :: acc))
      case AggSum(vars, CalcValue(ArithConst(t)))             => CalcValue(ArithConst(t))

    }
  }
}
