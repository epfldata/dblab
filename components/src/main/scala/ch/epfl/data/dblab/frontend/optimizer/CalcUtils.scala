package ch.epfl.data.dblab.frontend.optimizer

import ch.epfl.data.dblab.frontend.parser.CalcAST._
import ch.epfl.data.dblab.frontend.parser.{ CalcAST, SQLAST }
import ch.epfl.data.dblab.frontend.parser.SQLAST._
import ch.epfl.data.dblab.schema.DateType
import ch.epfl.data.sc.pardis.types._

/**
 * @author Parand Alizadeh
 */

object CalcUtils {
  def escalateType(a: Tpe, b: Tpe): Tpe = {
    (a, b) match {
      case (at, bt) if (at.equals(bt)) => at
      case (t, AnyType)                => t
      case (AnyType, t)                => t
      case (IntType, BooleanType)      => IntType
      case (BooleanType, IntType)      => IntType
      case (IntType, FloatType)        => FloatType
      case (FloatType, IntType)        => FloatType
      case _                           => AnyType

    }
  }

  def escalateTypeList(list: List[Tpe]): Tpe = {
    if (list.isEmpty)
      return IntType
    list.tail.foldLeft(list.head)((acc, cur) => escalateType(acc, cur))
  }

  def typeOfConst(expression: SQLAST.LiteralExpression): Tpe = {
    expression match {
      case IntLiteral(_)    => IntType
      case DoubleLiteral(_) => DoubleType
      case FloatLiteral(_)  => FloatType
      case StringLiteral(_) => StringType
      case DateLiteral(_)   => DateType
      case CharLiteral(_)   => CharType
      case _                => AnyType
    }
  }

  def typeOfValue(expr: ArithExpr): Tpe = {
    def neg(tpe: Tpe): Tpe = {
      tpe match {
        case IntType   => tpe
        case FloatType => tpe
        case _         => throw new Exception
      }
    }
    expr match {
      case ArithSum(sum)          => escalateTypeList(sum.map(x => typeOfValue(x)))
      case ArithProd(prod)        => escalateTypeList(prod.map(x => typeOfValue(x)))
      case ArithNeg(n)            => neg(typeOfValue(n))
      case ArithConst(c)          => typeOfConst(c)
      case ArithVar(t)            => t.tp
      case ArithFunc(_, inps, tp) => tp
    }

  }

  def typeOfExpression(expr: CalcExpr): Tpe = {
    def leaf(calcExpr: CalcExpr): Tpe = {
      calcExpr match {
        case CalcValue(v)            => typeOfValue(v)
        case External(_, _, _, e, _) => e
        case AggSum(_, sub)          => typeOfExpression(sub)
        case Rel(_, _, _, _)         => IntType
        case Cmp(_, _, _)            => IntType
        case CmpOrList(_, _)         => IntType
        case Lift(_, _)              => IntType
        case CalcAST.Exists(_)       => IntType
        case _                       => IntType
      }
    }

    def neg(tpe: Tpe): Tpe = tpe
    return fold(escalateTypeList, escalateTypeList, neg, leaf, expr)
  }

  def relsOfExpr(expr: CalcExpr): List[String] = {

    def multiunion(list: List[List[String]]): List[String] = {
      return list.foldLeft(List.empty[String])((acc, cur) => acc.toSet.union(cur.toSet).toList)
    }
    def leaf(calcExpr: CalcExpr): List[String] = {
      calcExpr match {
        case CalcValue(_)                  => List()
        case External(_, _, _, _, None)    => List()
        case External(_, _, _, _, Some(e)) => relsOfExpr(e)
        case AggSum(_, sub)                => relsOfExpr(sub)
        case Rel(_, rn, _, _)              => List(rn)
        case CmpOrList(_, _)               => List()
        case Lift(_, sub)                  => relsOfExpr(sub)
        case CalcAST.Exists(sub)           => relsOfExpr(sub)
        case Cmp(_, _, _)                  => List()
      }
    }

    def neg(list: List[String]): List[String] = list
    fold(multiunion, multiunion, neg, leaf, expr)
  }

  def exprHasDeltaRels(expr: CalcExpr): Boolean = {
    expr match {
      case CalcProd(pl)               => pl.map(x => exprHasDeltaRels(x)).exists(x => x)
      case CalcSum(sl)                => sl.map(x => exprHasDeltaRels(x)).forall(x => x)
      case CalcNeg(e)                 => exprHasDeltaRels(e)
      case CalcValue(_)               => false
      case Cmp(_, _, _)               => false
      case CmpOrList(_, _)            => false
      case Rel(_, _, _, _)            => false
      case AggSum(_, sub)             => exprHasDeltaRels(sub)
      case Lift(_, sub)               => exprHasDeltaRels(sub)
      case CalcAST.Exists(sub)        => exprHasDeltaRels(sub)
      case External(name, _, _, _, _) => (name.contains("DELTA") || name.contains("DOMAIN"))
    }
  }

  def exprHasLowCardinality(expr: CalcExpr): Boolean = exprHasDeltaRels(expr)

  def exprHasHighCardinality(expr: CalcExpr): Boolean = {
    (!exprHasLowCardinality(expr)) && (relsOfExpr(expr).size != 0)
  }
  /**
   * Determine whether two expressions safely commute (in a product).
   *
   * param scope  (optional) The scope in which the expression is evaluated
   * (having a subset of the scope may produce false negatives)
   * param e1     The left hand side expression
   * param e2     The right hand side expression
   * return       true if and only if e1 * e2 = e2 * e1
   */

  def commutes(expr1: CalcExpr, expr2: CalcExpr): Boolean = {
    val (_, ovars1) = schemaOfExpression(expr1)
    val (ivars2, _) = schemaOfExpression(expr2)

    return (ivars2.toSet.intersect(ovars1.toSet).isEmpty && !((exprHasLowCardinality(expr1)) && exprHasHighCardinality(expr2)))

  }

  //Ring Fold
  def fold[A](sumFun: List[A] => A, prodFun: List[A] => A, negFun: A => A, leafFun: CalcExpr => A, expr: CalcExpr): A = {

    def rcr(expr: CalcExpr): A = {
      fold(sumFun, prodFun, negFun, leafFun, expr)
    }
    expr match {
      case CalcSum(list)  => sumFun(list.map(x => rcr(x)))
      case CalcProd(list) => prodFun(list.map(x => rcr(x)))
      case CalcNeg(e)     => negFun(rcr(e))
      case _ => {
        leafFun(expr)
      }
    }
  }

  def foldOfVars(sum: List[List[VarT]] => List[VarT], prod: List[List[VarT]] => List[VarT], neg: List[VarT] => List[VarT], leaf: ArithExpr => List[VarT], expr: ArithExpr): List[VarT] = {

    def rcr(expr: ArithExpr): List[VarT] = {

      return foldOfVars(sum, prod, neg, leaf, expr)
    }

    expr match {
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
      arithExpr match {
        case ArithConst(_)       => List()
        case ArithVar(v)         => List(v)
        case ArithFunc(_, tr, _) => multiunion(tr.map(x => varsOfValue(x)))
      }
    }
    return foldOfVars(multiunion, multiunion, (x => x), leaf, expr)

  }
  def schemaOfExpression(expr: CalcExpr): (List[VarT], List[VarT]) = {

    def sum(sumlist: List[(List[VarT], List[VarT])]): (List[VarT], List[VarT]) = {

      val (ivars, ovars) = sumlist.unzip
      val oldivars = ivars.foldLeft(List.empty[VarT])((acc, cur) => acc.toSet.union(cur.toSet).toList)
      val oldovars = ovars.foldLeft(List.empty[VarT])((acc, cur) => acc.toSet.union(cur.toSet).toList)
      val newivars = oldovars.toSet.diff(ovars.foldLeft(Set.empty[VarT])((acc, cur) => acc.intersect(cur.toSet))).toList

      return (oldivars.toSet.union(newivars.toSet).toList, oldovars.toSet.diff(newivars.toSet).toList)
    }

    def prod(prodList: List[(List[VarT], List[VarT])]): (List[VarT], List[VarT]) = {

      return prodList.foldLeft((List.empty[VarT], List.empty[VarT]))((oldvars, newvars) => ((oldvars._1.toSet).union(newvars._1.toSet.diff(oldvars._2.toSet)).toList, (oldvars._2.toSet.union(newvars._2.toSet)).diff(oldvars._1.toSet).toList))

    }

    def negSch(varTs: (List[VarT], List[VarT])): (List[VarT], List[VarT]) = { return varTs }

    def leafSch(calcExpr: CalcExpr): (List[VarT], List[VarT]) = {

      def lift(target: VarT, expr: CalcExpr): (List[VarT], List[VarT]) = {
        val (ivars, ovars) = schemaOfExpression(expr)
        return (ivars.toSet.union(ovars.toSet).toList, List(target))
      }

      def aggsum(gbvars: List[VarT], subexp: CalcExpr): (List[VarT], List[VarT]) = {

        val (ivars, ovars) = schemaOfExpression(subexp)
        val trimmedGbVars = ovars.toSet.intersect(gbvars.toSet).toList
        if (!(trimmedGbVars.equals(gbvars)))
          throw new Exception
        else
          return (ivars, gbvars)
      }

      calcExpr match {
        case CalcValue(v)                   => (varsOfValue(v), List())
        case External(_, eins, eouts, _, _) => (eins, eouts)
        case AggSum(gbvars, subexp)         => { aggsum(gbvars, subexp) }
        case Rel("Rel", _, rvars, _)        => (List(), rvars)
        case Cmp(_, v1, v2)                 => (varsOfValue(v1).toSet.union(varsOfValue(v2).toSet).toList, List())
        case CmpOrList(v, _)                => (varsOfValue(v), List())
        case Lift(target, subexpr)          => lift(target, subexpr)
        case CalcAST.Exists(expr)           => schemaOfExpression(expr)
        // case _                              => (List(), List())

      }
    }
    fold(sum, prod, negSch, leafSch, expr)

  }
  def rewrite(expression: CalcExpr, sumFunc: List[CalcExpr] => CalcExpr, prodFunc: List[CalcExpr] => CalcExpr, negFunc: CalcExpr => CalcExpr, leafFunc: CalcExpr => CalcExpr): CalcExpr = {
    expression match {
      case CalcQuery(name, expr) => CalcQuery(name, rewrite(expr, sumFunc, prodFunc, negFunc, leafFunc))
      case CalcProd(list)        => prodFunc(list.foldLeft(List.empty[CalcExpr])((acc, cur) => acc :+ rewrite(cur, sumFunc, prodFunc, negFunc, leafFunc)))
      case CalcSum(list)         => sumFunc(list.foldLeft(List.empty[CalcExpr])((acc, cur) => acc :+ rewrite(cur, sumFunc, prodFunc, negFunc, leafFunc)))
      case CalcNeg(expr)         => rewrite(negFunc(expr), sumFunc, prodFunc, negFunc, leafFunc)
      case AggSum(t, expr)       => AggSum(t, rewrite(expr, sumFunc, prodFunc, negFunc, leafFunc))
      case External(name, inps, outs, tp, meta) => meta match {
        case Some(expr) => External(name, inps, outs, tp, Some(rewrite(expr, sumFunc, prodFunc, negFunc, leafFunc)))
        case None       => expression
      }
      case Lift(t, expr)        => Lift(t, rewrite(expr, sumFunc, prodFunc, negFunc, leafFunc))
      case CalcAST.Exists(expr) => CalcAST.Exists(rewrite(expr, sumFunc, prodFunc, negFunc, leafFunc))
      case _                    => leafFunc(expression)
    }
  }

}