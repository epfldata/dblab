package ch.epfl.data.dblab.frontend.optimizer

import ch.epfl.data.dblab.frontend.parser.CalcAST._
import ch.epfl.data.dblab.frontend.parser.{ CalcAST, SQLAST }
import ch.epfl.data.dblab.frontend.parser.SQLAST._
import ch.epfl.data.dblab.schema.DateType
import ch.epfl.data.sc.pardis.types._
import ch.epfl.data.dblab.frontend.optimizer.CalcUtils._
/**
 * @author Parand Alizadeh
 */

object CalcOptimizer {

  // evaluating constants in product list and remove unnecessary calc expression (0 or 1 in product list)
  def prodGeneral(exprs: List[CalcExpr]): CalcExpr = {
    val (cs, ncs) = exprs.foldLeft[(List[Double], List[CalcExpr])]((Nil, Nil))({
      case ((acc1, acc2), cur) => cur match {
        case CalcValue(ArithConst(IntLiteral(t)))    => (t :: acc1, acc2)
        case CalcValue(ArithConst(DoubleLiteral(t))) => (t :: acc1, acc2)
        case _                                       => (acc1, acc2 :+ cur)
      }
    })

    var res = 1.0
    if (cs.length > 0)
      res = cs.foldLeft(1.0)((acc, cur) => (acc * cur))

    val result = if (res == 0)
      CalcValue(ArithConst(DoubleLiteral(0.0)))
    else if (res == 1.0 && ncs.length > 1)
      CalcProd(ncs)
    else if (res == 1.0 && ncs.length > 0)
      ncs.head

    else if (res == 1.0 && ncs.length == 0)
      CalcValue(ArithConst(DoubleLiteral(1.0)))

    else if (ncs.length > 0)
      CalcProd(CalcValue(ArithConst(DoubleLiteral(res))) :: ncs)
    else
      CalcValue(ArithConst(DoubleLiteral(res)))

    result match {
      case CalcProd(list) =>
        val newExprs = list.flatMap(x => x match {
          case CalcProd(l) => l
          case _           => List(x)
        })
        CalcProd(newExprs)
      case _ => result
    }
  }

  def sumGeneral(exprs: List[CalcExpr]): CalcExpr = {
    val elems = exprs.foldLeft[(List[CalcExpr])]((Nil))({
      case (acc1, cur) => cur match {
        case CalcValue(ArithConst(IntLiteral(0)))      => (acc1)
        case CalcValue(ArithConst(DoubleLiteral(0.0))) => (acc1)
        case _                                         => (acc1 :+ cur)
      }
    })

    if (elems.length > 1)
      CalcSum(elems)
    else if (elems.length == 1)
      elems.head
    else
      CalcValue(ArithConst(IntLiteral(0)))

  }

  def negGeneral(expr: CalcExpr): CalcExpr = {
    expr match {
      case CalcProd(list) => prodGeneral(CalcValue(ArithConst(IntLiteral(-1))) :: list)
      case _              => CalcProd(CalcValue(ArithConst(IntLiteral(-1))) :: List(expr))
    }
  }

  /* Normalize a given expression by replacing all Negs with {-1} and
    evaluating all constants in the product list. */
  def normalize(expr: CalcExpr): CalcExpr = {
    expr match {
      case CalcQuery(name, e) =>
        CalcQuery(name, rewrite(e, sumGeneral, prodGeneral, negGeneral, (x => x)))
      case _ =>
        rewrite(expr, sumGeneral, prodGeneral, negGeneral, (x => x))
    }

  }

  def nestingRewrites(bigexpr: CalcExpr): CalcExpr = {

    def leafNest(expr: CalcExpr): CalcExpr = {

      def aggsum(gbvars: List[VarT], unpsubterm: CalcExpr): CalcExpr = {
        val subterm = normalize(rcr(unpsubterm))
        //        val subterm = rcr(unpsubterm)
        if ((schemaOfExpression(subterm)_2).length == 0)
          subterm

        subterm match {
          case CalcSum(list) => {
            val (sumivars, _) = schemaOfExpression(subterm)
            val rewritten = sumGeneral(list.map(term => {
              val (_, termovars) = schemaOfExpression(term)
              val termgbvars = gbvars.toSet.union(sumivars.toSet.intersect(termovars.toSet)).toList
              AggSum(termgbvars, term)
            }))
            rewritten
          }
          case CalcProd(list) => {

            val (unnested, nested) = list.foldLeft[(CalcExpr, CalcExpr)](((CalcValue(ArithConst(IntLiteral(1)))), (CalcValue(ArithConst(IntLiteral(1))))))((acc, cur) => {
              (if (commutes(acc._2, cur) && (schemaOfExpression(cur)._2).toSet.subsetOf(gbvars.toSet)) (CalcProd(List(acc._1, cur)), acc._2) else (acc._1, CalcProd(List(acc._2, cur))))
            })
            val unnestedivars = schemaOfExpression(unnested)._1
            val newgbvars = (schemaOfExpression(nested)._2).toSet.intersect(gbvars.toSet.union(unnestedivars.toSet)).toList
            CalcProd(List(unnested, AggSum(newgbvars, nested)))
          }

          case AggSum(_, t) => {
            AggSum(gbvars, t)
          }
          case _ => {
            if ((schemaOfExpression(subterm)_2).toSet.subsetOf(gbvars.toSet)) {
              subterm
            } else {
              AggSum(gbvars, subterm)
            }
          }
        }
      }

      expr match {
        case AggSum(gbvars, CalcValue(ArithConst(IntLiteral(0))))      => CalcValue(ArithConst(IntLiteral(0)))
        case AggSum(gbvars, CalcValue(ArithConst(DoubleLiteral(0.0)))) => CalcValue(ArithConst(DoubleLiteral(0.0)))
        case AggSum(gbvars, subterm)                                   => aggsum(gbvars, subterm)
        case CalcAST.Exists(subterm) => rcr(subterm) match {
          case CalcValue(ArithConst(IntLiteral(0))) => CalcValue(ArithConst(IntLiteral(0)))
          case CalcValue(ArithConst(IntLiteral(_))) => CalcValue(ArithConst(IntLiteral(1)))
          case CalcValue(ArithConst(_))             => throw new Exception
          case _                                    => CalcAST.Exists(rcr(subterm))
        }

        case Lift(v, term) => {
          val nested = rcr(term)
          val (nestedivars, nestedovars) = schemaOfExpression(nested)
          if (nestedovars.contains(v))
            throw new Exception
          else if (nestedivars.contains(v)) {
            nested match {
              case CalcValue(cmp) => Cmp(Eq, ArithVar(v), cmp)
              case _ => {
                val tmpVar = getTmpVar(typeOfExpression(nested))
                CalcProd(List(Lift(tmpVar, nested), Cmp(Eq, ArithVar(v), ArithVar(tmpVar))))
              }
            }
          } else
            Lift(v, nested)
        }

        case _ => {
          expr
        }

      }
    }

    def rcr(expr: CalcExpr): CalcExpr = {
      val res = expr match {
        case CalcQuery(name, e) =>
          CalcQuery(name, fold(sumGeneral, prodGeneral, negGeneral, leafNest, e))
        case _ =>
          fold(sumGeneral, prodGeneral, negGeneral, leafNest, expr)
      }
      res
    }

    val rewrittenExpr = rcr(bigexpr)
    //    println("nesting rewrite done!")
    rewrittenExpr

  }
}
