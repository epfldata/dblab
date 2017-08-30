package ch.epfl.data
package dblab
package frontend
package optimizer

import ch.epfl.data.dblab.frontend.optimizer.CalcOptimizer.{ SchemaOfExpression, commutes, typeOfExpression }
import ch.epfl.data.dblab.frontend.parser.CalcAST
import parser.CalcAST._
import parser.SQLAST._
import sc.pardis.ast._
import sc.pardis.rules._

object CalcRules {
  case object Agg0 extends Rule("Agg0") {
    def generate(node: Node): Option[Node] = node match {
      case AggSum(gbvars, CalcValue(ArithConst(IntLiteral(0)))) =>
        Some(CalcValue(ArithConst(IntLiteral(0))))
      case AggSum(gbvars, CalcValue(ArithConst(DoubleLiteral(0.0)))) =>
        Some(CalcValue(ArithConst(DoubleLiteral(0.0))))
      case _ => None
    }
  }

  case object Prod0 extends Rule("Prod0") {
    //multiply constants
    override def generate(node: Node): Option[Node] = node match {
      case CalcProd(exprs) => {
        val (cs, ncs) = exprs.foldLeft[(List[Double], List[CalcExpr])]((Nil, Nil))({
          case ((acc1, acc2), cur) => cur match {
            case CalcValue(ArithConst(IntLiteral(t)))    => (t :: acc1, acc2)
            case CalcValue(ArithConst(DoubleLiteral(t))) => (t :: acc1, acc2)
            case _                                       => (acc1, acc2 :+ cur)
          }
        })

        if (cs.length > 0) {
          var res = cs.foldLeft(1.0)((acc, cur) => (acc * cur))
          if (res == 0)
            Some(CalcValue(ArithConst(DoubleLiteral(0.0))))
          else if (res == 1.0 && ncs.length > 0)
            Some(CalcProd(ncs))
          else
            Some(CalcProd(CalcValue(ArithConst(DoubleLiteral(res))) :: ncs))

        } else
          None
      }
      case _ => None

    }
  }

  case object Prod1 extends Rule("Prod1") {
    override def generate(node: Node): Option[Node] = node match {
      case CalcProd(exprs) => {
        if (exprs.length == 1)
          Some(exprs.head)
        else
          None
      }
      case _ => None
    }
  }

  case object Sum0 extends Rule("Sum0") {
    //remove 0s
    override def generate(node: Node): Option[Node] = node match {
      case CalcSum(exprs) => {
        val elems = exprs.foldLeft[(List[CalcExpr])]((Nil))({
          case (acc1, cur) => cur match {
            case CalcValue(ArithConst(IntLiteral(0)))      => (acc1)
            case CalcValue(ArithConst(DoubleLiteral(0.0))) => (acc1)
            case _                                         => (acc1 :+ cur)
          }
        })

        if (elems.length > 0)
          Some(CalcSum(elems))
        else
          Some(CalcValue(ArithConst(IntLiteral(0))))
      }
      case _ => None
    }
  }

  case object Sum1 extends Rule("Sum1") {
    override def generate(node: Node): Option[Node] = node match {
      case CalcSum(exprs) => {
        if (exprs.length == 1)
          Some(exprs.head)
        else
          None
      }
      case _ => None
    }
  }
  case object AggSum1 extends Rule("Agg1") {
    override def generate(node: Node): Option[Node] = node match {
      case AggSum(_, sub) if (SchemaOfExpression(sub)_2).length == 0 =>
        Some(sub)
      case _ => None
    }
  }

  //TODO: if not aggsum1
  case object AggSum2 extends Rule("Agg2") {
    override def generate(node: Node): Option[Node] = node match {
      case AggSum(gbvars, CalcSum(list)) => {
        val (sumivars, _) = SchemaOfExpression(CalcSum(list))
        val rewritten = CalcSum(list.map(term => {
          val (_, termovars) = SchemaOfExpression(term)
          val termgbvars = gbvars.toSet.union(sumivars.toSet.intersect(termovars.toSet)).toList
          (AggSum(termgbvars, term))
        }))
        if (rewritten.equals(AggSum(gbvars, CalcSum(list))))
          None
        else
          Some(rewritten)
      }
      case _ => None

    }
  }

  //TODO: if not aggsum1
  case object AggSum3 extends Rule("Agg3") {
    override def generate(node: Node): Option[Node] = node match {
      case AggSum(gbvars, CalcProd(list)) => {
        val (unnested, nested) = list.foldLeft[(CalcExpr, CalcExpr)](((CalcValue(ArithConst(IntLiteral(1)))), (CalcValue(ArithConst(IntLiteral(1))))))((acc, cur) => {
          (if (commutes(acc._2, cur) && (SchemaOfExpression(cur)._2).toSet.subsetOf(gbvars.toSet)) (CalcProd(List(acc._1, cur)), acc._2) else (acc._1, CalcProd(List(acc._2, cur))))
        })
        val unnestedivars = SchemaOfExpression(unnested)._1
        val newgbvars = (SchemaOfExpression(nested)._2).toSet.intersect(gbvars.toSet.union(unnestedivars.toSet)).toList
        val res = CalcProd(List(unnested, AggSum(newgbvars, nested)))
        if (res.equals(AggSum(gbvars, CalcProd(list))))
          None
        else
          Some(res)
      }
      case _ => None
    }
  }

  case object AggSum4 extends Rule("Agg4") {
    override def generate(node: Node): Option[Node] = node match {
      case AggSum(_, CalcProd(_)) => None
      case AggSum(_, CalcProd(_)) => None
      case AggSum(gbvars, AggSum(_, t)) => {
        //TODO: if not aggsum1
        Some(AggSum(gbvars, t))
      }
      case _ => None
    }
  }

  case object AggSum5 extends Rule("Agg5") {
    override def generate(node: Node): Option[Node] = node match {
      case AggSum(_, CalcProd(_))  => None
      case AggSum(_, CalcProd(_))  => None
      case AggSum(_, AggSum(_, _)) => None
      case AggSum(gbvars, AggSum(name, t)) => {
        if ((SchemaOfExpression(AggSum(name, t)) _2).toSet.subsetOf(gbvars.toSet))
          Some(AggSum(name, t))
        else
          None

      }
      case _ => None
    }
  }

  case object exists extends Rule("Exists0") {
    override def generate(node: Node): Option[Node] = node match {
      case CalcAST.Exists(CalcValue(ArithConst(IntLiteral(0)))) => Some(CalcValue(ArithConst(IntLiteral(0))))
      case CalcAST.Exists(CalcValue(ArithConst(IntLiteral(_)))) => Some(CalcValue(ArithConst(IntLiteral(1))))
      case _ => None
    }
  }

  case object lift extends Rule("Lift0") {
    override def generate(node: Node): Option[Node] = node match {
      case Lift(v, nested) => {
        val (nestedivars, nestedovars) = SchemaOfExpression(nested)
        if (nestedovars.contains(v))
          None
        else if (nestedivars.contains(v)) {
          nested match {
            case CalcValue(cmp) => Some(Cmp(Eq, ArithVar(v), cmp))
            case _ => {
              val tmpVar = getTmpVar(typeOfExpression(nested))
              Some(CalcProd(List(Lift(tmpVar, nested), Cmp(Eq, ArithVar(v), ArithVar(tmpVar)))))
            }
          }
        } else
          None
      }
    }
  }

}