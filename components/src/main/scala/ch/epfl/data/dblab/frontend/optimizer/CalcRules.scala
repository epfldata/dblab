package ch.epfl.data
package dblab
package frontend
package optimizer

import ch.epfl.data.dblab.frontend.optimizer.CalcOptimizer.{ SchemaOfExpression }
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
      case AggSum(_, sub) => {
        if ((SchemaOfExpression(sub)_2).length == 0)
          Some(sub)
        else None
      }
      case _ => None
    }
  }

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

}