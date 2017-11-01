package ch.epfl.data
package dblab
package frontend
package analyzer

import parser.CalcAST._
import sc.pardis.search.CostingContext
import sc.pardis.ast.Node
import schema._
import scala.math._

class CalcCosting(schema: Schema) extends CostingContext {
  def apply(node: Node): Double = cost(node.asInstanceOf[CalcExpr])

  // TODO add proper costs for different calc expressions
  // most probably an appropriate cost will be parameteric and dependent on
  // the cardinality of relations.

  println("here : ")
  println(schema)

  def searchCost(x: Double): Double = max(1, scala.math.log(x) / scala.math.log(2))

  def cost(exp: CalcExpr): Double = {

    val r = exp match {
      case CalcProd(List()) => 0.0
      case CalcProd(lst) =>
        val headCardinality = cardinality(lst.head)
        lst.map(cost).sum +
          lst.foldLeft((1.0, 0.0))((a, b) => (a._1 * cardinality(b), a._2 + a._1 * cardinality(b)))._2 - headCardinality
      case CalcSum(lst) =>
        val headCardinality = cardinality(lst.head)
        lst.map(cost).sum + lst.map(x => searchCost(cardinality(x))).sum +
          headCardinality - searchCost(headCardinality) //TODO what if they had different rows ?
      case CalcNeg(e)                     => cost(e) + cardinality(e)
      case AggSum(List(), e)              => cost(e) + cardinality(e)
      case AggSum(v, e)                   => cost(e) + cardinality(e) * searchCost(cardinality(e)) + cardinality(e) //TODO we should multiply sorting part to a constant if we know what algorithm is used
      case _: Rel                         => cardinality(exp)
      case Cmp(c, first, second)          => max(cardinality(first), cardinality(second)) //TODO : +they should be same size ? +can be a part of CalcSum ?
      case External(_, inps, outs, tp, _) => ???
      case CmpOrList(v, consts)           => ???
      case Lift(vr, e)                    => ???
      case Exists(term)                   => cardinality(term)
      case CalcValue(v: ArithFunc)        => 5.0
      case CalcValue(v: ArithVar)         => cardinality(v)
      case CalcValue(_)                   => 1.0
      case ArithVar(v)                    => schema.stats.getCardinalityOrElse("PART", 1) //TODO it should change finding size of R in schema
    }
    println(prettyprint(exp))
    println("cost is : " + r)
    r
  }

  def cardinality(exp: CalcExpr): Double = {
    exp match {
      case CalcProd(lst) => lst.foldLeft(1.0)((a, b) => a * cardinality(b))
      case CalcSum(lst)  => lst.foldLeft(0.0)((a, b) => max(a, cardinality(b)))
      case CalcNeg(e)    => cardinality(e)
      case AggSum(v, e) => min(cardinality(e),
        v.foldLeft(1)((a, b) => schema.stats.getDistinctAttrValuesOrElse(b.name, cardinality(e).toInt) * a))
      case Rel(_, name, _, _)             => schema.stats.getCardinalityOrElse("PART", 1) //TODO it should change to name
      case Cmp(c, first, second)          => max(cardinality(first), cardinality(second)) * 1 // TODO : +we can have some inference here +they should be same size I think
      case External(_, inps, outs, tp, _) => ???
      case CmpOrList(v, consts)           => ???
      case Lift(vr, e)                    => ???
      case Exists(term)                   => cardinality(term)
      case CalcValue(v: ArithVar)         => cardinality(v)
      case CalcValue(_)                   => 1
      case ArithVar(v)                    => schema.stats.getCardinalityOrElse("PART", 1) //TODO it should change finding size of R in schema
    }
  }
}
