package ch.epfl.data
package dblab
package frontend
package analyzer

import ch.epfl.data.dblab.frontend.parser.SQLAST
import parser.CalcAST._
import sc.pardis.search.CostingContext
import sc.pardis.ast.Node
import schema._

import scala.math._

class CalcCosting(schema: Schema) extends CostingContext {
  def apply(node: Node): Double = cost(node.asInstanceOf[CalcExpr])
  var count = 0

  def searchCost(x: Double): Double = max(1, scala.math.log(x) / scala.math.log(2))

  def findVars(exp: CalcExpr): scala.collection.mutable.Map[VarT, CalcExpr] = {
    val vars = scala.collection.mutable.Map[VarT, CalcExpr]()
    exp match {
      case CalcProd(lst) => lst.foldLeft(vars)((a, b) => a ++ findVars(b))
      case CalcSum(lst)  => lst.foldLeft(vars)((a, b) => a ++ findVars(b))
      case AggSum(v, e)  => findVars(e)
      case CalcNeg(e)    => findVars(e)
      case Lift(vr, e)   => vars += (vr -> e)
      case _             => vars
    }
  }

  def cost(exp: CalcExpr): Double = {
    count += 1
    val vars = findVars(exp)
    costExpr(exp, vars)
  }

  def costExpr(exp: CalcExpr, vars: scala.collection.mutable.Map[VarT, CalcExpr]): Double = {

    exp match {
      case CalcProd(List()) => 0.0
      case CalcProd(lst) =>
        val headCardinality = cardinality(lst.head, vars)._2
        lst.map(x => costExpr(x, vars)).sum +
          lst.foldLeft((1.0, 0.0))((a, b) => {
            if (cardinality(b, vars)._1 == 2)
              (a._1 * cardinality(b, vars)._2, a._2 + a._1 * cardinality(b, vars)._2)
            else if (cardinality(b, vars)._1 == 1 && a._1 == 1)
              (cardinality(b, vars)._2, a._2 + cardinality(b, vars)._2)
            else if (cardinality(b, vars)._1 == 1 && a._1 != 1)
              (a._1, a._2 + a._1)
            else
              a
          })._2 - headCardinality
      case CalcSum(lst) =>
        val headCardinality = cardinality(lst.head, vars)._2
        lst.map(x => costExpr(x, vars)).sum + lst.map(x => searchCost(cardinality(x, vars)._2)).sum +
          headCardinality - searchCost(headCardinality) //TODO what if they had different rows ?
      case CalcNeg(e)        => costExpr(e, vars) + cardinality(e, vars)._2
      case AggSum(List(), e) => costExpr(e, vars) + cardinality(e, vars)._2
      case AggSum(v, e) => costExpr(e, vars) + cardinality(e, vars)._2 *
        searchCost(cardinality(e, vars)._2) + cardinality(e, vars)._2 //TODO we should multiply sorting part to a constant if we know what algorithm is used
      case _: Rel                         => cardinality(exp, vars)._2
      case Cmp(c, first, second)          => costExpr(first, vars) + costExpr(second, vars) + max(cardinality(first, vars)._2, cardinality(second, vars)._2)
      case External(_, inps, outs, tp, _) => ???
      case CmpOrList(v, consts)           => ???
      case Lift(vr, e)                    => searchCost(vars.size)
      case Exists(term)                   => costExpr(term, vars) + cardinality(term, vars)._2
      case CalcValue(v: ArithFunc)        => 5.0
      case CalcValue(v: ArithVar)         => cardinality(v, vars)._2
      case CalcValue(_)                   => 1.0
      case ArithVar(v) =>
        if (vars.contains(v))
          costExpr(vars(v), vars)
        else
          schema.stats.getCardinalityOrElse("PART", 1) //TODO it should change finding size of R in schema
      case ArithProd(lst) =>
        lst.map(x => costExpr(x, vars)).sum +
          lst.foldLeft((1.0, 0.0))((a, b) => {
            if (cardinality(b, vars)._1 == 1 && a._1 == 1)
              (cardinality(b, vars)._2, a._2 + a._1)
            else if (cardinality(b, vars)._1 == 1 && a._1 != 1)
              (a._1, a._2 + a._1)
            else
              a
          })._2

      case _: ArithFunc                        => 5.0
      case ArithConst(s: SQLAST.StringLiteral) => s.v.length.toDouble
      case _: ArithConst                       => 1.0
    }
  }

  def cardinality(exp: CalcExpr, vars: scala.collection.mutable.Map[VarT, CalcExpr]): (Int, Double) = {
    exp match {
      case CalcProd(lst) => (lst.foldLeft(1)((a, b) => max(a, cardinality(b, vars)._1)), lst.foldLeft(1.0)((a, b) => {
        if (cardinality(b, vars)._1 == 2)
          a * cardinality(b, vars)._2
        else if (cardinality(b, vars)._1 == 2 && a == 1)
          cardinality(b, vars)._2
        else
          a
      }))
      case CalcSum(lst) => (lst.foldLeft(1)((a, b) => max(a, cardinality(b, vars)._1)),
        lst.foldLeft(0.0)((a, b) => max(a, cardinality(b, vars)._2)))
      case CalcNeg(e)        => cardinality(e, vars)
      case AggSum(List(), e) => (1, 1)
      case AggSum(v, e) => (cardinality(e, vars)._1, min(cardinality(e, vars)._2,
        v.foldLeft(1)((a, b) => schema.stats.getDistinctAttrValuesOrElse(b.name, cardinality(e, vars)._2.toInt) * a)) * 0.5)
      case Rel(_, name, _, _)             => (2, schema.stats.getCardinalityOrElse("PART", 1)) //TODO it should change to name
      case Cmp(c, first, second)          => (1, max(cardinality(first, vars)._2, cardinality(second, vars)._2) * 1) // TODO : +we can have some inference here
      case External(_, inps, outs, tp, _) => ???
      case CmpOrList(v, consts)           => ???
      case Lift(vr, e)                    => (0, 1)
      case Exists(term)                   => cardinality(term, vars) //TODO we can have inference here
      case CalcValue(v: ArithVar)         => cardinality(v, vars)
      case CalcValue(_)                   => (1, 1)
      case ArithVar(v) =>
        if (vars.contains(v))
          cardinality(vars(v), vars)
        else
          (1, schema.stats.getCardinalityOrElse("PART", 1)) //TODO it should change finding size of R in schema
      case _: ArithConst => (1, 1)
      case _: ArithFunc  => (1, 1)
      case ArithProd(lst) => (lst.foldLeft(1)((a, b) => max(a, cardinality(b, vars)._1)), lst.foldLeft(1.0)((a, b) => {
        if (cardinality(b, vars)._1 == 2 && a == 1)
          cardinality(b, vars)._2
        else
          a
      }))

    }
  }
}
