package ch.epfl.data.dblab.frontend.parser

import org.scalatest._
import Matchers._
import ch.epfl.data.dblab.frontend.optimizer.CalcOptimizer
import ch.epfl.data.dblab.frontend.parser.CalcAST._
import ch.epfl.data.dblab.frontend.parser.SQLAST.IntLiteral
import ch.epfl.data.sc.pardis.types.IntType

/**
 * @author Parand Alizadeh
 */
class CalcOptimizerTest extends FlatSpec {

  "CalcOptimzer" should "optimize query with multiple constants in prod" in {
    val optimizer = CalcOptimizer
    val r = CalcProd(List(CalcValue(ArithConst(IntLiteral(2))), CalcValue(ArithConst(IntLiteral(3))), CalcValue(ArithConst(IntLiteral(4))), CalcValue(ArithConst(IntLiteral(5))), CalcSum(List(CalcValue(ArithConst(IntLiteral(3))), CalcValue(ArithConst(IntLiteral(0))), CalcValue(ArithConst(IntLiteral(4)))))))
    val res = optimizer.Normalize(r)
    res should not be None
  }

  "nesting rewrite" should "optimize query with zero in aggsum" in {

    val optimizer = CalcOptimizer
    val r = AggSum(List(VarT("A", IntType)), CalcValue(ArithConst(IntLiteral(0))))

    val res = optimizer.nestingRewrites(r)
    println("############")
    println(prettyprint(r))
    println(prettyprint(res))
    res should not be None
  }

  "nesting rewrite" should "optimize aggsum query" in {

    val optimizer = CalcOptimizer
    val r = AggSum(List(VarT("A", IntType)), CalcValue(ArithVar(VarT("A", IntType))))

    val res = optimizer.nestingRewrites(r)
    println("############")
    println(prettyprint(r))
    println(prettyprint(res))
    res should not be None
  }

  "nesting rewrite" should "optimize aggsum query with sum" in {

    val optimizer = CalcOptimizer
    val r = CalcSum(List(AggSum(List(VarT("A", IntType)), CalcValue(ArithVar(VarT("A", IntType)))), AggSum(List(VarT("B", IntType)), CalcValue(ArithVar(VarT("B", IntType)))), AggSum(List(VarT("C", IntType)), CalcValue(ArithVar(VarT("C", IntType))))))

    val res = optimizer.nestingRewrites(r)
    println("############")
    println(prettyprint(r))
    println(prettyprint(res))
    res should not be None
  }
}
