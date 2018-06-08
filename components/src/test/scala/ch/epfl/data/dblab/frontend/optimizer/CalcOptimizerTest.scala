package ch.epfl.data
package dblab
package frontend
package optimizer

import parser.CalcAST._
import parser.SQLAST.IntLiteral
import sc.pardis.types.IntType
import org.scalatest.Matchers._
import org.scalatest._
import parser.CalcParser
import ch.epfl.data.dblab.frontend.optimizer.CalcUtils._
/**
 * @author Parand Alizadeh
 */
class CalcOptimizerTest extends FlatSpec {

  "CalcOptimzer" should "optimize query with multiple constants in prod" in {
    val optimizer = CalcOptimizer
    val r = CalcProd(List(CalcValue(ArithConst(IntLiteral(2))), CalcValue(ArithConst(IntLiteral(3))), CalcValue(ArithConst(IntLiteral(4))), CalcValue(ArithConst(IntLiteral(5))), CalcSum(List(CalcValue(ArithConst(IntLiteral(3))), CalcValue(ArithConst(IntLiteral(0))), CalcValue(ArithConst(IntLiteral(4)))))))
    val res = optimizer.normalize(r)
    res should not be None
  }

  "nesting rewrite" should "optimize query with zero in aggsum" in {

    val optimizer = CalcOptimizer
    val r = AggSum(List(VarT("A", IntType)), CalcValue(ArithConst(IntLiteral(0))))

    val res = optimizer.nestingRewrites(r)
    //    println("############")
    //    println(prettyprint(r))
    //    println(prettyprint(res))
    res should not be None
  }

  "nesting rewrite" should "optimize aggsum query" in {

    val optimizer = CalcOptimizer
    val r = AggSum(List(VarT("A", IntType)), CalcValue(ArithVar(VarT("A", IntType))))

    val res = optimizer.nestingRewrites(r)
    //    println("############")
    //    println(prettyprint(r))
    //    println(prettyprint(res))
    res should not be None
  }

  "nesting rewrite" should "optimize aggsum query with sum" in {

    val optimizer = CalcOptimizer
    val r = CalcSum(List(AggSum(List(VarT("A", IntType)), CalcValue(ArithVar(VarT("A", IntType)))), AggSum(List(VarT("B", IntType)), CalcValue(ArithVar(VarT("B", IntType)))), AggSum(List(VarT("C", IntType)), CalcValue(ArithVar(VarT("C", IntType))))))

    val res = optimizer.nestingRewrites(r)
    //    println("############")
    //    println(prettyprint(r))
    //    println(prettyprint(res))
    res should not be None
  }

  import CalcRules._
  val ruleBasedOptimizer = new CalcRuleBasedTransformer(allRules)

  "nesting rewrite" should "optimize simple queries similar to the rule-based version" in {
    val optimizer = CalcOptimizer
    val parser = CalcParser
    val folder = "experimentation/dbtoaster/queries/calcsimple/raw"
    val files = getCalcFiles(folder)
    for (file <- files) {
      //      println(s"optimizing $file")
      val queries = parser.parse(scala.io.Source.fromFile(file).mkString)
      for (q <- queries) {
        val o1 = optimizer.normalize(optimizer.nestingRewrites(q))
        val o2 = ruleBasedOptimizer(q)
        o1 should be(o2)
      }
    }
  }

  "nesting rewrite" should "optimize tpch queries similar to the rule-based version" in {
    val optimizer = CalcOptimizer
    val parser = CalcParser
    val folder = "experimentation/dbtoaster/queries/calctpch/raw"
    val files = getCalcFiles(folder)
    for (file <- files) {
      // println(s"optimizing $file")
      val queries = parser.parse(scala.io.Source.fromFile(file).mkString)
      for (q <- queries) {
        val o1 = optimizer.normalize(optimizer.nestingRewrites(q))
        val o2 = ruleBasedOptimizer(q)
        o1 should be(o2)
      }
    }
  }
}
