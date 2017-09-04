package ch.epfl.data
package dblab
package frontend
package optimizer

import parser.CalcAST._
import parser.SQLAST.IntLiteral
import org.scalatest.Matchers._
import org.scalatest._
import CalcUtils._
import sc.pardis.search.BFS
import analyzer.CalcCosting

class CalcSearchTest extends FlatSpec {

  val costEngine = CalcCosting
  val calcTransformationRules = new CalcRuleBasedTransformationRules(CalcRules.allRules)
  val bfs = new BFS(costEngine, calcTransformationRules) // can be changed with other search algorithms

  "CalcSearch" should "optimize query with multiple constants in prod" in {
    val optimizer = CalcOptimizer
    val r = CalcProd(List(CalcValue(ArithConst(IntLiteral(2))), CalcValue(ArithConst(IntLiteral(3))), CalcValue(ArithConst(IntLiteral(4))), CalcValue(ArithConst(IntLiteral(5))), CalcSum(List(CalcValue(ArithConst(IntLiteral(3))), CalcValue(ArithConst(IntLiteral(0))), CalcValue(ArithConst(IntLiteral(4)))))))
    val res = bfs.search(r, 5)
    println(res)
    res should not be None
  }

}
