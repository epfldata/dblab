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
import ch.epfl.data.dblab.frontend.parser.CalcParser
import ch.epfl.data.dblab.schema.TPCHSchema

class CalcSearchTest extends FlatSpec {

  val tpchSchema = TPCHSchema.getSchema("experimentation/dbtoaster/queries/sf0.001/", 0.0001)
  val costEngine = new CalcCosting(tpchSchema)
  val calcTransformationRules = new CalcRuleBasedTransformationRules(CalcRules.allRules)
  val bfs = new BFS(costEngine, calcTransformationRules) // can be changed with other search algorithms

  "CalcSearch" should "optimize query with multiple constants in prod" in {
    val optimizer = CalcOptimizer
    val r = CalcProd(List(CalcValue(ArithConst(IntLiteral(2))), CalcValue(ArithConst(IntLiteral(3))), CalcValue(ArithConst(IntLiteral(4))), CalcValue(ArithConst(IntLiteral(5))), CalcSum(List(CalcValue(ArithConst(IntLiteral(3))), CalcValue(ArithConst(IntLiteral(0))), CalcValue(ArithConst(IntLiteral(4)))))))
    //    val r = CalcSum(List(CalcValue(ArithConst(IntLiteral(3))), CalcValue(ArithConst(IntLiteral(0))), CalcValue(ArithConst(IntLiteral(4)))))
    println(prettyprint(r))
    val res = bfs.search(r, 50)
    println(prettyprint(res.asInstanceOf[CalcExpr]))
    res should not be None
  }

  import CalcRules._
  val ruleBasedOptimizer = new CalcRuleBasedTransformer(allRules)

  "CalcSearch" should "optimize simple queries similar to the rule-based version" in {
    val parser = CalcParser
    val folder = "experimentation/dbtoaster/queries/calcsimple/ok_raw"
    val files = getCalcFiles(folder)
    for (file <- files) {
      println(s"optimizing $file")
      val queries = parser.parse(scala.io.Source.fromFile(file).mkString)
      for (q <- queries) {
        //        println(q)
        val o1 = bfs.search(q.asInstanceOf[CalcQuery].expr, 200).asInstanceOf[CalcExpr]
        val o2 = ruleBasedOptimizer(q).asInstanceOf[CalcQuery].expr
        println(prettyprint(q))
        println(" " + prettyprint(o1))
        println("cost is : " + costEngine.cost(o1))
        println(" " + prettyprint(o2))
        println("cost is : " + costEngine.cost(o2))
        o1 should be(o2)
      }
    }
  }

}
