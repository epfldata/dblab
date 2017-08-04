package ch.epfl.data.dblab.frontend.parser

import org.scalatest._
import Matchers._
import ch.epfl.data.dblab.frontend.optimizer.CalcOptimizer
import ch.epfl.data.dblab.frontend.parser.CalcAST._
import ch.epfl.data.dblab.frontend.parser.SQLAST.IntLiteral

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

}
