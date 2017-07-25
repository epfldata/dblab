package ch.epfl.data.dblab.frontend.parser

import org.scalatest._
import Matchers._

class CalcParserTest extends FlatSpec {

  "CalcParser" should "parse simpler query w/o relation correctly" in {
    val parser = CalcParser
    val r = parser.parse("q2: AggSum([], (C ^= 0) * {C:int > 0})")
    r should not be None
  }
}
