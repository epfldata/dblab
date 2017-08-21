package ch.epfl.data.dblab.frontend.parser

import org.scalatest._
import Matchers._

class CalcParserTest extends FlatSpec {

  "CalcParser" should "parse simpler query w/o relation correctly" in {
    val parser = CalcParser
    val r = parser.parse("declare query q2 := AggSum([], (C ^= 0) * {C:int > 0});")
    r should not be None
  }

  "CalcParser" should "parse DBToaster simple queries" in {
    val parser = CalcParser
    val folder = "experimentation/dbtoaster/queries/calcsimple"
    val f = new java.io.File(folder)
    val files = if (!f.exists) {
      throw new Exception(s"$f does not exist!")
    } else
      f.listFiles.map(folder + "/" + _.getName).toList
    for (file <- files) {
      // println(s"parsing $file")
      val r = parser.parse(scala.io.Source.fromFile(file).mkString)
      r should not be None
    }
  }

  "CalcParser" should "parse DBToaster tpch queries" in {
    val parser = CalcParser
    val folder = "experimentation/dbtoaster/queries/calctpch"
    val f = new java.io.File(folder)
    val files = if (!f.exists) {
      throw new Exception(s"$f does not exist!")
    } else
      f.listFiles.map(folder + "/" + _.getName).toList
    for (file <- files) {
      // println(s"parsing $file")
      val r = parser.parse(scala.io.Source.fromFile(file).mkString)
      r should not be None
    }
  }
}
