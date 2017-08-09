package ch.epfl.data.dblab.frontend.parser

import org.scalatest._
import Matchers._
import ch.epfl.data.dblab.frontend.parser.CalcAST._
import ch.epfl.data.sc.pardis.types.IntType

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
      println(s"parsing $file")
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
      println(s"parsing $file")
      val r = parser.parse(scala.io.Source.fromFile(file).mkString)
      r should not be None
    }
  }

  "CalcParser" should "pretty print a simple query" in {
    val parser = CalcParser
    var r = CalcProd(List(Rel("Rel", "R", List(VarT("R_B", IntType), VarT("R_A", IntType)), ""), Rel("Rel", "S", List(VarT("S_C", IntType), VarT("S_B", IntType)), "")))
    println(CalcAST.prettyprint(r))
  }

  "CalcParser" should "pretty print DBToaster simple queries" in {
    val parser = CalcParser
    val folder = "experimentation/dbtoaster/queries/calcsimple"
    val f = new java.io.File(folder)
    val files = if (!f.exists) {
      throw new Exception(s"$f does not exist!")
    } else
      f.listFiles.map(folder + "/" + _.getName).toList
    for (file <- files) {
      println(s"parsing $file")
      val r = parser.parse(scala.io.Source.fromFile(file).mkString)
      val res = r.foldLeft()((acc, cur) => acc + CalcAST.prettyprint(cur))
      res should not be None
    }
  }

  "CalcParser" should "pretty print DBToaster tpch queries" in {
    val parser = CalcParser
    val folder = "experimentation/dbtoaster/queries/calctpch"
    val f = new java.io.File(folder)
    val files = if (!f.exists) {
      throw new Exception(s"$f does not exist!")
    } else
      f.listFiles.map(folder + "/" + _.getName).toList
    for (file <- files) {
      println(s"parsing $file")
      val r = parser.parse(scala.io.Source.fromFile(file).mkString)
      val res = r.foldLeft()((acc, cur) => acc + CalcAST.prettyprint(cur))
      res should not be None
    }
  }
}
