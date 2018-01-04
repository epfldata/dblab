package ch.epfl.data.dblab.frontend.optimizer

import ch.epfl.data.dblab.frontend.optimizer.CalcUtils.getCalcFiles
import ch.epfl.data.dblab.frontend.parser.CalcAST.CalcQuery
import ch.epfl.data.dblab.frontend.parser.CalcParser
import ch.epfl.data.dblab.schema.{ Schema, Statistics }
import org.scalatest.Matchers.be
import org.scalatest._

import scala.collection.mutable.ArrayBuffer

class CalcCompilerTest extends FlatSpec {

  "CalcCompiler" should "compile simple queries " in {
    //println("@@@@@@@")
    val parser = CalcParser
    val folder = "experimentation/dbtoaster/queries/calcsimple"
    val files = getCalcFiles(folder)
    for (file <- files) {
      // println(s"compiling $file")
      val tree = parser.parse(scala.io.Source.fromFile(file).mkString)

      val queries = tree.foldLeft(List.empty[CalcQuery])((acc, cur) => {
        cur match {
          case CalcQuery(x, y) => acc :+ CalcQuery(x, y)
          case _               => acc
        }
      })

      val ans = CalcCompiler.compile(Some(1), queries, Schema(ArrayBuffer(), Statistics()))

    }

  }

  "CalcCompiler" should "compile tpch queries " in {
    //println("@@@@@@@")
    val parser = CalcParser
    val folder = "experimentation/dbtoaster/queries/calctpch"
    val files = getCalcFiles(folder)
    for (file <- files) {
      // println(s"compiling $file")
      val tree = parser.parse(scala.io.Source.fromFile(file).mkString)

      val queries = tree.foldLeft(List.empty[CalcQuery])((acc, cur) => {
        cur match {
          case CalcQuery(x, y) => acc :+ CalcQuery(x, y)
          case _               => acc
        }
      })

      val ans = CalcCompiler.compile(Some(1), queries, Schema(ArrayBuffer(), Statistics()))

    }

  }
}
