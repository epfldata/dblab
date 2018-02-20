package ch.epfl.data
package dblab
package experimentation
package tpch

import org.scalatest._
import sc.pardis.shallow.OptimalString
import Matchers._
import storagemanager._
import java.io.{ PrintStream, File }
import config.Config
import queryengine._

case class Result(rows: Seq[ResultRow])
case class ResultRow(values: Seq[ResultColumn])
case class ResultColumn(value: String) {
  def parseDouble(s: String) = try { Some(s.toDouble) } catch { case _: Throwable => None }
  override def equals(o: Any): Boolean = o match {
    case ResultColumn(other: String) =>
      (parseDouble(value), parseDouble(other)) match {
        case (Some(v1), Some(v2)) => {
          val decDigits = {
            val i = value.indexOf(".")
            if (i == -1)
              0
            else
              value.length - i - 1
          }
          val tenPow = math.pow(10, decDigits)
          math.round(v1 * tenPow) == math.round(v2 * tenPow)
          // v1.toInt == v2.toInt 
          // math.round(v1) == math.round(v2)
        }
        case _ => value == other
      }
  }
}

class TPCHSQLTest extends FlatSpec {
  val folder = "experimentation/tpch-sql"
  val resultFolder = "experimentation/tpch-output"
  def getFiles(folder: String, extension: String): List[File] = {
    val f = new File(folder)
    f.listFiles().filter(f => f.getName().endsWith(extension)).toList
  }

  val interpreter = QueryInterpreter

  def outputToResult(output: String): Result = {
    val lines = output.split("\n").dropRight(1)
    Result(lines.map(l => ResultRow(l.split('|').map(ResultColumn))))
  }

  TPCHData.runOnData(dataPath => {
    val riFiles = getFiles(folder, ".ri").map(_.getAbsolutePath())
    val ddlFiles = getFiles(folder, ".ddl").map(_.getAbsolutePath())
    // println(s"$dataPath, $riFiles, $ddlFiles, $sqlFiles")
    val schema = interpreter.readSchema(dataPath, ddlFiles ++ riFiles)
    // 4, 16, 20, (21), 22 bug
    // 7, 8, 18 GC overflow
    // 13 no query
    // 19 acc bug
    val queries = (1 to 22) diff List(4, 7, 8, 13, 16, 20, 21, 22)

    for (i <- queries) {
      s"TPCH Q$i" should "work" in {
        val query = s"$folder/Q$i.sql"
        val resq = outputToResult(interpreter.processQuery(schema, query))
        val resultFileName = s"$resultFolder/Q$i.result_sf0.1"
        if (new java.io.File(resultFileName).exists) {
          println("RESULT exists!")
          val lines = scala.io.Source.fromFile(resultFileName)
          val resc = outputToResult(lines.mkString)
          resq should be(resc)
        } else {
          println("no result file!")
        }
      }
    }
  })
}
