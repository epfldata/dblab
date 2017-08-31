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

class TPCHSQLTest extends FlatSpec {
  val folder = "experimentation/tpch-sql"
  val resultFolder = "experimentation/tpch-output"
  def getFiles(folder: String, extension: String): List[File] = {
    val f = new File(folder)
    f.listFiles().filter(f => f.getName().endsWith(extension)).toList
  }

  val interpreter = QueryInterpreter

  TPCHData.runOnData(dataPath => {
    val riFiles = getFiles(folder, ".ri").map(_.getAbsolutePath())
    val ddlFiles = getFiles(folder, ".ddl").map(_.getAbsolutePath())
    // println(s"$dataPath, $riFiles, $ddlFiles, $sqlFiles")
    val schema = interpreter.readSchema(dataPath, ddlFiles ++ riFiles)
    "TPCH Q1" should "work" in {
      for (i <- 1 to 1) {
        val query = s"$folder/Q$i.sql"
        val resq = interpreter.processQuery(schema, query)
        val resultFileName = s"$resultFolder/Q$i.result_sf0.1"
        if (new java.io.File(resultFileName).exists) {
          val resc = scala.io.Source.fromFile(resultFileName).mkString
          resq should be(resc)
        } else {
          println("no result file!")
        }
      }
    }
  })
}
