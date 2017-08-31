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
  def getFiles(folder: String, extension: String): List[File] = {
    val f = new File(folder)
    f.listFiles().filter(f => f.getName().endsWith(extension)).toList
  }

  val interpreter = QueryInterpreter

  TPCHData.runOnData(datapath => {
    val riFiles = getFiles(folder, ".ri").map(_.getAbsolutePath())
    val ddlFiles = getFiles(folder, ".ddl").map(_.getAbsolutePath())
    val sqlFiles = getFiles(folder, ".sql").map(_.getAbsolutePath())
    println(s"$datapath, $riFiles, $ddlFiles, $sqlFiles")
    "TPCH" should "work" in {
      interpreter.interpret(datapath, Map(".ri" -> riFiles, ".ddl" -> ddlFiles, ".sql" -> List(sqlFiles.head)))
    }
  })
}
