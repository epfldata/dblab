package ch.epfl.data
package dblab
package experimentation
package tpch

import org.scalatest._
import sc.pardis.shallow.OptimalString
import Matchers._
import storagemanager._
import java.io.PrintStream
import config.Config

class TPCHQPlanTest extends FlatSpec {
  val interpreter = TPCHInterpreter

  TPCHData.runOnData(datapath => {

    val schema = TPCHSchema.getSchema(datapath, 0.1)
    Config.datapath = datapath
    interpreter.scalingFactor = 0.1
    val queries = (1 to 22) map (i => s"Q$i")
    for (currQuery <- queries) {
      s"TPCH $currQuery written using the QPlan DSL" should "run correctly with SF0.1" in {
        val outputName = interpreter.getOutputName(currQuery)
        Console.withOut(new PrintStream(outputName)) {
          interpreter.executeQuery(currQuery, schema)
          // Check results
          val resultFileName = interpreter.getResultFileName(currQuery)
          val resq = scala.io.Source.fromFile(outputName).mkString
          if (new java.io.File(resultFileName).exists) {
            val resc = scala.io.Source.fromFile(resultFileName).mkString
            resq should be(resc)
          } else {
            System.out.println(s"Reference result file '$resultFileName' not found. Skipping checking of result")
          }
        }
      }
    }
  })

}
