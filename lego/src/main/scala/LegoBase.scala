package ch.epfl.data
package legobase

import utils.Utilities._
import java.io.PrintStream

trait LegoRunner {
  var currQuery: java.lang.String = ""
  Config.checkResults = true

  def getOutputName = currQuery + "Output.txt"

  def executeQuery(query: String): Unit

  def run(args: Array[String]) {

    val sf = if (args(1).contains(".")) args(1).toDouble.toString else args(1).toInt.toString
    Config.sf = sf.toDouble
    Config.datapath = args(0) + "/sf" + sf + "/"

    val excludedQueries = Nil

    val queries: scala.collection.immutable.List[String] =
      if (args.length == 3 && args(2) == "testsuite-scala") (for (i <- 1 to 22 if !excludedQueries.contains(i)) yield "Q" + i).toList
      else if (args.length == 3 && args(2) == "testsuite-c") (for (i <- 1 to 22 if !excludedQueries.contains(i)) yield "Q" + i + "_C").toList
      else args.tail.tail.toList
    for (q <- queries) {
      currQuery = q
      Console.withOut(new PrintStream(getOutputName)) {
        executeQuery(currQuery)
        // Check results
        if (Config.checkResults) {
          val getResultFileName = "results/" + currQuery + ".result_sf" + sf
          if (new java.io.File(getResultFileName).exists) {
            val resc = {
              val str = scala.io.Source.fromFile(getResultFileName).mkString
              str * Config.numRuns
            }
            val resq = scala.io.Source.fromFile(getOutputName).mkString
            if (resq != resc) {
              System.out.println("-----------------------------------------")
              System.out.println("QUERY" + q + " DID NOT RETURN CORRECT RESULT!!!")
              System.out.println("Correct result:")
              System.out.println(resc)
              System.out.println("Result obtained from execution:")
              System.out.println(resq)
              System.out.println("-----------------------------------------")
              System.exit(0)
            } else System.out.println("CHECK RESULT FOR QUERY " + q + ": [OK]")
          } else System.out.println("Reference result file not found. Skipping checking of result")
        }
      }
    }
  }
}

object MiniDB extends LegoRunner {
  import Queries._

  def executeQuery(query: String): Unit = query match {
    case "Q1"     => Q1(Config.numRuns)
    case "Q2"     => Q2(Config.numRuns)
    case "Q3"     => Q3(Config.numRuns)
    case "Q4"     => Q4(Config.numRuns)
    case "Q5"     => Q5(Config.numRuns)
    case "Q6"     => Q6(Config.numRuns)
    case "Q7"     => Q7(Config.numRuns)
    case "Q8"     => Q8(Config.numRuns)
    case "Q9"     => Q9(Config.numRuns)
    case "Q10"    => Q10(Config.numRuns)
    case "Q11"    => Q11(Config.numRuns)
    case "Q12"    => Q12(Config.numRuns)
    case "Q13"    => Q13(Config.numRuns)
    case "Q14"    => Q14(Config.numRuns)
    case "Q15"    => Q15(Config.numRuns)
    case "Q16"    => Q16(Config.numRuns)
    case "Q17"    => Q17(Config.numRuns)
    case "Q18"    => Q18(Config.numRuns)
    case "Q19"    => Q19(Config.numRuns)
    case "Q20"    => Q20(Config.numRuns)
    case "Q21"    => Q21(Config.numRuns)
    case "Q22"    => Q22(Config.numRuns)
    case dflt @ _ => throw new Exception("Query " + dflt + " not supported!")
  }

  def main(args: Array[String]) {
    // Some checks to avoid stupid exceptions
    if (args.length < 3) {
      System.out.println("ERROR: Invalid number (" + args.length + ") of command line arguments!")
      System.out.println("USAGE: run <data_folder> <scaling_factor_number> <list of queries to run>")
      System.out.println("     : data_folder_name should contain folders named sf0.1 sf1 sf2 sf4 etc")
      System.exit(0)
    }

    run(args)
  }
}
