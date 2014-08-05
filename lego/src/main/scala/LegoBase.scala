package ch.epfl.data
package legobase

import utils.Utilities._
import java.io.PrintStream

trait ScalaImpl {
  val sdf = new java.text.SimpleDateFormat("yyyy-MM-dd")
  def parseDate(x: String): Long = {
    sdf.parse(x).getTime
  }
  def parseString(x: String): Array[Byte] = x.getBytes
}

trait LegoRunner {
  val numRuns: scala.Int = 1
  var currQuery: java.lang.String = ""

  def getOutputName = currQuery + "Output.txt"

  def getResultFileName = "results/" + currQuery + ".result_big"

  def run(args: Array[String], funs: List[(Int => Unit)]) {
    Config.datapath = args(0)
    Config.checkResults = true

    val queries: scala.collection.immutable.List[String] =
      if (args.length == 2 && args(1) == "testsuite") (for (i <- 1 to 22) yield "Q" + i).toList
      else args.tail.toList
    for (q <- queries) {
      currQuery = q
      Console.withOut(new PrintStream(getOutputName)) {
        currQuery match {
          case "Q1"  => funs(0)(numRuns)
          case "Q2"  => funs(1)(numRuns)
          case "Q3"  => funs(2)(numRuns)
          case "Q4"  => funs(3)(numRuns)
          case "Q5"  => funs(4)(numRuns)
          case "Q6"  => funs(5)(numRuns)
          case "Q7"  => funs(6)(numRuns)
          case "Q8"  => funs(7)(numRuns)
          case "Q9"  => funs(8)(numRuns)
          case "Q10" => funs(9)(numRuns)
          case "Q12" => funs(10)(numRuns)
          case "Q17" => funs(11)(numRuns)
          case _     => throw new Exception("Query not supported!")
        }
        // Check results
        if (Config.checkResults) {
          if (new java.io.File(getResultFileName).exists) {
            val resq = scala.io.Source.fromFile(getOutputName).mkString
            val resc = {
              val str = scala.io.Source.fromFile(getResultFileName).mkString
              str * numRuns
            }
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

object MiniDB extends LegoRunner with storagemanager.Loader with Queries {

  def main(args: Array[String]) {
    val q1 = (x: Int) => Q1(x)
    val q2 = (x: Int) => Q2(x)
    val q3 = (x: Int) => Q3(x)
    val q4 = (x: Int) => Q4(x)
    val q5 = (x: Int) => Q5(x)
    val q6 = (x: Int) => Q6(x)
    val q7 = (x: Int) => Q7(x)
    val q8 = (x: Int) => Q8(x)
    val q9 = (x: Int) => Q9(x)
    val q10 = (x: Int) => Q10(x)
    val q12 = (x: Int) => Q12(x)
    val q17 = (x: Int) => Q17(x)
    run(args, List(q1, q2, q3, q4, q5, q6, q7, q8, q9, q10, q12, q17))
  }
}
