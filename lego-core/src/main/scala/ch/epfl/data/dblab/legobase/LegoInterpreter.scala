package ch.epfl.data
package dblab.legobase

import utils.Utilities._
import java.io.PrintStream

/**
 * The main object for interpreting queries.
 */
object LegoInterpreter extends LegoRunner {
  import tpch.Queries._

  /**
   * Interprets the given TPCH query with the given scaling factor.
   *
   * @param query the input TPCH query (TODO should be generalized)
   * @param scalingFactor the scaling factor for TPCH queries (TODO should be generalize)
   */
  def executeQuery(query: String, scalingFactor: Double): Unit = query match {
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
