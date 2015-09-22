package ch.epfl.data
package dblab.legobase
package tpch

import schema._
import utils.Utilities._
import java.io.PrintStream

/**
 * The main object for interpreting TPCH queries.
 */
object TPCHInterpreter extends TPCHRunner {
  import Queries._

  /**
   * Interprets the given TPCH query with the given scaling factor.
   *
   * @param query the input TPCH query (TODO should be generalized)
   */
  def executeQuery(query: String, schema: Schema): Unit = query match {
    case "Q1"             => Q1(Config.numRuns)
    case "Q2"             => Q2(Config.numRuns)
    case "Q3"             => Q3(Config.numRuns)
    case "Q4"             => Q4(Config.numRuns)
    case "Q5"             => Q5(Config.numRuns)
    case "Q6"             => Q6(Config.numRuns)
    case "Q7"             => Q7(Config.numRuns)
    case "Q8"             => Q8(Config.numRuns)
    case "Q9"             => Q9(Config.numRuns)
    case "Q10"            => Q10(Config.numRuns)
    case "Q11"            => Q11(Config.numRuns)
    case "Q12"            => Q12(Config.numRuns)
    case "Q13"            => Q13(Config.numRuns)
    case "Q14"            => Q14(Config.numRuns)
    case "Q15"            => Q15(Config.numRuns)
    case "Q16"            => Q16(Config.numRuns)
    case "Q17"            => Q17(Config.numRuns)
    case "Q18"            => Q18(Config.numRuns)
    case "Q19"            => Q19(Config.numRuns)
    case "Q20"            => Q20(Config.numRuns)
    case "Q21"            => Q21(Config.numRuns)
    case "Q22"            => Q22(Config.numRuns)
    case "Q1_functional"  => Q1_functional(Config.numRuns)
    case "Q2_functional"  => Q2_functional(Config.numRuns)
    case "Q3_functional"  => Q3_functional(Config.numRuns)
    case "Q4_functional"  => Q4_functional(Config.numRuns)
    case "Q5_functional"  => Q5_functional(Config.numRuns)
    case "Q6_functional"  => Q6_functional(Config.numRuns)
    case "Q9_functional"  => Q9_functional(Config.numRuns)
    case "Q10_functional" => Q10_functional(Config.numRuns)
    case "Q12_functional" => Q12_functional(Config.numRuns)
    case "Q14_functional" => Q14_functional(Config.numRuns)
    case dflt @ _         => throw new Exception("Query " + dflt + " not supported!")
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
