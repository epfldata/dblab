package ch.epfl.data
package dblab
package experimentation
package runner

import schema._
import utils.Utilities._
import java.io.PrintStream
import config._

/**
 * The common trait for all Query Runners (either a Query Interpreter or a Query Compiler)
 */
trait QueryRunner {
  /**
   * Executes the given query with the given schema.
   *
   * This method should be implemented by a query interpreter to interpret the given
   * query or by a query compiler to compile the given query.
   *
   * @param query the input query
   */
  def executeQuery(query: String, schema: Schema): Unit

  /**
   * The result file associated to the given query
   */
  def getResultFileName(query: String): String

  /**
   * In the case of running multiple queries, returns the list of queries
   */
  def getQueries(args: Array[String]): List[String]

  /**
   * Returns the schema based on the given arguments
   */
  def getSchema(args: Array[String]): Schema

  /**
   * Preprocesses the given arguments
   */
  def preprocessArgs(args: Array[String]): Unit

  /**
   * Returns the output file name for the given query
   */
  def getOutputName(query: String): String

  /**
   * The starting point of a query runner which uses the arguments as its setting.
   *
   * @param args the setting arguments passed through command line
   */
  def run(args: Array[String]) {
    preprocessArgs(args)
    val schema = getSchema(args)
    val queries = getQueries(args)
    for (currQuery <- queries) {
      val outputName = getOutputName(currQuery)
      Console.withOut(new PrintStream(outputName)) {
        executeQuery(currQuery, schema)
        // Check results
        if (Config.checkResults) {
          val resultFileName = getResultFileName(currQuery)
          val resq = scala.io.Source.fromFile(outputName).mkString
          if (new java.io.File(resultFileName).exists) {
            val resc = {
              val str = scala.io.Source.fromFile(resultFileName).mkString
              str * Config.numRuns
            }
            if (resq != resc) {
              System.out.println("-----------------------------------------")
              System.out.println("QUERY" + currQuery + " DID NOT RETURN CORRECT RESULT!!!")
              System.out.println("Correct result:")
              System.out.println(resc)
              System.out.println("Result obtained from execution:")
              System.out.println(resq)
              System.out.println("-----------------------------------------")
              //System.exit(0)
            } else System.out.println("CHECK RESULT FOR QUERY " + currQuery + ": [OK]")
          } else {
            System.out.println(s"Reference result file '$resultFileName' not found. Skipping checking of result")
            if (Config.printResult) {
              System.out.println("Execution results:")
              System.out.println(resq)
            }
          }
        }
      }
    }
  }
}
