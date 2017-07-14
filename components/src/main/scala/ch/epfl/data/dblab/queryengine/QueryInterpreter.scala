package ch.epfl.data
package dblab
package queryengine

import schema._
import frontend.parser._
import frontend.optimizer._
import frontend.analyzer._
import utils.Utilities._
import java.io.PrintStream
import frontend.parser.OperatorAST._
import config._
import schema._

/**
 * The Query Interpreter module which reads a SQL query and interprets it.
 *
 * @author Yannis Klonatos
 */
object QueryInterpreter {
  var currQuery: java.lang.String = ""
  var queryName: java.lang.String = ""
  Config.checkResults = true

  def getOutputName = queryName + "Output.txt"

  /**
   * The starting point of a query interpreter which uses the arguments as its setting.
   *
   * @param args the setting arguments passed through command line
   */
  // FIXME revert the name back to main
  def main2(args: Array[String]) {
    if (args.size < 1) {
      System.out.println("ERROR: Invalid number (" + args.length + ") of command line arguments!")
      System.out.println("USAGE: run <data_folder> <list of DDL files and SQL queries>")
      System.out.println("Example: run /home/data/sf0.1/ experimentation/tpch-sql/dss.ddl experimentation/tpch-sql/Q6.sql")
      System.exit(1)
    }
    // Set the folder containing data
    Config.datapath = args(0);
    val dataFolder = new java.io.File(Config.datapath)
    if (!dataFolder.exists || !dataFolder.isDirectory) {
      println("Data folder " + Config.datapath + " does not exist or is not a directory. Cannot proceed");
      return
    }

    val filesToExecute = args.tail.map(arg => {
      val f = new java.io.File(arg)
      if (!f.exists) {
        println("Warning: Command line parameter " + f + " is not a file or directory. Skipping this argument...")
        List()
      } else if (f.isDirectory) f.listFiles.map(arg + "/" + _.getName).toList
      else List(arg)
    }).flatten.groupBy(f => f.substring(f.lastIndexOf('.'), f.length))

    val ddlInterpreter = new DDLInterpreter(new Catalog(scala.collection.mutable.Map()))

    // TODO -- Ideally, the following should not be dependant on the file extension, but OK for now.
    for (f <- (filesToExecute.get(".ddl") ++ filesToExecute.get(".ri")).flatten.toList) {
      System.out.println("Executing file " + f)
      val ddlDefStr = scala.io.Source.fromFile(f).mkString
      val ddlObj = DDLParser.parse(ddlDefStr)
      ddlInterpreter.interpret(ddlObj)
    }
    //System.out.println(ddlInterpreter.getCurrSchema)

    // TODO -- That must be calculated as well in ddlInterpreter
    if (Config.gatherStats) {
      ddlInterpreter.getCurrSchema.stats += "NUM_YEARS_ALL_DATES" -> 7
      System.out.println(ddlInterpreter.getCurrSchema.stats.mkString("\n"))
    }

    // Now run all queries specified
    val schema = ddlInterpreter.getCurrSchema
    val subqNorm = new SQLSubqueryNormalizer(schema)

    for (q <- filesToExecute.get(".sql").toList.flatten) {
      currQuery = q
      queryName = q.substring(q.lastIndexOf('/') + 1, q.length).replace(".sql", "")
      println("Executing file " + q + " (queryName = " + queryName + ")")

      Console.withOut(new PrintStream(getOutputName)) {
        val sqlParserTree = SQLParser.parse(scala.io.Source.fromFile(q).mkString)
        if (Config.debugQueryPlan)
          System.out.println("Original SQL Parser Tree:\n" + sqlParserTree + "\n\n")

        val subqueryNormalizedqTree = subqNorm.normalize(sqlParserTree)
        if (Config.debugQueryPlan)
          System.out.println("After Subqyery Normalization:\n" + subqueryNormalizedqTree + "\n\n")

        new SQLAnalyzer(schema).checkAndInfer(subqueryNormalizedqTree)
        val operatorTree = new SQLToQueryPlan(schema).convert(subqueryNormalizedqTree)

        if (Config.debugQueryPlan)
          System.out.println("Before Optimizer:\n" + operatorTree + "\n\n")

        val optimizerTree = new QueryPlanNaiveOptimizer(schema).optimize(operatorTree)

        if (Config.debugQueryPlan)
          System.out.println("After Optimizer:\n" + optimizerTree + "\n\n")

        PlanExecutor.executeQuery(optimizerTree, schema)

        val resq = scala.io.Source.fromFile(getOutputName).mkString

        if (Config.debugQueryPlan)
          System.out.println(resq)

        // Check results
        if (Config.checkResults) {
          val resultFile = filesToExecute.get(".result").toList.flatten.filter(f => f.contains(queryName + ".result")) match {
            case elem :: _ => elem
            case List()    => ""
          }
          if (new java.io.File(resultFile).exists) {
            val resc = {
              val str = scala.io.Source.fromFile(resultFile).mkString
              str * Config.numRuns
            }
            if (resq != resc) {
              System.out.println("-----------------------------------------")
              System.out.println("QUERY " + q + " DID NOT RETURN CORRECT RESULT!!!")
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