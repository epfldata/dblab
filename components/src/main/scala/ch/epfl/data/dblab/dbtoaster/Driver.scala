package ch.epfl.data
package dblab
package dbtoaster

import schema._
import frontend.parser._
import frontend.optimizer._
import frontend.analyzer._
import utils.Utilities._
import java.io.PrintStream

import ch.epfl.data.dblab.frontend.parser.DDLAST.UseSchema
import ch.epfl.data.dblab.frontend.parser.SQLAST._
import frontend.parser.OperatorAST._
import config._
import schema._

object Driver {
  /**
   * The starting point of DBToaster
   *
   * @param args the setting arguments passed through command line
   */
  def main(args: Array[String]) {
    if (args.length < 1) {
      System.out.println("ERROR: Invalid number (" + args.length + ") of command line arguments!")
      System.out.println("USAGE: run <SQL query>")
      System.out.println("Example: run experimentation/tpch-sql/Q6.sql")
      System.exit(1)
    }
    val filesToExecute = args.map(arg => {
      val f = new java.io.File(arg)
      if (!f.exists) {
        println("Warning: Command line parameter " + f + " is not a file or directory. Skipping this argument...")
        List()
      } else if (f.isDirectory) f.listFiles.map(arg + "/" + _.getName).toList
      else List(arg)
    }).flatten.groupBy(f => f.substring(f.lastIndexOf('.'), f.length))

    for (q <- args) {
      val sqlParserTree = SQLParser.parseStream(scala.io.Source.fromFile(q).mkString)
      val sqlProgram = sqlParserTree.asInstanceOf[IncludeStatement]
      val tables = sqlProgram.streams.toList.map(x => x.asInstanceOf[CreateStream]) // ok ?
      val ddlInterpreter = new DDLInterpreter(new Catalog(scala.collection.mutable.Map()))
      val query = sqlProgram.body
      //      println(query)
      val schema = ddlInterpreter.interpret(UseSchema("DBToaster") :: tables)
      val namedQuery = new SQLNamer(schema).nameQuery(query)
      val calc_expr = SQLToCalc.CalcOfQuery(None, tables, namedQuery)
      calc_expr.map({ case (tgt_name, tgt_calc) => tgt_name + " : \n" + CalcAST.prettyprint(tgt_calc) }).foreach(println)
      //      calc_expr.map({ case (tgt_name, tgt_calc) => tgt_name + " : \n" + tgt_calc }).foreach(println) // TODO this is for test
      //      if (Config.debugQueryPlan)
      //        System.out.println("Original SQL Parser Tree:\n" + sqlParserTree + "\n\n")
    }
  }
}