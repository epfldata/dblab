package ch.epfl.data
package dblab
package queryengine

import schema._
import frontend.parser._
import frontend.optimizer._
import frontend.analyzer._
import java.io.PrintStream
import java.io.PrintWriter
import java.io.File
import frontend.parser.OperatorAST._
import config._
import schema._
import java.util.Date
import java.text.SimpleDateFormat;

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
  def main(args: Array[String]) {
    if (args.size < 1) {
      System.out.println("ERROR: Invalid number (" + args.length + ") of command line arguments!")
      System.out.println("USAGE: olap-interpreter <data_folder> <list of DDL files and SQL queries>")
      System.out.println("Example: olap-interpreter /home/data/sf0.1/ experimentation/tpch-sql/dss.ddl experimentation/tpch-sql/Q6.sql")
      System.exit(1)
    }

    val filesToExecute = args.toList.tail.map(arg => {
      val f = new java.io.File(arg)
      if (!f.exists) {
        println("Warning: Command line parameter " + f + " is not a file or directory. Skipping this argument...")
        List()
      } else if (f.isDirectory) f.listFiles.map(arg + "/" + _.getName).toList
      else List(arg)
    }).flatten.groupBy(f => f.substring(f.lastIndexOf('.'), f.length)).toMap

    interpret(args(0), filesToExecute)
  }

  def processQuery(schema: Schema, q: String): String = {
    val subqNorm = new SQLSubqueryNormalizer(schema)
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

      // System.out.println(subqueryNormalizedqTree)

      val namedQuery = new SQLNamer(schema).nameQuery(subqueryNormalizedqTree)
      // val namedQuery = subqueryNormalizedqTree
      val typedQuery = new SQLTyper(schema).typeQuery(namedQuery)
      // val typedQuery = namedQuery

      // System.out.println(typedQuery)

      // new SQLAnalyzer(schema).checkAndInfer(typedQuery)
      val operatorTree = new SQLToQueryPlan(schema).convert(typedQuery)
      visualize(operatorTree, queryName + "_normal")
      val costingPlan = new PlanCosting(schema, operatorTree)
      System.out.println("Normal cost: " + costingPlan.cost())
      //System.out.println(operatorTree)

      if (Config.debugQueryPlan)
        System.out.println("Before Optimizer:\n" + operatorTree + "\n\n")

      val optimizerTree = new QueryPlanNaiveOptimizer(schema).optimize(operatorTree)
      // val optimizerTree = operatorTree
      visualize(optimizerTree, queryName + "_optimized")
      val costingPlanOptimized = new PlanCosting(schema, optimizerTree)
      System.out.println("Optimized cost: " + costingPlan.cost())

      if (Config.debugQueryPlan)
        System.out.println("After Optimizer:\n" + optimizerTree + "\n\n")

      PlanExecutor.executeQuery(optimizerTree, schema)

      val resq = scala.io.Source.fromFile(getOutputName).mkString

      // if (Config.debugQueryPlan)
      resq
    }
  }

  def readSchema(dataPath: String, schemaFiles: List[String]): Schema = {
    // Set the folder containing data
    Config.datapath = dataPath
    val dataFolder = new java.io.File(Config.datapath)
    if (!dataFolder.exists || !dataFolder.isDirectory) {
      throw new Exception(s"Data folder ${Config.datapath} does not exist or is not a directory. Cannot proceed")
    }
    val ddlInterpreter = new DDLInterpreter(new Catalog(scala.collection.mutable.Map()))
    // TODO -- Ideally, the following should not be dependant on the file extension, but OK for now.
    for (f <- schemaFiles) {
      System.out.println("Executing file " + f)
      val ddlDefStr = scala.io.Source.fromFile(f).mkString
      val ddlObj = DDLParser.parse(ddlDefStr)
      ddlInterpreter.interpret(ddlObj)
    }
    //System.out.println(ddlInterpreter.getCurrSchema)

    // TODO -- That must be calculated as well in ddlInterpreter
    if (Config.gatherStats) {
      ddlInterpreter.getCurrSchema.stats += "NUM_YEARS_ALL_DATES" -> 7
      // System.out.println(ddlInterpreter.getCurrSchema.stats.mkString("\n"))
    }

    ddlInterpreter.getCurrSchema
  }

  def interpret(dataPath: String, filesToExecute: Map[String, List[String]]): Unit = {
    // Now run all queries specified
    val schema = readSchema(dataPath, (filesToExecute.get(".ddl") ++ filesToExecute.get(".ri")).flatten.toList)

    for (q <- filesToExecute.get(".sql").toList.flatten) {
      val resq = processQuery(schema, q)

      // if (Config.debugQueryPlan)
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

  def visualize(tree: QueryPlanTree, queryName: String) {
    def parseTree(tree: OperatorNode): String = {
      def getLabel(node: OperatorNode): String = {
        node match {
          case ScanOpNode(_, scanOpName, _)             => "SCAN " + scanOpName
          case UnionAllOpNode(_, _)                     => "U"
          case SelectOpNode(_, cond, _)                 => "Select " + cond.toString
          case JoinOpNode(_, _, clause, joinType, _, _) => joinType + " " + clause.toString
          case AggOpNode(_, aggs, gb, _)                => "Aggregate " + aggs.toString + " group by " + gb.map(_._2).toString
          case MapOpNode(_, mapIndices)                 => "Map " + mapIndices.map(mi => mi._1 + " = " + mi._1 + " / " + mi._2).mkString(", ").toString
          case OrderByNode(_, ob)                       => "Order by " + ob.map(ob => ob._1 + " " + ob._2).mkString(", ").toString
          case PrintOpNode(_, projNames, limit) => "Project " + projNames.map(p => p._2 match {
            case Some(al) => al
            case None     => p._1
          }).mkString(",").toString + {
            if (limit != -1) ", limit = " + limit.toString
            else ""
          }
          case SubqueryNode(parent)                        => "Subquery"
          case SubquerySingleResultNode(parent)            => "Single subquery"
          case ProjectOpNode(_, projNames, origFieldNames) => "Project " + (projNames zip origFieldNames).toString
          case ViewOpNode(_, _, name)                      => "View " + name
        }
      }

      def connectTwo(node1: OperatorNode, node2: OperatorNode, alias: String = ""): String = {
        if (alias == "") {
          "\"" + getLabel(node1) + "\" -> \"" + getLabel(node2) + "\";\n"
        } else {
          "\"" + getLabel(node1) + "\" -> \"" + alias + "\";\n"
        }
      }

      tree match {
        case ScanOpNode(_, _, _)                                  => ""
        case UnionAllOpNode(top, bottom)                          => connectTwo(tree, top) + connectTwo(tree, bottom) + parseTree(top) + parseTree(bottom)
        case SelectOpNode(parent, _, _)                           => connectTwo(tree, parent) + parseTree(parent)
        case JoinOpNode(left, right, _, _, leftAlias, rightAlias) => connectTwo(tree, left, leftAlias) + connectTwo(tree, right, rightAlias) + parseTree(right) + parseTree(left)
        case AggOpNode(parent, _, _, _)                           => connectTwo(tree, parent) + parseTree(parent)
        case MapOpNode(parent, _)                                 => connectTwo(tree, parent) + parseTree(parent)
        case OrderByNode(parent, ob)                              => connectTwo(tree, parent) + parseTree(parent)
        case PrintOpNode(parent, _, _)                            => connectTwo(tree, parent) + parseTree(parent)
        case SubqueryNode(parent)                                 => connectTwo(tree, parent) + parseTree(parent)
        case SubquerySingleResultNode(parent)                     => connectTwo(tree, parent) + parseTree(parent)
        case ProjectOpNode(parent, _, _)                          => connectTwo(tree, parent) + parseTree(parent)
        case ViewOpNode(parent, _, name)                          => connectTwo(tree, parent) + parseTree(parent)
      }
    }

    val dateFormat = new SimpleDateFormat("yyyy-MM-dd_HH-mm-ss");

    val date = new Date()

    val fileName = dateFormat.format(date) + "_" + queryName

    val pw = new PrintWriter(new File("/Users/michal/Desktop/SemesterProject/visualisations/" + fileName + ".gv"))

    pw.write("digraph G { \n	size=\"8,8\"\n")
    val edges = parseTree(tree.rootNode)
    pw.write(edges)
    pw.write("}")
    pw.close
  }
}