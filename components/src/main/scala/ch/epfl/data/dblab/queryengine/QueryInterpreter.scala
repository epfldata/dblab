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
import SQLAST._
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

      val costingPlan = new PlanCosting(schema, operatorTree)
      visualize(operatorTree, costingPlan, queryName + "_normal")
      System.out.println("Normal cost: " + costingPlan.cost())
      System.out.println(operatorTree)

      if (Config.debugQueryPlan)
        System.out.println("Before Optimizer:\n" + operatorTree + "\n\n")

      val optimizerTree = new QueryPlanNaiveOptimizer(schema).optimize(operatorTree)
      // val optimizerTree = operatorTree
      val costingPlanOptimized = new PlanCosting(schema, optimizerTree)
      visualize(optimizerTree, costingPlanOptimized, queryName + "_optimized")
      System.out.println("Optimized cost: " + costingPlan.cost())
      System.out.println(optimizerTree)

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
    addStats(schema)
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

  def visualize(tree: QueryPlanTree, planCosting: PlanCosting, queryName: String, graphType: String = "simple") {
    val simple = graphType.equals("simple")

    def parseTree(tree: OperatorNode): String = {
      val subquery = 0

      def connectTwo(node1: OperatorNode, node2: OperatorNode, alias: String = ""): String = {
        if (alias == "") {
          "\"" + getLabel(node1) + "\" -> \"" + getLabel(node2) + "\" [label=\"" + planCosting.size(node2) + "\"];\n"
        } else {
          "\"" + getLabel(node1) + "\" -> \"" + alias + "\" [label=\"" + planCosting.size(node2) + "\"];\n"
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

    def simplify(node: OperatorNode): String = node match {
      case ScanOpNode(_, scanOpName, _)                                => ""
      case UnionAllOpNode(top, bottom)                                 => "\"" + getLabel(node) + "\"" + " [label=\"∪\"];\n" + simplify(top) + simplify(bottom)
      case SelectOpNode(parent, _, _)                                  => "\"" + getLabel(node) + "\"" + " [label=\"σ\"];\n" + simplify(parent)
      case JoinOpNode(left, right, _, joinType, leftAlias, rightAlias) => "\"" + getLabel(node) + "\"" + " [label=\"⋈: " + joinType + "\"];\n" + simplify(left) + simplify(right)
      case AggOpNode(parent, _, _, _)                                  => "\"" + getLabel(node) + "\"" + " [label=\"Γ\"];\n" + simplify(parent)
      case MapOpNode(parent, _)                                        => "\"" + getLabel(node) + "\"" + " [label=\"X\"];\n" + simplify(parent)
      case OrderByNode(parent, ob)                                     => "\"" + getLabel(node) + "\"" + " [label=\"SORT\"];\n" + simplify(parent)
      case PrintOpNode(parent, _, _)                                   => "\"" + getLabel(node) + "\"" + " [label=\"Print\"];\n" + simplify(parent)
      case SubqueryNode(parent)                                        => "\"" + getLabel(node) + "\"" + " [label=\"Subquery\"];\n" + simplify(parent)
      case SubquerySingleResultNode(parent)                            => "\"" + getLabel(node) + "\"" + " [label=\"Subquery single\"];\n" + simplify(parent)
      case ProjectOpNode(parent, _, _)                                 => "\"" + getLabel(node) + "\"" + " [label=\"π\"];\n" + simplify(parent)
      case ViewOpNode(parent, _, name)                                 => "\"" + getLabel(node) + "\"" + " [label=\"View\"];\n" + simplify(parent)
    }

    val dateFormat = new SimpleDateFormat("MM-dd_HH-mm-ss");

    val date = new Date()

    val graphSuffix = if (simple) "_simple" else "_full"
    val fileName = queryName + "_" + dateFormat.format(date) + graphSuffix

    val pw = new PrintWriter(new File("/Users/michal/Desktop/SemesterProject/visualisations/" + fileName + ".gv"))

    pw.write("digraph G { \n	size=\"8,8\"\nrankdir = TB;\nedge[dir=back];\n")
    val simpleLabels = if (simple) simplify(tree.rootNode) else "gowno"
    val edges = parseTree(tree.rootNode)
    val result = simpleLabels + edges
    pw.write(result)
    pw.write("}")
    pw.close
  }

  def addStats(tpchSchema: Schema) {
    val YEARS = 7

    val lineItemTable = tpchSchema.findTable("LINEITEM") match {
      case Some(v) => v
      case None    => throw new Exception("Can't find table LINEITEM")
    }
    val nationTable = tpchSchema.findTable("NATION") match {
      case Some(v) => v
      case None    => throw new Exception("Can't find table NATION")
    }
    val ordersTable = tpchSchema.findTable("ORDERS") match {
      case Some(v) => v
      case None    => throw new Exception("Can't find table ORDERS")
    }
    val customerTable = tpchSchema.findTable("CUSTOMER") match {
      case Some(v) => v
      case None    => throw new Exception("Can't find table CUSTOMER")
    }
    val regionTable = tpchSchema.findTable("REGION") match {
      case Some(v) => v
      case None    => throw new Exception("Can't find table REGION")
    }
    val supplierTable = tpchSchema.findTable("SUPPLIER") match {
      case Some(v) => v
      case None    => throw new Exception("Can't find table SUPPLIER")
    }
    val partTable = tpchSchema.findTable("PART") match {
      case Some(v) => v
      case None    => throw new Exception("Can't find table PART")
    }
    val partsuppTable = tpchSchema.findTable("PARTSUPP") match {
      case Some(v) => v
      case None    => throw new Exception("Can't find table PARTSUPP")
    }

    tpchSchema.stats += "CARDINALITY_ORDERS" -> ordersTable.rowCount
    tpchSchema.stats += "CARDINALITY_CUSTOMER" -> customerTable.rowCount
    tpchSchema.stats += "CARDINALITY_LINEITEM" -> lineItemTable.rowCount
    tpchSchema.stats += "CARDINALITY_SUPPLIER" -> supplierTable.rowCount
    tpchSchema.stats += "CARDINALITY_PARTSUPP" -> partsuppTable.rowCount
    tpchSchema.stats += "CARDINALITY_PART" -> partTable.rowCount
    tpchSchema.stats += "CARDINALITY_NATION" -> nationTable.rowCount
    tpchSchema.stats += "CARDINALITY_REGION" -> regionTable.rowCount

    tpchSchema.stats += "CARDINALITY_Q1GRP" -> 4
    tpchSchema.stats += "CARDINALITY_Q3GRP" -> ordersTable.rowCount / 100
    tpchSchema.stats += "CARDINALITY_Q9GRP" -> nationTable.rowCount * YEARS
    tpchSchema.stats += "CARDINALITY_Q10GRP" -> customerTable.rowCount
    tpchSchema.stats += "CARDINALITY_Q20GRP" -> supplierTable.rowCount

    tpchSchema.stats += "DISTINCT_L_SHIPMODE" -> YEARS
    tpchSchema.stats += "DISTINCT_L_RETURNFLAG" -> 3
    tpchSchema.stats += "DISTINCT_L_LINESTATUS" -> 2
    tpchSchema.stats += "DISTINCT_L_ORDERKEY" -> ordersTable.rowCount * 5
    tpchSchema.stats += "DISTINCT_L_PARTKEY" -> partTable.rowCount
    tpchSchema.stats += "DISTINCT_L_SUPPKEY" -> supplierTable.rowCount
    tpchSchema.stats += "DISTINCT_N_NAME" -> nationTable.rowCount
    tpchSchema.stats += "DISTINCT_N_NATIONKEY" -> nationTable.rowCount
    tpchSchema.stats += "DISTINCT_O_SHIPPRIORITY" -> 1
    tpchSchema.stats += "DISTINCT_O_ORDERDATE" -> 365 * YEARS
    tpchSchema.stats += "DISTINCT_O_ORDERPRIORITY" -> 5
    tpchSchema.stats += "DISTINCT_O_ORDERKEY" -> ordersTable.rowCount * 5
    tpchSchema.stats += "DISTINCT_O_COMMENT" -> ordersTable.rowCount
    tpchSchema.stats += "DISTINCT_O_CUSTKEY" -> customerTable.rowCount
    tpchSchema.stats += "DISTINCT_P_PARTKEY" -> partTable.rowCount
    tpchSchema.stats += "DISTINCT_P_BRAND" -> 25
    tpchSchema.stats += "DISTINCT_P_SIZE" -> 50
    tpchSchema.stats += "DISTINCT_P_TYPE" -> 150
    tpchSchema.stats += "DISTINCT_P_NAME" -> partTable.rowCount
    tpchSchema.stats += "DISTINCT_PS_PARTKEY" -> partTable.rowCount
    tpchSchema.stats += "DISTINCT_PS_SUPPKEY" -> supplierTable.rowCount
    tpchSchema.stats += "DISTINCT_PS_AVAILQTY" -> 9999
    tpchSchema.stats += "DISTINCT_S_NAME" -> supplierTable.rowCount
    tpchSchema.stats += "DISTINCT_S_COMMENT" -> supplierTable.rowCount
    tpchSchema.stats += "DISTINCT_S_NATIONKEY" -> nationTable.rowCount
    tpchSchema.stats += "DISTINCT_S_SUPPKEY" -> supplierTable.rowCount
    tpchSchema.stats += "DISTINCT_C_CUSTKEY" -> customerTable.rowCount
    tpchSchema.stats += "DISTINCT_C_NAME" -> customerTable.rowCount
    tpchSchema.stats += "DISTINCT_C_NATIONKEY" -> nationTable.rowCount
    tpchSchema.stats += "DISTINCT_R_REGIONKEY" -> 5

    tpchSchema.stats.conflicts("PS_PARTKEY") = 16
    tpchSchema.stats.conflicts("P_PARTKEY") = 4
    tpchSchema.stats.conflicts("L_PARTKEY") = 64
    tpchSchema.stats.conflicts("L_ORDERKEY") = 8
    tpchSchema.stats.conflicts("C_NATIONKEY") = customerTable.rowCount / 20
    tpchSchema.stats.conflicts("S_NATIONKEY") = supplierTable.rowCount / 20

    tpchSchema.stats += "NUM_YEARS_ALL_DATES" -> YEARS

  }
}