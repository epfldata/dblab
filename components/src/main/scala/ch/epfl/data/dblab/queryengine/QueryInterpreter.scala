package ch.epfl.data
package dblab
package queryengine

import frontend.parser._
import frontend.optimizer._
import frontend.analyzer._
import frontend.visualizer._
import java.io.PrintStream
import java.io.PrintWriter
import java.io.File
import frontend.parser.OperatorAST._
import config._
import schema._
import java.util.Date
import java.text.SimpleDateFormat
import sc.pardis.types._
import scala.collection.mutable.ArrayBuffer

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
        System.out.println("After Subquery Normalization:\n" + subqueryNormalizedqTree + "\n\n")

      // System.out.println(subqueryNormalizedqTree)

      val namedQuery = new SQLNamer(schema).nameQuery(subqueryNormalizedqTree)
      // val namedQuery = subqueryNormalizedqTree
      val typedQuery = new SQLTyper(schema).typeQuery(namedQuery)
      // val typedQuery = namedQuery

      // System.out.println(typedQuery)

      // new SQLAnalyzer(schema).checkAndInfer(typedQuery)
      val selingerOptimizer = new SelingerOptimizer(schema)
      val operatorTree = new SQLToQueryPlan(schema).convert(typedQuery)

      val costingPlan = new PlanCosting(schema, operatorTree)
      DOTvisualizer.visualize(operatorTree, costingPlan, queryName + "_normal", false)
      System.out.println("Normal cost: " + costingPlan.cost())

      if (Config.debugQueryPlan)
        System.out.println("Before Optimizer:\n" + operatorTree + "\n\n")

      System.out.println("Optimizing with selinger")
      val selingerOptimizerTree = selingerOptimizer.optimize(operatorTree)
      System.out.println("End of selinger optimizing")
      val selingerCosting = new PlanCosting(schema, selingerOptimizerTree)
      //System.out.println("Optimized joins: \n" + selingerOptimizerTree)
      DOTvisualizer.visualize(selingerOptimizerTree, selingerCosting, queryName + "_selinger", true)
      //System.out.println("Optimized cost: " + selingerCosting.cost())

      val optimizerTree = new QueryPlanNaiveOptimizer(schema).optimize(operatorTree)
      val costingPlanOptimized = new PlanCosting(schema, optimizerTree)
      DOTvisualizer.visualize(optimizerTree, costingPlanOptimized, queryName + "_naive", true)

      //System.out.println(optimizerTree)
      if (Config.debugQueryPlan)
        System.out.println("After Optimizer:\n" + optimizerTree + "\n\n")

      System.out.println("Normal result")
      PlanExecutor.executeQuery(optimizerTree, schema)

      val resq = scala.io.Source.fromFile(getOutputName).mkString

      System.out.println("Normal result")
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
    //TPCHSchema.getSchema(dataPath, 1.0)
    ddlInterpreter.getCurrSchema
  }

  def interpret(dataPath: String, filesToExecute: Map[String, List[String]]): Unit = {
    // Now run all queries specified
    val schema = readSchema(dataPath, (filesToExecute.get(".ddl") ++ filesToExecute.get(".ri")).flatten.toList)
    val schemaWithStats = addStats(schema)
    for (q <- filesToExecute.get(".sql").toList.flatten) {
      val resq = processQuery(schemaWithStats, q)

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

  def addStats(tpchSchema: Schema): Schema = {
    val YEARS = 7
    val SCALING_FACTOR = 10.0

    val lineItem = tpchSchema.findTable("LINEITEM") match {
      case Some(v) => v
      case None    => throw new Exception("Can't find table LINEITEM")
    }
    val nation = tpchSchema.findTable("NATION") match {
      case Some(v) => v
      case None    => throw new Exception("Can't find table NATION")
    }
    val orders = tpchSchema.findTable("ORDERS") match {
      case Some(v) => v
      case None    => throw new Exception("Can't find table ORDERS")
    }
    val customer = tpchSchema.findTable("CUSTOMER") match {
      case Some(v) => v
      case None    => throw new Exception("Can't find table CUSTOMER")
    }
    val region = tpchSchema.findTable("REGION") match {
      case Some(v) => v
      case None    => throw new Exception("Can't find table REGION")
    }
    val supplier = tpchSchema.findTable("SUPPLIER") match {
      case Some(v) => v
      case None    => throw new Exception("Can't find table SUPPLIER")
    }
    val part = tpchSchema.findTable("PART") match {
      case Some(v) => v
      case None    => throw new Exception("Can't find table PART")
    }
    val partsupp = tpchSchema.findTable("PARTSUPP") match {
      case Some(v) => v
      case None    => throw new Exception("Can't find table PARTSUPP")
    }

    val lineItemTable = {
      val L_ORDERKEY: Attribute = "L_ORDERKEY" -> IntType
      val L_LINENUMBER: Attribute = "L_LINENUMBER" -> IntType

      new Table(lineItem.name, lineItem.attributes, ArrayBuffer(
        PrimaryKey(List(L_ORDERKEY, L_LINENUMBER)),
        ForeignKey("LINEITEM", "ORDERS", List(("L_ORDERKEY", "O_ORDERKEY"))),
        ForeignKey("LINEITEM", "PARTSUPP", List(("L_PARTKEY", "PS_PARTKEY"), ("L_SUPPKEY", "PS_SUPPKEY")))), lineItem.resourceLocator, lineItem.rowCount)
    }

    val nationTable = {
      val N_NATIONKEY: Attribute = "N_NATIONKEY" -> IntType
      val N_NAME: Attribute = Attribute("N_NAME", VarCharType(25), List(Compressed))

      new Table(nation.name, nation.attributes, ArrayBuffer(
        PrimaryKey(List(N_NATIONKEY)),
        Continuous(N_NATIONKEY, 0),
        ForeignKey("NATION", "REGION", List(("N_REGIONKEY", "R_REGIONKEY")))), nation.resourceLocator, nation.rowCount)
    }

    val ordersTable = {
      val O_ORDERKEY: Attribute = "O_ORDERKEY" -> IntType
      val O_COMMENT = Attribute("O_COMMENT", VarCharType(79), List(Compressed))
      val O_ORDERPRIORITY = Attribute("O_ORDERPRIORITY", VarCharType(15))

      new Table(orders.name, orders.attributes, ArrayBuffer(
        PrimaryKey(List(O_ORDERKEY)),
        ForeignKey("ORDERS", "CUSTOMER", List(("O_CUSTKEY", "C_CUSTKEY")))), orders.resourceLocator, orders.rowCount)
    }

    val customerTable = {
      val ck: Attribute = "C_CUSTKEY" -> IntType

      new Table(customer.name, customer.attributes, ArrayBuffer(
        PrimaryKey(List(ck)),
        Continuous(ck, 1),
        ForeignKey("CUSTOMER", "NATION", List(("C_NATIONKEY", "N_NATIONKEY")))), customer.resourceLocator, customer.rowCount)
    }

    val regionTable = {
      val R_REGIONKEY: Attribute = "R_REGIONKEY" -> IntType

      new Table(region.name, region.attributes, ArrayBuffer(
        PrimaryKey(List(R_REGIONKEY)),
        Continuous(R_REGIONKEY, 0)), region.resourceLocator, region.rowCount)
    }

    val supplierTable = {
      val sk: Attribute = "S_SUPPKEY" -> IntType

      new Table(supplier.name, supplier.attributes, ArrayBuffer(
        PrimaryKey(List(sk)),
        Continuous(sk, 1),
        ForeignKey("SUPPLIER", "NATION", List(("S_NATIONKEY", "N_NATIONKEY")))), supplier.resourceLocator, supplier.rowCount)
    }

    val partTable = {
      val P_PARTKEY: Attribute = "P_PARTKEY" -> IntType

      new Table(part.name, part.attributes, ArrayBuffer(
        PrimaryKey(List(P_PARTKEY)),
        Continuous(P_PARTKEY, 1)), part.resourceLocator, part.rowCount)
    }

    val partsuppTable = {
      val pk: Attribute = "PS_PARTKEY" -> IntType
      val sk: Attribute = "PS_SUPPKEY" -> IntType

      new Table(partsupp.name, partsupp.attributes, ArrayBuffer(
        PrimaryKey(List(pk, sk)),
        ForeignKey("PARTSUPP", "PART", List(("PS_PARTKEY", "P_PARTKEY"))),
        ForeignKey("PARTSUPP", "SUPPLIER", List(("PS_SUPPKEY", "S_SUPPKEY")))), partsupp.resourceLocator, partsupp.rowCount)
    }

    val newSchema = new Schema(List(lineItemTable, regionTable, nationTable, supplierTable, partTable, partsuppTable, customerTable, ordersTable))

    newSchema.stats += "CARDINALITY_ORDERS" -> ordersTable.rowCount * SCALING_FACTOR
    newSchema.stats += "CARDINALITY_CUSTOMER" -> customerTable.rowCount * SCALING_FACTOR
    newSchema.stats += "CARDINALITY_LINEITEM" -> lineItemTable.rowCount * SCALING_FACTOR
    newSchema.stats += "CARDINALITY_SUPPLIER" -> supplierTable.rowCount * SCALING_FACTOR
    newSchema.stats += "CARDINALITY_PARTSUPP" -> partsuppTable.rowCount * SCALING_FACTOR
    newSchema.stats += "CARDINALITY_PART" -> partTable.rowCount * SCALING_FACTOR
    newSchema.stats += "CARDINALITY_NATION" -> nationTable.rowCount
    newSchema.stats += "CARDINALITY_REGION" -> regionTable.rowCount

    newSchema.stats += "CARDINALITY_Q1GRP" -> 4
    newSchema.stats += "CARDINALITY_Q3GRP" -> ordersTable.rowCount / 100
    newSchema.stats += "CARDINALITY_Q9GRP" -> nationTable.rowCount * YEARS
    newSchema.stats += "CARDINALITY_Q10GRP" -> customerTable.rowCount * SCALING_FACTOR
    newSchema.stats += "CARDINALITY_Q20GRP" -> supplierTable.rowCount * SCALING_FACTOR

    newSchema.stats += "DISTINCT_L_SHIPMODE" -> YEARS
    newSchema.stats += "DISTINCT_L_RETURNFLAG" -> 3
    newSchema.stats += "DISTINCT_L_LINESTATUS" -> 2
    newSchema.stats += "DISTINCT_L_ORDERKEY" -> ordersTable.rowCount * 5 * SCALING_FACTOR
    newSchema.stats += "DISTINCT_L_PARTKEY" -> partTable.rowCount * SCALING_FACTOR
    newSchema.stats += "DISTINCT_L_SUPPKEY" -> supplierTable.rowCount * SCALING_FACTOR
    newSchema.stats += "DISTINCT_N_NAME" -> nationTable.rowCount
    newSchema.stats += "DISTINCT_N_NATIONKEY" -> nationTable.rowCount
    newSchema.stats += "DISTINCT_O_SHIPPRIORITY" -> 1
    newSchema.stats += "DISTINCT_O_ORDERDATE" -> 365 * YEARS
    newSchema.stats += "DISTINCT_O_ORDERPRIORITY" -> 5
    newSchema.stats += "DISTINCT_O_ORDERKEY" -> ordersTable.rowCount * 5 * SCALING_FACTOR
    newSchema.stats += "DISTINCT_O_COMMENT" -> ordersTable.rowCount * SCALING_FACTOR
    newSchema.stats += "DISTINCT_O_CUSTKEY" -> customerTable.rowCount * SCALING_FACTOR
    newSchema.stats += "DISTINCT_P_PARTKEY" -> partTable.rowCount * SCALING_FACTOR
    newSchema.stats += "DISTINCT_P_BRAND" -> 25
    newSchema.stats += "DISTINCT_P_SIZE" -> 50
    newSchema.stats += "DISTINCT_P_TYPE" -> 150
    newSchema.stats += "DISTINCT_P_NAME" -> partTable.rowCount * SCALING_FACTOR
    newSchema.stats += "DISTINCT_PS_PARTKEY" -> partTable.rowCount * SCALING_FACTOR
    newSchema.stats += "DISTINCT_PS_SUPPKEY" -> supplierTable.rowCount * SCALING_FACTOR
    newSchema.stats += "DISTINCT_PS_AVAILQTY" -> 9999
    newSchema.stats += "DISTINCT_S_NAME" -> supplierTable.rowCount * SCALING_FACTOR
    newSchema.stats += "DISTINCT_S_COMMENT" -> supplierTable.rowCount * SCALING_FACTOR
    newSchema.stats += "DISTINCT_S_NATIONKEY" -> nationTable.rowCount
    newSchema.stats += "DISTINCT_S_SUPPKEY" -> supplierTable.rowCount * SCALING_FACTOR
    newSchema.stats += "DISTINCT_C_CUSTKEY" -> customerTable.rowCount * SCALING_FACTOR
    newSchema.stats += "DISTINCT_C_NAME" -> customerTable.rowCount * SCALING_FACTOR
    newSchema.stats += "DISTINCT_C_NATIONKEY" -> nationTable.rowCount
    newSchema.stats += "DISTINCT_R_REGIONKEY" -> 5

    newSchema.stats.conflicts("PS_PARTKEY") = 16
    newSchema.stats.conflicts("P_PARTKEY") = 4
    newSchema.stats.conflicts("L_PARTKEY") = 64
    newSchema.stats.conflicts("L_ORDERKEY") = 8
    newSchema.stats.conflicts("C_NATIONKEY") = customerTable.rowCount / 20
    newSchema.stats.conflicts("S_NATIONKEY") = supplierTable.rowCount / 20

    newSchema.stats += "NUM_YEARS_ALL_DATES" -> YEARS

    newSchema

  }
}
