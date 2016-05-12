package ch.epfl.data
package dblab
package legobase
package experimentation
package tpch

import compiler._
import schema._
import deep._
import prettyprinter._
import transformers._
import sc.pardis.optimization._
import sc.pardis.ir._
import sc.pardis.types.PardisTypeImplicits._
import sc.pardis.types._
import deep.quasi._
import legobase.deep.LegoBaseQueryEngineExp
import dblab.experimentation.tpch._
import config.Config
import sc.pardis.shallow.OptimalString

/**
 * Generates synthetic queries produced using quasi quotes.
 *
 * Run it with this command:
 * test:run /mnt/ramdisk/tpch 8 Q6_functional +monad-lowering +monad-iterator +malloc-hoist -name-with-flag
 *
 * Or to generate all the micro benchmarks for fusion run the following command:
 * test:run /mnt/ramdisk/tpch fusion_micro
 */
object SyntheticQueries extends TPCHRunner {

  var settings: Settings = _

  val ONE_LOADER_FOR_ALL = false
  val oneAgg = false
  // val oneAgg = true

  def datesGenerator: List[String] = {
    // val years = 1992 to 1998
    // 1996
    // val months = (1 to 12 by 2) map (x => if (x < 10) s"0$x" else x.toString)
    // val dates = for (y <- years; m <- months) yield s"$y-$m-01"
    // val days = 1 to 30 map (x => if (x < 10) s"0$x" else x.toString)
    // val dates = for (d <- days) yield s"1996-12-$d"
    // val dates = for (d <- days) yield s"1998-11-$d"
    // val dates = for (y <- List(4, 5, 6, 7, 8); m <- if (y == 4) List(7) else List(1, 7)) yield s"199$y-0$m-01"
    val dates = List("1998-01-01")
    dates.toList
  }

  var param: String = _

  def process(args: List[String]): Unit = process(args.toArray)

  def process(args: Array[String]): Unit = {
    newContext()
    Config.checkResults = false
    settings = new Settings(args.toList)

    if (ONE_LOADER_FOR_ALL) {
      run(args)
    } else {
      for (d <- datesGenerator) {
        param = d
        run(args)
      }
    }
  }

  val MICRO_RUNS = 10

  def fusionMicroBenchmark(args: Array[String]): Unit = {
    val folder = args(0)

    val fixedFlags = List("+monad-lowering", "-name-with-flag",
      "+force-compliant", "+monad-opt", "-name-with-flag")
    val variableFlags = List(List("+monad-cps"), List("+monad-iterator"), List("+monad-stream"),
      List("+monad-stream", "+monad-stream-church"))
    val SFs1 = List(8, 16, 32)
    val queries1 = List("fc", "fms", "ffms")
    val SFs2 = List(1)
    val queries2 = List("fm", "fmt")
    for (flags <- variableFlags) {
      for (sf <- SFs1) {
        for (q <- queries1) {
          process(folder :: sf.toString :: q :: flags ++ fixedFlags)
        }
      }

      for (sf <- SFs2) {
        for (q <- queries2) {
          process(folder :: sf.toString :: q :: flags ++ fixedFlags)
        }
      }
    }
  }

  def main(args: Array[String]): Unit = {
    if (args.length == 2 && args(1) == "fusion_micro") {
      fusionMicroBenchmark(args)
    } else if (args.length < 3) {
      System.out.println("ERROR: Invalid number (" + args.length + ") of command line arguments!")
      System.exit(0)
    } else {
      process(args)
    }
  }

  var _context: LegoBaseQueryEngineExp = _

  def newContext(): Unit = {
    _context = new LegoBaseQueryEngineExp {}
  }

  implicit def context: LegoBaseQueryEngineExp = _context

  type Rep[T] = LegoBaseQueryEngineExp#Rep[T]

  def query12_p2(numRuns: Int): Rep[Unit] = {
    import dblab.queryengine.monad.Query
    import dblab.experimentation.tpch.TPCHLoader._
    import dblab.queryengine.GenericEngine._
    val lineitemTable = dsl"""Query(loadLineitem())"""
    val ordersTable = dsl"Query(loadOrders())"
    // val endDate = "1994-07-01"
    // val endDate = "1998-12-01"
    val hasFilter = true
    def generateQueryForEndDate(endDate: String): Rep[Unit] = {
      def selection = if (hasFilter)
        dsl"""
          val mail = parseString("MAIL")
          val ship = parseString("SHIP")
          val constantDate = parseDate($endDate)
          val constantDate2 = parseDate("1994-01-01")
          val so2 = $lineitemTable.filter(x =>
            x.L_RECEIPTDATE < constantDate && x.L_COMMITDATE < constantDate && x.L_SHIPDATE < constantDate && x.L_SHIPDATE < x.L_COMMITDATE && x.L_COMMITDATE < x.L_RECEIPTDATE && x.L_RECEIPTDATE >= constantDate2 && (x.L_SHIPMODE === mail || x.L_SHIPMODE === ship))
          so2
        """
      else
        lineitemTable
      def selectivity = dsl"""
          val filtered = $selection
          val original = $lineitemTable
          filtered.count.asInstanceOf[Double] / original.count
        """
      dsl"""
        printf($endDate)
        printf("\nselectivity: %f\n", $selectivity)
        printf("==========\n")"""
      dsl"""
        runQuery({
          val so2 = $selection
          val jo = $ordersTable.mergeJoin(so2)((x, y) => x.O_ORDERKEY - y.L_ORDERKEY)((x, y) => x.O_ORDERKEY == y.L_ORDERKEY)
          val URGENT = parseString("1-URGENT")
          val HIGH = parseString("2-HIGH")
          val aggOp = jo.groupBy(x => x.L_SHIPMODE[OptimalString]).mapValues(list =>
            Array(list.filter(t => t.O_ORDERPRIORITY[OptimalString] === URGENT || t.O_ORDERPRIORITY[OptimalString] === HIGH).count,
              list.filter(t => t.O_ORDERPRIORITY[OptimalString] =!= URGENT && t.O_ORDERPRIORITY[OptimalString] =!= HIGH).count))
          aggOp.printRows(kv =>
            printf("%s|%d|%d-", kv._1.string, kv._2(0), kv._2(1)), -1)
        })
      """
    }
    assert(!ONE_LOADER_FOR_ALL)
    if (ONE_LOADER_FOR_ALL) {
      for (d <- datesGenerator) {
        generateQueryForEndDate(d)
      }
      context.unit()
    } else {
      generateQueryForEndDate(param)
    }
  }

  def querySimple(numRuns: Int): Rep[Unit] = {
    import dblab.queryengine.monad.Query
    import dblab.experimentation.tpch.TPCHLoader._
    import dblab.queryengine.GenericEngine._
    dsl"""
      val result = {
        var sumResult = 0.0
        for(t <- Query(loadLineitem())) {
          sumResult += t.L_EXTENDEDPRICE
        }
        sumResult
      }
      printf("%d", result)
      """
  }

  def filterCount(numRuns: Int): Rep[Unit] = {
    import dblab.queryengine.monad.Query
    import dblab.experimentation.tpch.TPCHLoader._
    import dblab.queryengine.GenericEngine._
    val loadedLineitemTable = dsl"loadLineitem()"
    def lineitemTable = dsl"""Query($loadedLineitemTable)"""
    val startDate = "1995-12-01"
    for (i <- 0 until numRuns) {
      dsl"""
        runQuery {
          val constantDate1: Int = parseDate($startDate)
          val result = $lineitemTable.filter(_.L_SHIPDATE >= constantDate1).
            count
          printf("%d\n", result)
        }
      """
    }
    dsl"()"
  }

  def filterMapSum(numRuns: Int): Rep[Unit] = {
    import dblab.queryengine.monad.Query
    import dblab.experimentation.tpch.TPCHLoader._
    import dblab.queryengine.GenericEngine._
    val loadedLineitemTable = dsl"loadLineitem()"
    def lineitemTable = dsl"""Query($loadedLineitemTable)"""
    val startDate = "1995-12-01"
    for (i <- 0 until numRuns) {
      dsl"""
        runQuery {
          val constantDate1: Int = parseDate($startDate)
          val result = $lineitemTable.filter(_.L_SHIPDATE >= constantDate1).
            map(t => t.L_EXTENDEDPRICE * t.L_DISCOUNT).foldLeft(0.0)(_ + _)
          printf("%.4f\n", result)
        }
      """
    }
    dsl"()"
  }

  def filterMap(numRuns: Int): Rep[Unit] = {
    import dblab.queryengine.monad.Query
    import dblab.experimentation.tpch.TPCHLoader._
    import dblab.queryengine.GenericEngine._
    val loadedLineitemTable = dsl"loadLineitem()"
    def lineitemTable = dsl"""Query($loadedLineitemTable)"""
    val startDate = "1998-01-01"
    for (i <- 0 until numRuns) {
      dsl"""
        runQuery {
          val constantDate1: Int = parseDate($startDate)
          $lineitemTable.filter(_.L_SHIPDATE >= constantDate1).
            map(t => t.L_EXTENDEDPRICE * t.L_DISCOUNT).printRows(t =>
              printf("%.4f\n", t)
            , -1)
        }
      """
    }
    dsl"()"
  }

  def filterMapTake(numRuns: Int): Rep[Unit] = {
    import dblab.queryengine.monad.Query
    import dblab.experimentation.tpch.TPCHLoader._
    import dblab.queryengine.GenericEngine._
    val loadedLineitemTable = dsl"loadLineitem()"
    def lineitemTable = dsl"""Query($loadedLineitemTable)"""
    val startDate = "1998-01-01"
    for (i <- 0 until numRuns) {
      dsl"""
        runQuery {
          val constantDate1: Int = parseDate($startDate)
          $lineitemTable.filter(_.L_SHIPDATE >= constantDate1).
            map(t => t.L_EXTENDEDPRICE * t.L_DISCOUNT).take(100).printRows(t =>
              printf("%.4f\n", t)
            , -1)
        }
      """
    }
    dsl"()"
  }

  def filterFilterMapSum(numRuns: Int): Rep[Unit] = {
    import dblab.queryengine.monad.Query
    import dblab.experimentation.tpch.TPCHLoader._
    import dblab.queryengine.GenericEngine._
    val loadedLineitemTable = dsl"loadLineitem()"
    def lineitemTable = dsl"""Query($loadedLineitemTable)"""
    val startDate = "1995-12-01"
    val endDate = "1997-01-01"
    for (i <- 0 until numRuns) {
      dsl"""
        runQuery {
          val constantDate1: Int = parseDate($startDate)
          val constantDate2: Int = parseDate($endDate)
          val result = $lineitemTable.filter(_.L_SHIPDATE >= constantDate1).
            filter(_.L_SHIPDATE < constantDate2).
            map(t => t.L_EXTENDEDPRICE * t.L_DISCOUNT).foldLeft(0.0)(_ + _)
          printf("%.4f\n", result)
        }
      """
    }
    dsl"()"
  }

  def query6(numRuns: Int): Rep[Unit] = {
    import dblab.queryengine.monad.Query
    import dblab.experimentation.tpch.TPCHLoader._
    import dblab.queryengine.GenericEngine._
    val lineitemTable = dsl"""Query(loadLineitem())"""

    def generateQueryForStartDate(startDate: String): Rep[Unit] = {
      def selection = dsl"""
        val constantDate1: Int = parseDate($startDate)
        val constantDate2: Int = parseDate("1997-01-01")
        $lineitemTable
          //.filter(x => x.L_SHIPDATE >= constantDate1 && (x.L_SHIPDATE < constantDate2 && (x.L_DISCOUNT >= 0.08 && (x.L_DISCOUNT <= 0.1 && (x.L_QUANTITY < 24))))) 
          .filter(x => x.L_SHIPDATE >= constantDate1)
        """
      def selectivity = dsl"""
          val filtered = $selection
          val original = $lineitemTable
          filtered.count.asInstanceOf[Double] / original.count
        """
      dsl"""
        printf($startDate)
        printf("\nselectivity: %f\n", $selectivity)
        printf("==========\n")"""
      if (oneAgg)
        dsl"""
          runQuery {
            val result = {
              val filtered = $selection
              var sumResult = 0.0
              for(t <- filtered) {
                sumResult += t.L_EXTENDEDPRICE * t.L_DISCOUNT
              }
              sumResult
            }
            printf("%.4f\n", result)
          }
        printf("==========\n")
        """
      else
        dsl"""
          runQuery {
            val filtered = $selection
            var sumResult = 0.0
            var sumResult2 = 0.0
            var sumResult3 = 0.0
            for(t <- filtered) {
              sumResult += t.L_EXTENDEDPRICE * t.L_DISCOUNT
              sumResult2 += t.L_DISCOUNT
              sumResult3 += t.L_EXTENDEDPRICE
            }
            val r1 = sumResult
            val r2 = sumResult2
            val r3 = sumResult3
            printf("%.4f, %.4f, %.4f\n", r1, r2, r3)
          }
        printf("==========\n")
        """
    }

    if (ONE_LOADER_FOR_ALL) {
      for (d <- datesGenerator) {
        generateQueryForStartDate(d)
      }
      context.unit()
    } else {
      generateQueryForStartDate(param)
    }
  }

  def executeQuery(query: String, schema: Schema): Unit = {
    System.out.println(s"\nRunning $query!")

    val (queryNumber, queryFunction) = query match {
      case "QSimple"           => (0, () => querySimple(1))
      case "fc"                => (25, () => filterCount(MICRO_RUNS))
      case "fms"               => (23, () => filterMapSum(MICRO_RUNS))
      case "ffms"              => (24, () => filterFilterMapSum(MICRO_RUNS))
      case "fm"                => (26, () => filterMap(MICRO_RUNS))
      case "fmt"               => (27, () => filterMapTake(MICRO_RUNS))
      case "Q6_functional"     => (6, () => query6(1))
      case "Q12_functional_p2" => (12, () => query12_p2(1))
    }

    val validatedSettings = settings.validate()

    val compiler = new LegoCompiler(context, validatedSettings, schema, "ch.epfl.data.dblab.experimentation.tpch.TPCHRunner") {
      def postfix: String = s"sf${settings.args(1)}_${super.outputFile}"
      override def outputFile =
        if (ONE_LOADER_FOR_ALL)
          postfix
        else
          s"${param}_${postfix}"
    }
    compiler.compile(queryFunction())
  }
}
