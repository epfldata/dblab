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
 * test:run /mnt/ramdisk/tpch fusion_tpch
 * test:run /mnt/ramdisk/tpch fusion_tpch_inter
 * test:run /mnt/ramdisk/tpch mem_cons_tpch
 * test:run /mnt/ramdisk/tpch vary_sel [MICRO_QUERY]
 * test:run /mnt/ramdisk/tpch vary_sel_papi [MICRO_QUERY]
 *    // don't forget to add `papi --libs glib-2.0 papi` flags in front of `pkg-config`.
 * test:run /mnt/ramdisk/tpch fusion_micro_cstore
 */
object SyntheticQueries extends TPCHRunner {

  var settings: Settings = _

  val ONE_LOADER_FOR_ALL = false
  val oneAgg = false
  // val oneAgg = true
  var tpchBenchmark = false

  def datesGenerator: List[String] = {
    val years = 1992 to 1998
    // List(1998)
    val months = (4 to 12 by 6) map (x => if (x < 10) s"0$x" else x.toString)
    val dates = for (y <- years; m <- months) yield s"$y-$m-01"
    // val days = 1 to 30 map (x => if (x < 10) s"0$x" else x.toString)
    // val dates = for (d <- days) yield s"1996-12-$d"
    // val dates = for (d <- days) yield s"1998-11-$d"
    // val dates = for (y <- List(4, 5, 6, 7, 8); m <- if (y == 4) List(7) else List(1, 7)) yield s"199$y-0$m-01"
    // val dates = List("1998-11-01")
    dates.toList
  }

  def joinDate: String = "1998-11-01"

  def singleDate: String = "1995-12-01"

  var param: String = _

  def process(args: List[String]): Unit = process(args.toArray)

  def process(args: Array[String]): Unit = {
    newContext()
    Config.checkResults = false
    settings = new Settings(args.toList)
    settings.init()

    // if (ONE_LOADER_FOR_ALL) {
    //   run(args)
    // } else {
    //   for (d <- datesGenerator) {
    //     param = d
    //     run(args)
    //   }
    // }
    run(args)
  }

  val MICRO_RUNS = 10
  val MICRO_JOIN_RUNS = 5
  val TPCH_DEFAULT_RUNS = 1
  var tpchRuns = TPCH_DEFAULT_RUNS

  val FIXED_OPTIMIZATION_FLAGS = List("+monad-lowering", "-name-with-flag",
    "+force-compliant" /*, "+relation-column" */ )
  val VARIABLE_OPTIMIZATION_FLAGS = List(List("+monad-cps"), List("+monad-iterator"), List("+monad-stream"),
    List("+monad-stream", "+monad-stream-church"))

  def fusionBenchmarkProcess(f: List[String] => Unit, variableFlags: List[List[String]]): Unit = {
    for (flags <- variableFlags) {
      f(flags ++ FIXED_OPTIMIZATION_FLAGS)
    }
  }

  def fusionBenchmarkProcess(f: List[String] => Unit): Unit = {
    fusionBenchmarkProcess(f, VARIABLE_OPTIMIZATION_FLAGS)
  }

  def fusionTPCHBenchmark(args: Array[String], additionalFlags: List[String],
                          scenarios: List[List[String]] = VARIABLE_OPTIMIZATION_FLAGS, queryNumbers: List[Int] = (1 to 6).toList ++ List(9, 10, 12, 14, 19, 20)): Unit = {
    tpchBenchmark = true
    val folder = args(0)
    val SFs = List(8)
    // val queryNumbers = (1 to 6).toList ++ (9 to 12).toList ++ List(14)
    val queries = queryNumbers.map(x => s"Q${x}_functional")
    fusionBenchmarkProcess({ flags =>
      for (sf <- SFs) {
        for (q <- queries) {
          process(folder :: sf.toString :: q :: flags ++ additionalFlags)
        }
      }
    }, scenarios)
  }

  def fusionTPCHBenchmarkCompilationTime(args: Array[String], additionalFlags: List[String],
                                         scenarios: List[List[String]] = VARIABLE_OPTIMIZATION_FLAGS, queryNumbers: List[Int] = (1 to 6).toList ++ List(9, 10, 12, 14, 19, 20)): Unit = {
    tpchBenchmark = true
    val folder = args(0)
    val SFs = List(8)
    // val queryNumbers = (1 to 6).toList ++ (9 to 12).toList ++ List(14)
    val queries = queryNumbers.map(x => s"Q${x}_functional")
    val warmUpQueries = List(2, 9).map(x => s"Q${x}_functional")
    fusionBenchmarkProcess({ flags =>
      for (sf <- SFs) {
        def compileQuery(q: String): Unit = {
          utils.Utilities.time({
            TPCHCompiler.turnOffConsoleOutput {
              process(folder :: sf.toString :: q :: flags ++ additionalFlags)
            }
          }, s"Compilation of $q")
        }
        for (i <- 0 until 25) {
          for (wq <- warmUpQueries) {
            compileQuery(wq)
          }
        }
        for (q <- queries) {
          compileQuery(q)
        }
      }
    }, List(scenarios.last))
  }

  def fusionTPCHInterpretBenchmark(args: Array[String], additionalFlags: List[String]): Unit = {
    tpchBenchmark = true
    val folder = args(0)
    val SFs = List(8)
    // val queryNumbers = (1 to 6).toList ++ (9 to 12).toList ++ List(14)
    val queryNumbers = (1 to 6).toList ++ List(9, 10, 12, 14, 19, 20)
    val queries = queryNumbers.map(x => s"Q${x}")
    for (sf <- SFs) {
      for (q <- queries) {
        process(folder :: sf.toString :: q :: "-name-with-flag" :: "-no-spec-engine" :: additionalFlags)
      }
    }
  }

  def varySelBenchmark(args: Array[String], additionalFlags: List[String]): Unit = {
    val folder = args(0)
    val SFs = List(8)
    val query = args(2)
    fusionBenchmarkProcess { flags =>
      for (sf <- SFs) {
        for (d <- datesGenerator) {
          param = d
          process(folder :: sf.toString :: query :: (additionalFlags ++ flags))
        }
      }
    }
  }

  def fusionMicroBenchmark(args: Array[String], additionalFlags: List[String]): Unit = {
    val folder = args(0)

    val SFs1 = List(8)
    val queries1 = List("fc", "fs", "ffs")
    val SFs2 = List(8)
    val queries2 = List("fst", "fmt", "ts")
    val SFs3 = List(8)
    val queries3 = List("fmjs", "fhjs", "fhsjs")
    fusionBenchmarkProcess { flags =>
      param = singleDate
      for (sf <- SFs1) {
        for (q <- queries1) {
          process(folder :: sf.toString :: q :: (additionalFlags ++ flags))
        }
      }

      // param = "1998-10-01"
      param = singleDate
      for (sf <- SFs2) {
        for (q <- queries2) {
          process(folder :: sf.toString :: q :: (additionalFlags ++ flags))
        }
      }

      param = joinDate
      for (sf <- SFs3) {
        for (q <- queries3) {
          process(folder :: sf.toString :: q :: (additionalFlags ++ flags))
        }
      }
    }
  }

  def fusionMicroBenchmarkCStore2(args: Array[String], additionalFlags: List[String]): Unit = {
    val folder = args(0)

    val SFs = List(8)
    val queries = List("fc", "fs1", "fs", "fs3")
    fusionBenchmarkProcess { flags =>
      param = singleDate
      for (sf <- SFs) {
        for (q <- queries) {
          process(folder :: sf.toString :: q :: (additionalFlags ++ flags))
        }
      }
    }
  }

  def main(args: Array[String]): Unit = {
    if (args.length == 2 && args(1) == "fusion_micro") {
      fusionMicroBenchmark(args, Nil)
    } else if (args.length == 2 && args(1) == "fusion_micro_cstore") {
      fusionMicroBenchmark(args, List("+relation-column"))
    } else if (args.length == 2 && args(1) == "fusion_tpch") {
      fusionTPCHBenchmark(args, Nil)
      fusionTPCHBenchmark(args, Nil, List(
        List("+monad-stream", "+monad-no-escape"),
        List("+monad-iterator", "+monad-iterator-bad-filter")),
        List(14, 19))
    } else if (args.length == 2 && args(1) == "fusion_tpch_inter") {
      fusionTPCHInterpretBenchmark(args, Nil)
    } else if (args.length == 2 && args(1) == "fusion_tpch_ctime") {
      fusionTPCHBenchmarkCompilationTime(args, Nil)
    } else if (args.length == 2 && args(1) == "mem_cons_tpch") {
      tpchRuns = 1
      fusionTPCHBenchmark(args, List("-malloc-profile"), List(
        List("+monad-stream", "+monad-no-escape"),
        List("+monad-stream", "+monad-church")),
        List(14, 19))
    } else if (args.length == 3 && args(1) == "vary_sel") {
      varySelBenchmark(args, Nil)
    } else if (args.length == 3 && args(1) == "vary_sel_papi") {
      varySelBenchmark(args, List("-papi-profile"))
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
    val startDate = param
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

  def filterSum(numRuns: Int): Rep[Unit] = {
    import dblab.queryengine.monad.Query
    import dblab.experimentation.tpch.TPCHLoader._
    import dblab.queryengine.GenericEngine._
    val loadedLineitemTable = dsl"loadLineitem()"
    def lineitemTable = dsl"""Query($loadedLineitemTable)"""
    val startDate = param
    for (i <- 0 until numRuns) {
      dsl"""
        runQuery {
          val constantDate1: Int = parseDate($startDate)
          val result = $lineitemTable.filter(_.L_SHIPDATE >= constantDate1).
            foldLeft(0.0)((acc, cur) => acc + cur.L_EXTENDEDPRICE * cur.L_DISCOUNT)
          printf("%.4f\n", result)
        }
      """
    }
    dsl"()"
  }

  def filterSum1(numRuns: Int): Rep[Unit] = {
    import dblab.queryengine.monad.Query
    import dblab.experimentation.tpch.TPCHLoader._
    import dblab.queryengine.GenericEngine._
    val loadedLineitemTable = dsl"loadLineitem()"
    def lineitemTable = dsl"""Query($loadedLineitemTable)"""
    val startDate = param
    for (i <- 0 until numRuns) {
      dsl"""
        runQuery {
          val constantDate1: Int = parseDate($startDate)
          val result = $lineitemTable.filter(_.L_SHIPDATE >= constantDate1).
            foldLeft(0.0)((acc, cur) => acc + cur.L_EXTENDEDPRICE)
          printf("%.4f\n", result)
        }
      """
    }
    dsl"()"
  }

  def filterSum3(numRuns: Int): Rep[Unit] = {
    import dblab.queryengine.monad.Query
    import dblab.experimentation.tpch.TPCHLoader._
    import dblab.queryengine.GenericEngine._
    val loadedLineitemTable = dsl"loadLineitem()"
    def lineitemTable = dsl"""Query($loadedLineitemTable)"""
    val startDate = param
    for (i <- 0 until numRuns) {
      dsl"""
        runQuery {
          val constantDate1: Int = parseDate($startDate)
          val result = $lineitemTable.filter(_.L_SHIPDATE >= constantDate1).
            foldLeft(0.0)((acc, cur) => acc + cur.L_EXTENDEDPRICE * cur.L_DISCOUNT * cur.L_TAX)
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
    val startDate = "1998-11-01"
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
    val startDate = param
    for (i <- 0 until numRuns) {
      dsl"""
        runQuery {
          val constantDate1: Int = parseDate($startDate)
          $lineitemTable.filter(_.L_SHIPDATE >= constantDate1).
            map(t => t.L_EXTENDEDPRICE * t.L_DISCOUNT).take(1000).printRows(t =>
              printf("%.4f\n", t)
            , -1)
        }
      """
    }
    dsl"()"
  }

  def takeSum(numRuns: Int): Rep[Unit] = {
    import dblab.queryengine.monad.Query
    import dblab.experimentation.tpch.TPCHLoader._
    import dblab.queryengine.GenericEngine._
    val loadedLineitemTable = dsl"loadLineitem()"
    def lineitemTable = dsl"""Query($loadedLineitemTable)"""
    val startDate = param
    for (i <- 0 until numRuns) {
      dsl"""
        runQuery {
          val constantDate1: Int = parseDate($startDate)
          val result = $lineitemTable.take(1000).
            foldLeft(0.0)((acc, cur) => acc + cur.L_EXTENDEDPRICE * cur.L_DISCOUNT)
          printf("%.4f\n", result)
        }
      """
    }
    dsl"()"
  }

  def filterTakeSum(numRuns: Int): Rep[Unit] = {
    import dblab.queryengine.monad.Query
    import dblab.experimentation.tpch.TPCHLoader._
    import dblab.queryengine.GenericEngine._
    val loadedLineitemTable = dsl"loadLineitem()"
    def lineitemTable = dsl"""Query($loadedLineitemTable)"""
    val startDate = param
    for (i <- 0 until numRuns) {
      dsl"""
        runQuery {
          val constantDate1: Int = parseDate($startDate)
          val result = $lineitemTable.filter(_.L_SHIPDATE >= constantDate1).take(1000).
            foldLeft(0.0)((acc, cur) => acc + cur.L_EXTENDEDPRICE * cur.L_DISCOUNT)
          printf("%.4f\n", result)
        }
      """
    }
    dsl"()"
  }

  def filterSortByTake(numRuns: Int): Rep[Unit] = {
    import dblab.queryengine.monad.Query
    import dblab.experimentation.tpch.TPCHLoader._
    import dblab.queryengine.GenericEngine._
    val loadedLineitemTable = dsl"loadLineitem()"
    def lineitemTable = dsl"""Query($loadedLineitemTable)"""
    val startDate = param
    for (i <- 0 until numRuns) {
      dsl"""
        runQuery {
          val constantDate1: Int = parseDate($startDate)
          $lineitemTable.filter(_.L_SHIPDATE >= constantDate1).
            sortBy(_.L_ORDERKEY).take(1000).printRows(t =>
              printf("%.4f\n", t.L_EXTENDEDPRICE)
            , -1)
        }
      """
    }
    dsl"()"
  }

  def filterFilterSum(numRuns: Int): Rep[Unit] = {
    import dblab.queryengine.monad.Query
    import dblab.experimentation.tpch.TPCHLoader._
    import dblab.queryengine.GenericEngine._
    val loadedLineitemTable = dsl"loadLineitem()"
    def lineitemTable = dsl"""Query($loadedLineitemTable)"""
    val startDate = param
    val endDate = "1997-01-01"
    for (i <- 0 until numRuns) {
      dsl"""
        runQuery {
          val constantDate1: Int = parseDate($startDate)
          val constantDate2: Int = parseDate($endDate)
          val result = $lineitemTable.filter(_.L_SHIPDATE >= constantDate1).
            filter(_.L_SHIPDATE < constantDate2).
            foldLeft(0.0)((acc, cur) => acc + cur.L_EXTENDEDPRICE * cur.L_DISCOUNT)
          printf("%.4f\n", result)
        }
      """
    }
    dsl"()"
  }

  def filter4Sum(numRuns: Int): Rep[Unit] = {
    import dblab.queryengine.monad.Query
    import dblab.experimentation.tpch.TPCHLoader._
    import dblab.queryengine.GenericEngine._
    val loadedLineitemTable = dsl"loadLineitem()"
    def lineitemTable = dsl"""Query($loadedLineitemTable)"""
    val startDate = param
    val endDate = "1997-01-01"
    for (i <- 0 until numRuns) {
      dsl"""
        runQuery {
          val constantDate1: Int = parseDate($startDate)
          val constantDate2: Int = parseDate($endDate)
          val constantDate3: Int = constantDate1
          val constantDate4: Int = constantDate2
          val MAIL = parseString("MAIL")
          val result = $lineitemTable.
            filter(_.L_SHIPDATE >= constantDate1).
            filter(_.L_SHIPDATE < constantDate2).
            filter(_.L_SHIPMODE === MAIL).
            // filter(_.L_COMMITDATE >= constantDate3).
            // filter(_.L_COMMITDATE < constantDate4).
            // filter(_.L_RECEIPTDATE >= constantDate3).
            // filter(_.L_RECEIPTDATE < constantDate4).
            foldLeft(0.0)((acc, cur) => acc + cur.L_EXTENDEDPRICE * cur.L_DISCOUNT)
            // count
          printf("%.4f\n", result)
        }
      """
    }
    dsl"()"
  }

  def filterMergeJoinSum(numRuns: Int): Rep[Unit] = {
    import dblab.queryengine.monad.Query
    import dblab.experimentation.tpch.TPCHLoader._
    import dblab.queryengine.GenericEngine._
    val loadedLineitemTable = dsl"loadLineitem()"
    def lineitemTable = dsl"""Query($loadedLineitemTable)"""
    val loadedOrdersTable = dsl"loadOrders()"
    def ordersTable = dsl"Query($loadedOrdersTable)"
    val startDate = param
    val hasFilter = true
    for (i <- 0 until numRuns) {
      def selection1 = if (hasFilter)
        dsl"""
            val constantDate1 = parseDate($startDate)
            val so2 = $ordersTable.filter(x => x.O_ORDERDATE >= constantDate1)
            so2
          """
      else
        ordersTable
      def selection2 = if (hasFilter)
        dsl"""
            val constantDate1 = parseDate($startDate)
            val so2 = $lineitemTable.filter(x => x.L_SHIPDATE >= constantDate1)
            so2
          """
      else
        lineitemTable
      dsl"""
        runQuery({
          val jo = $selection2.mergeJoin($selection1)((x, y) => x.L_ORDERKEY - y.O_ORDERKEY)((x, y) => x.L_ORDERKEY == y.O_ORDERKEY)
          val result = jo.foldLeft(0.0)((acc, cur) => cur.O_TOTALPRICE[Double] + acc)
          printf("%.4f\n", result)
        })
      """
    }
    dsl"()"
  }

  def filterHashJoinSum(numRuns: Int): Rep[Unit] = {
    import dblab.queryengine.monad.Query
    import dblab.experimentation.tpch.TPCHLoader._
    import dblab.queryengine.GenericEngine._
    val loadedLineitemTable = dsl"loadLineitem()"
    def lineitemTable = dsl"""Query($loadedLineitemTable)"""
    val loadedOrdersTable = dsl"loadOrders()"
    def ordersTable = dsl"Query($loadedOrdersTable)"
    val startDate = param
    val hasFilter = true
    for (i <- 0 until numRuns) {
      def selection1 = if (hasFilter)
        dsl"""
            val constantDate1 = parseDate($startDate)
            val so2 = $ordersTable.filter(x => x.O_ORDERDATE >= constantDate1)
            so2
          """
      else
        ordersTable
      def selection2 = if (hasFilter)
        dsl"""
            val constantDate1 = parseDate($startDate)
            val so2 = $lineitemTable.filter(x => x.L_SHIPDATE >= constantDate1)
            so2
          """
      else
        lineitemTable
      dsl"""
        runQuery({
          val jo = $selection2.hashJoin($selection1)(x => x.L_ORDERKEY)(x => x.O_ORDERKEY)((x, y) => x.L_ORDERKEY == y.O_ORDERKEY)
          val result = jo.foldLeft(0.0)((acc, cur) => cur.O_TOTALPRICE[Double] + acc)
          printf("%.4f\n", result)
        })
      """
    }
    dsl"()"
  }

  def filterHashSemiJoinSum(numRuns: Int): Rep[Unit] = {
    import dblab.queryengine.monad.Query
    import dblab.experimentation.tpch.TPCHLoader._
    import dblab.queryengine.GenericEngine._
    val loadedLineitemTable = dsl"loadLineitem()"
    def lineitemTable = dsl"""Query($loadedLineitemTable)"""
    val loadedOrdersTable = dsl"loadOrders()"
    def ordersTable = dsl"Query($loadedOrdersTable)"
    val startDate = param
    val hasFilter = true
    for (i <- 0 until numRuns) {
      def selection1 = if (hasFilter)
        dsl"""
            val constantDate1 = parseDate($startDate)
            val so2 = $ordersTable.filter(x => x.O_ORDERDATE >= constantDate1)
            so2
          """
      else
        ordersTable
      def selection2 = if (hasFilter)
        dsl"""
            val constantDate1 = parseDate($startDate)
            val so2 = $lineitemTable.filter(x => x.L_SHIPDATE >= constantDate1)
            so2
          """
      else
        lineitemTable
      dsl"""
        runQuery({
          val jo = $selection1.leftHashSemiJoin($selection2)(x => x.O_ORDERKEY)(x => x.L_ORDERKEY)((x, y) => x.O_ORDERKEY == y.L_ORDERKEY)
          val result = jo.foldLeft(0.0)((acc, cur) => cur.O_TOTALPRICE + acc)
          printf("%.4f\n", result)
        })
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
    val ctx = context
    import ctx.unit
    import ctx.Queries._

    if (param == null) {
      param = singleDate
      System.out.println(s"\nParameter was null. Set to $param by default!")
    }

    val (queryNumber, queryFunction) = query match {
      case "QSimple"        => (0, () => querySimple(1))
      case "fc"             => (25, () => filterCount(MICRO_RUNS))
      case "fs"             => (23, () => filterSum(MICRO_RUNS))
      case "fs1"            => (32, () => filterSum1(MICRO_RUNS))
      case "fs3"            => (33, () => filterSum3(MICRO_RUNS))
      case "ffs"            => (24, () => filterFilterSum(MICRO_RUNS))
      case "fm"             => (26, () => filterMap(MICRO_RUNS))
      case "fmt"            => (27, () => filterMapTake(MICRO_RUNS))
      case "fst"            => (32, () => filterTakeSum(MICRO_RUNS))
      case "ts"             => (33, () => takeSum(MICRO_RUNS))
      case "fot"            => (30, () => filterSortByTake(MICRO_RUNS))
      case "fmjs"           => (28, () => filterMergeJoinSum(MICRO_JOIN_RUNS))
      case "fhjs"           => (29, () => filterHashJoinSum(MICRO_JOIN_RUNS))
      case "fhsjs"          => (31, () => filterHashSemiJoinSum(MICRO_JOIN_RUNS))
      case "f4s"            => (34, () => filter4Sum(MICRO_RUNS))
      case "Q1_functional"  => (1, () => Q1_functional_p2(unit(tpchRuns)))
      case "Q2_functional"  => (2, () => Q2_functional(unit(tpchRuns)))
      case "Q3_functional"  => (3, () => Q3_functional(unit(tpchRuns)))
      case "Q4_functional"  => (4, () => Q4_functional(unit(tpchRuns)))
      case "Q5_functional"  => (5, () => Q5_functional(unit(tpchRuns)))
      case "Q6_functional"  => (6, () => Q6_functional(unit(tpchRuns)))
      case "Q9_functional"  => (9, () => Q9_functional(unit(tpchRuns)))
      case "Q10_functional" => (10, () => Q10_functional(unit(tpchRuns)))
      case "Q11_functional" => (11, () => Q11_functional(unit(tpchRuns)))
      case "Q12_functional" => (12, () => Q12_functional_p2(unit(tpchRuns)))
      case "Q14_functional" => (14, () => Q14_functional(unit(tpchRuns)))
      case "Q19_functional" => (19, () => Q19_functional(unit(tpchRuns)))
      case "Q20_functional" => (20, () => Q20_functional(unit(tpchRuns)))
      case "Q1"             => (1, () => Q1(unit(tpchRuns)))
      case "Q2"             => (2, () => Q2(unit(tpchRuns)))
      case "Q3"             => (3, () => Q3(unit(tpchRuns)))
      case "Q4"             => (4, () => Q4(unit(tpchRuns)))
      case "Q5"             => (5, () => Q5(unit(tpchRuns)))
      case "Q6"             => (6, () => Q6(unit(tpchRuns)))
      case "Q9"             => (9, () => Q9_p2(unit(tpchRuns)))
      case "Q10"            => (10, () => Q10_p2(unit(tpchRuns)))
      case "Q11"            => (11, () => Q11(unit(tpchRuns)))
      case "Q12"            => (12, () => Q12_p2(unit(tpchRuns)))
      case "Q14"            => (14, () => Q14(unit(tpchRuns)))
      case "Q19"            => (19, () => Q19_p2(unit(tpchRuns)))
      case "Q20"            => (20, () => Q20(unit(tpchRuns)))
      case "Q12_synthetic"  => (12, () => query12_p2(1))
    }

    val validatedSettings = settings.validate()

    val compiler = new LegoCompiler(context, validatedSettings, schema, "ch.epfl.data.dblab.experimentation.tpch.TPCHRunner") {
      def postfix: String = s"sf${settings.args(1)}_${super.outputFile}"
      override def outputFile =
        if (ONE_LOADER_FOR_ALL || tpchBenchmark)
          postfix
        else
          s"${param}_${postfix}"
    }
    compiler.compile(queryFunction())
  }
}
