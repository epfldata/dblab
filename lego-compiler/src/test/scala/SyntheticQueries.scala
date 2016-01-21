package ch.epfl.data
package dblab.legobase
package tpch

import compiler._
import schema._
import deep._
import prettyprinter._
import optimization._
import sc.pardis.optimization._
import sc.pardis.ir._
import sc.pardis.types.PardisTypeImplicits._
import sc.pardis.types._
import quasi._

/**
 * Generates synthetic queries produced using quasi quotes.
 *
 * Run it with this command:
 * test:run /mnt/ramdisk/tpch 8 Q6_functional +monad-lowering +monad-iterator +malloc-hoist -name-with-flag
 */
object SyntheticQueries extends TPCHRunner {

  var settings: Settings = _

  val ONE_LOADER_FOR_ALL = true
  val oneAgg = false
  // val oneAgg = true

  def datesGenerator: List[String] = {
    val years = 1992 to 1998
    // 1996
    val months = (1 to 12 by 2) map (x => if (x < 10) s"0$x" else x.toString)
    val dates = for (y <- years; m <- months) yield s"$y-$m-01"
    // val days = 1 to 30 map (x => if (x < 10) s"0$x" else x.toString)
    // val dates = for (d <- days) yield s"1996-12-$d"
    // val dates = List("1998-01-01")
    dates.toList
  }

  var param: String = _

  def main(args: Array[String]) {
    if (args.length < 3) {
      System.out.println("ERROR: Invalid number (" + args.length + ") of command line arguments!")
      System.exit(0)
    }
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

  implicit val context = new LegoBaseExp {}

  def query6(numRuns: Int): context.Rep[Unit] = {
    import dblab.legobase.queryengine.monad.Query
    import dblab.legobase.tpch.TPCHLoader._
    import dblab.legobase.queryengine.GenericEngine._
    val lineitemTable = dsl"""Query(loadLineitem())"""

    def generateQueryForStartDate(startDate: String): context.Rep[Unit] = {
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

    val (queryNumber, queryFunction) =
      (5, () => query6(1))

    val validatedSettings = settings.validate()

    val compiler = new LegoCompiler(context, validatedSettings, schema, "ch.epfl.data.dblab.legobase.tpch.TPCHRunner") {
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
