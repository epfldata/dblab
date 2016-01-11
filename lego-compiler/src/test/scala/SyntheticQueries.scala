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

  var param: String = _

  def main(args: Array[String]) {
    if (args.length < 3) {
      System.out.println("ERROR: Invalid number (" + args.length + ") of command line arguments!")
      System.exit(0)
    }
    Config.checkResults = false
    settings = new Settings(args.toList)
    // val years = 1992 to 1996
    // val months = 1 to 12 map (x => if (x < 10) s"0$x" else x.toString)
    // val dates = for (y <- years; m <- months) yield s"$y-$m-01"
    // val days = 1 to 30 map (x => if (x < 10) s"0$x" else x.toString)
    // val dates = for (d <- days) yield s"1996-12-$d"
    // val dates = List("1996-01-01")
    // for (d <- dates) {
    //   param = d
    run(args)
    // }

  }

  implicit val context = new LegoBaseExp {}

  def query6(numRuns: Int): context.Rep[Unit] = {
    import dblab.legobase.queryengine.monad.Query
    import dblab.legobase.tpch.TPCHLoader._
    import dblab.legobase.queryengine.GenericEngine._
    val lineitemTable = dsl"""Query(loadLineitem())"""
    val years = 1992 to 1996
    val months = 1 to 12 map (x => if (x < 10) s"0$x" else x.toString)
    val dates = for (y <- years; m <- months) yield s"$y-$m-01"
    for (startDate <- dates) {
      // val startDate = param
      def selection = dsl"""
      val constantDate1: Int = parseDate($startDate)
      val constantDate2: Int = parseDate("1997-01-01")
      $lineitemTable.filter(x => x.L_SHIPDATE >= constantDate1 && (x.L_SHIPDATE < constantDate2 && (x.L_DISCOUNT >= 0.08 && (x.L_DISCOUNT <= 0.1 && (x.L_QUANTITY < 24)))))
      """
      def selectivity = dsl"""
        val filtered = $selection
        val original = $lineitemTable
        filtered.count.asInstanceOf[Double] / original.count
      """
      dsl"""
      printf($startDate)
      printf("\nselectivity: %f\n", $selectivity)
      printf("==========\n")
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
    }
    context.unit(())
  }

  def executeQuery(query: String, schema: Schema): Unit = {
    System.out.println(s"\nRunning $query!")

    val (queryNumber, queryFunction) =
      (5, () => query6(1))

    val validatedSettings = settings.validate()

    val compiler = new LegoCompiler(context, validatedSettings, schema, "ch.epfl.data.dblab.legobase.tpch.TPCHRunner") {
      // override def outputFile = s"${param}_${super.outputFile}"
    }
    compiler.compile(queryFunction())
  }
}
