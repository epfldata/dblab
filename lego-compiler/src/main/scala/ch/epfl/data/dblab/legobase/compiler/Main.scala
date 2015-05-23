package ch.epfl.data
package dblab.legobase
package compiler

import schema._
import deep._
import prettyprinter._
import optimization._
import sc.pardis.optimization._
import sc.pardis.ir._
import sc.pardis.types.PardisTypeImplicits._
import sc.pardis.types._

/**
 * The starting point of the LegoBase compiler.
 */
object Main extends LegoRunner {

  object Q12SynthesizedExtract {
    val Pat = "Q12S(_\\w)?_(\\d*)".r
    def unapply(str: String): Option[(Boolean, Int)] = str match {
      case Pat(target, numFieldsStr) =>
        val isCCode = if (target == null) false else true
        val numFields = numFieldsStr.toInt
        Some(isCCode -> numFields)
      case _ => None
    }
  }

  var settings: Settings = _

  def main(args: Array[String]) {
    if (args.length < 3) {
      import Settings._
      System.out.println("ERROR: Invalid number (" + args.length + ") of command line arguments!")
      System.out.println("USAGE: run <data_folder> <scaling_factor_number> <list of queries to run> <copy>? <+optimizations> <-options>")
      System.out.println("     : data_folder_name should contain folders named sf0.1 sf1 sf2 sf4 etc")
      System.out.println("  Available optimizations:")
      System.out.println(Settings.ALL_SETTINGS.collect({ case opt: OptimizationSetting => opt.fullFlagName + ": " + opt.description }).mkString(" " * 6, "\n" + " " * 6, ""))
      System.out.println("  Available options:")
      System.out.println(Settings.ALL_SETTINGS.collect({ case opt: OptionSetting => opt.fullFlagName + ": " + opt.description }).mkString(" " * 6, "\n" + " " * 6, ""))
      // System.out.println("""  Synthesized queries:
      //       Q12S[_C]_N: N is the number of fields of the Lineitem table which should be used.
      // """)
      System.exit(0)
    }
    Config.checkResults = false
    settings = new Settings(args.toList)
    run(args)
  }

  /**
   * Generates a program for the given TPCH query with the given scaling factor.
   *
   * First, the target language and the TPCH query number is extracted from the
   * input string. Second, the corresponding query generator in the polymorphic embedding
   * traits is specified and is put into a thunk. Then, the setting arugments are validated
   * and parsed using [[Settings]] class. Finally, a [[LegoCompiler]] object is created and
   * its `compile` method is invoked by passing an appropriate query generator.
   *
   * @param query the input TPCH query together with the target language
   * @param scalingFactor the input scaling factor
   */
  def executeQuery(query: String, scalingFactor: Double, schema: Schema): Unit = {
    System.out.println(s"Running $query!")

    val context = new LoweringLegoBase {}

    val cCode =
      true

    val scalaCode =
      false

    import context.unit
    import context.Queries._
    val (queryNumber, targetCode, queryFunction) =
      query match {
        case "Q1"    => (1, scalaCode, () => Q1(unit(Config.numRuns)))
        case "Q1_C"  => (1, cCode, () => Q1(unit(Config.numRuns)))
        case "Q2"    => (2, scalaCode, () => Q2(unit(Config.numRuns)))
        case "Q2_C"  => (2, cCode, () => Q2(unit(Config.numRuns)))
        case "Q3"    => (3, scalaCode, () => Q3(unit(Config.numRuns)))
        case "Q3_C"  => (3, cCode, () => Q3(unit(Config.numRuns)))
        case "Q4"    => (4, scalaCode, () => Q4(unit(Config.numRuns)))
        case "Q4_C"  => (4, cCode, () => Q4(unit(Config.numRuns)))
        case "Q5"    => (5, scalaCode, () => Q5(unit(Config.numRuns)))
        case "Q5_C"  => (5, cCode, () => Q5(unit(Config.numRuns)))
        case "Q6"    => (6, scalaCode, () => Q6(unit(Config.numRuns)))
        case "Q6_C"  => (6, cCode, () => Q6(unit(Config.numRuns)))
        case "Q7"    => (7, scalaCode, () => Q7(unit(Config.numRuns)))
        case "Q7_C"  => (7, cCode, () => Q7(unit(Config.numRuns)))
        case "Q8"    => (8, scalaCode, () => Q8(unit(Config.numRuns)))
        case "Q8_C"  => (8, cCode, () => Q8(unit(Config.numRuns)))
        case "Q9"    => (9, scalaCode, () => Q9(unit(Config.numRuns)))
        case "Q9_C"  => (9, cCode, () => Q9(unit(Config.numRuns)))
        case "Q10"   => (10, scalaCode, () => Q10(unit(Config.numRuns)))
        case "Q10_C" => (10, cCode, () => Q10(unit(Config.numRuns)))
        case "Q11"   => (11, scalaCode, () => Q11(unit(Config.numRuns)))
        case "Q11_C" => (11, cCode, () => Q11(unit(Config.numRuns)))
        case "Q12"   => (12, scalaCode, () => Q12(unit(Config.numRuns)))
        case "Q12_C" => (12, cCode, () => Q12(unit(Config.numRuns)))
        case "Q13"   => (13, scalaCode, () => Q13(unit(Config.numRuns)))
        case "Q13_C" => (13, cCode, () => Q13(unit(Config.numRuns)))
        case "Q14"   => (14, scalaCode, () => Q14(unit(Config.numRuns)))
        case "Q14_C" => (14, cCode, () => Q14(unit(Config.numRuns)))
        case "Q15"   => (15, scalaCode, () => Q15(unit(Config.numRuns)))
        case "Q15_C" => (15, cCode, () => Q15(unit(Config.numRuns)))
        case "Q16"   => (16, scalaCode, () => Q16(unit(Config.numRuns)))
        case "Q16_C" => (16, cCode, () => Q16(unit(Config.numRuns)))
        case "Q17"   => (17, scalaCode, () => Q17(unit(Config.numRuns)))
        case "Q17_C" => (17, cCode, () => Q17(unit(Config.numRuns)))
        case "Q18"   => (18, scalaCode, () => Q18(unit(Config.numRuns)))
        case "Q18_C" => (18, cCode, () => Q18(unit(Config.numRuns)))
        case "Q19"   => (19, scalaCode, () => Q19(unit(Config.numRuns)))
        case "Q19_C" => (19, cCode, () => Q19(unit(Config.numRuns)))
        case "Q20"   => (20, scalaCode, () => Q20(unit(Config.numRuns)))
        case "Q20_C" => (20, cCode, () => Q20(unit(Config.numRuns)))
        case "Q21"   => (21, scalaCode, () => Q21(unit(Config.numRuns)))
        case "Q21_C" => (21, cCode, () => Q21(unit(Config.numRuns)))
        case "Q22"   => (22, scalaCode, () => Q22(unit(Config.numRuns)))
        case "Q22_C" => (22, cCode, () => Q22(unit(Config.numRuns)))
        case Q12SynthesizedExtract(targetCode, numFields) => {
          (12, targetCode, () => context.Q12Synthesized(unit(Config.numRuns), numFields))
        }
      }

    settings.validate(targetCode, queryNumber)

    val compiler = new LegoCompiler(context, queryNumber, scalingFactor, targetCode, settings, schema)
    compiler.compile(queryFunction())
  }
}
