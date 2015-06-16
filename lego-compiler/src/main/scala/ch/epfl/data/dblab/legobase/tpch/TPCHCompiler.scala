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

/**
 * The starting point of the TPCH Query Compiler.
 */
object TPCHCompiler extends TPCHRunner {

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
      System.out.println(Settings.ALL_SETTINGS.collect({
        case opt: OptimizationSetting =>
          opt.fullFlagName + ": " + opt.description
      }).mkString(" " * 6, "\n" + " " * 6, ""))
      System.out.println("  Available options:")
      System.out.println(Settings.ALL_SETTINGS.collect({
        case opt: OptionSetting =>
          opt.fullFlagName + ": " + opt.description
      }).mkString(" " * 6, "\n" + " " * 6, ""))
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
   */
  def executeQuery(query: String, schema: Schema): Unit = {
    System.out.println(s"\nRunning $query!")

    val context = new LoweringLegoBase {}

    import context.unit
    import context.Queries._
    val (queryNumber, queryFunction) =
      query match {
        case "Q1"  => (1, () => Q1(unit(Config.numRuns)))
        case "Q2"  => (2, () => Q2(unit(Config.numRuns)))
        case "Q3"  => (3, () => Q3(unit(Config.numRuns)))
        case "Q4"  => (4, () => Q4(unit(Config.numRuns)))
        case "Q5"  => (5, () => Q5(unit(Config.numRuns)))
        case "Q6"  => (6, () => Q6(unit(Config.numRuns)))
        case "Q7"  => (7, () => Q7(unit(Config.numRuns)))
        case "Q8"  => (8, () => Q8(unit(Config.numRuns)))
        case "Q9"  => (9, () => Q9(unit(Config.numRuns)))
        case "Q10" => (10, () => Q10(unit(Config.numRuns)))
        case "Q11" => (11, () => Q11(unit(Config.numRuns)))
        case "Q12" => (12, () => Q12(unit(Config.numRuns)))
        case "Q13" => (13, () => Q13(unit(Config.numRuns)))
        case "Q14" => (14, () => Q14(unit(Config.numRuns)))
        case "Q15" => (15, () => Q15(unit(Config.numRuns)))
        case "Q16" => (16, () => Q16(unit(Config.numRuns)))
        case "Q17" => (17, () => Q17(unit(Config.numRuns)))
        case "Q18" => (18, () => Q18(unit(Config.numRuns)))
        case "Q19" => (19, () => Q19(unit(Config.numRuns)))
        case "Q20" => (20, () => Q20(unit(Config.numRuns)))
        case "Q21" => (21, () => Q21(unit(Config.numRuns)))
        case "Q22" => (22, () => Q22(unit(Config.numRuns)))
        case Q12SynthesizedExtract(targetCode, numFields) => {
          (12, () => context.Q12Synthesized(unit(Config.numRuns), numFields))
        }
      }

    settings.optimalArgsHandler = () => {
      val prop_ = new java.util.Properties
      val propName = "config/optimal.properties"
      try {
        prop_.load(new java.io.FileInputStream(propName))
      } catch {
        case _: Throwable => System.err.println(s"Config file `$propName` does not exist!")
      }
      val argsString = prop_.getProperty(s"tpch.Q$queryNumber")
      if (argsString == null) {
        throw new Exception(s"${OptimalSetting.flagName} cannot be used for query $queryNumber, because there is no optimal combiniation defined for it=.")
      }
      prop_.getProperty(s"tpch.Q$queryNumber").split(" ").toList
    }
    val validatedSettings = settings.validate()

    val compiler = new LegoCompiler(context, validatedSettings, schema, "ch.epfl.data.dblab.legobase.tpch.TPCHRunner")
    compiler.compile(queryFunction())
  }
}
