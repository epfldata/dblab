package ch.epfl.data
package legobase
package compiler

import deep._
import prettyprinter._
import optimization._
import pardis.optimization._
import ch.epfl.data.pardis.ir._
import pardis.ir.pardisTypeImplicits._

object Main extends LegoRunner {

  def main(args: Array[String]) {
    if (args.length < 3) {
      System.out.println("ERROR: Invalid number (" + args.length + ") of command line arguments!")
      System.out.println("USAGE: run <data_folder> <scaling_factor_number> <list of queries to run> <copy>?")
      System.out.println("     : data_folder_name should contain folders named sf0.1 sf1 sf2 sf4 etc")
      System.exit(0)
    }
    Config.checkResults = false

    run(args)
    // if (args.length == 4 && args(3) == "copy") {
    //   import java.io.{ File, FileInputStream, FileOutputStream }
    //   val src = new File("generator-out/lala.scala")
    //   val dest = new File("legocompiler/src/test/scala/Generated.scala")
    //   new FileOutputStream(dest) getChannel () transferFrom (
    //     new FileInputStream(src) getChannel, 0, Long.MaxValue)
    // }
  }

  def executeQuery(query: String): Unit = {
    val lq = new LiftedQueries()
    query match {
      case "Q1"   => compileQuery(lq, lq.Q1, 1, false, false)
      case "Q1_C" => compileQuery(lq, lq.Q1, 1, false, true)
      case "Q2"   => compileQuery(lq, lq.Q2, 2, false, false)
      case "Q2_C" => compileQuery(lq, lq.Q2, 2, false, true)
      case "Q3"   => compileQuery(lq, lq.Q3, 3, false, false)
      case "Q3_C" => compileQuery(lq, lq.Q3, 3, false, true)
      case "Q4"   => compileQuery(lq, lq.Q4, 4, false, false)
      case "Q4_C" => compileQuery(lq, lq.Q4, 4, false, true)
      case "Q5"   => compileQuery(lq, lq.Q5, 5, false, false)
      case "Q5_C" => compileQuery(lq, lq.Q5, 5, false, true)
      case "Q6"   => compileQuery(lq, lq.Q6, 6, false, false)
      case "Q6_C" => compileQuery(lq, lq.Q6, 6, false, true)
    }
  }

  def compileQuery(lq: LiftedQueries, block: pardis.ir.PardisBlock[Unit], number: Int, shallow: Boolean, generateCCode: Boolean) {

    // Lowering (e.g. case classes to records)
    val lowering = new LBLowering(lq.context, lq.context)
    val loweredBlock = lowering.lower(block)
    val parameterPromotion = new LBParameterPromotion(lq.context)
    val operatorlessBlock = parameterPromotion.optimize(loweredBlock)

    // DCE
    val dce = new DCE(lq.context)
    val dceBlock = dce.optimize(operatorlessBlock)

    // Partial evaluation
    val partiallyEvaluator = new PartialyEvaluate(lq.context)
    val partiallyEvaluatedBlock = partiallyEvaluator.optimize(dceBlock)

    // Convert Scala constructs to C
    val finalBlock = {
      if (generateCCode) CTransformersPipeline(lq.context, dceBlock)
      else partiallyEvaluatedBlock
    }

    // Generate final program 
    val ir2Program = new { val IR = lq.context } with IRToProgram {}
    val finalProgram = ir2Program.createProgram(finalBlock)
    if (generateCCode) (new LegoCGenerator(shallow, "Q" + number)).apply(finalProgram)
    else (new LegoScalaGenerator(shallow, "Q" + number)).apply(finalProgram)
  }
}
