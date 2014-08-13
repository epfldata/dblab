package ch.epfl.data
package legobase
package compiler

import deep._
import prettyprinter._
import optimization._
import pardis.optimization._
import ch.epfl.data.pardis.ir._

object Main extends LegoRunner {

  def main(args: Array[String]) {
    if (args.length < 3) {
      System.out.println("ERROR: Invalid number (" + args.length + ") of command line arguments!")
      System.out.println("USAGE: run <data_folder> <scaling_factor_number> <list of queries to run>")
      System.out.println("     : data_folder_name should contain folders named sf0.1 sf1 sf2 sf4 etc")
      System.exit(0)
    }
    Config.checkResults = false

    run(args)
  }

  def executeQuery(query: String): Unit = query match {
    case "Q1"   => query1()
    case "Q1_U" => query1_unoptimized()
    case "Q2"   => query2()
  }

  def query1_unoptimized() {
    val lq = new LiftedQueries()
    val block = lq.Q1

    // Lowering (e.g. case classes to records)
    val lowering = new LBLowering {
      val from = lq.context
      val to = lq.context
    }
    val loweredBlock = lowering.transformProgram(block)
    /*val parameterPromotion = new LBParameterPromotion(lq.context)
    val operatorlessBlock = parameterPromotion.optimize(loweredBlock)*/

    // Convert Scala constructs to C
    val transformedBlock = (new t3(lq.context)).transformBlock(loweredBlock)

    val ir2Program = new { val IR = lq.context } with IRToProgram {}

    System.out.println(transformedBlock)

    val finalProgram = ir2Program.createProgram(transformedBlock)

    val LegoGenerator = new LegoCGenerator(2, true)
    LegoGenerator.apply(finalProgram)
  }

  def query1() {

    val lq = new LiftedQueries()
    val block = lq.Q1

    // println(block)
    // LegoGenerator.apply(block)

    val loweringContext = new LoweringLegoBase {}

    // it's written like this because of early definition: http://stackoverflow.com/questions/4712468/in-scala-what-is-an-early-initializer
    val lowering = new LBLowering {
      val from = lq.context
      val to = loweringContext
    }

    val loweredBlock = lowering.transformProgram(block)
    // val loweredBlock = block

    val parameterPromotion = new LBParameterPromotion(loweringContext)

    val operatorlessBlock = parameterPromotion.optimize(loweredBlock)
    // val operatorlessBlock = loweredBlock

    val dce = new DCE(loweringContext)

    val dceBlock = dce.optimize(operatorlessBlock)
    // val dceBlock = operatorlessBlock

    val partialyEvaluator = new PartialyEvaluate(loweringContext)

    val partialyEvaluated = partialyEvaluator.optimize(dceBlock)
    // val partialyEvaluated = dceBlock

    val ir2Program = new { val IR = loweringContext } with IRToProgram {
    }

    val finalProgram = ir2Program.createProgram(partialyEvaluated)

    println(finalProgram)
    val LegoGenerator = new LegoScalaGenerator(1, false)
    LegoGenerator.apply(finalProgram)
  }

  def query2() {
    val lq = new LiftedQueries()
    val block = lq.Q2

    val ir2Program = new { val IR = lq.context } with IRToProgram {
    }

    val finalProgram = ir2Program.createProgram(block)
    println(finalProgram)
    val LegoGenerator = new LegoScalaGenerator(2, true)
    LegoGenerator.apply(finalProgram)
  }

  // test1()
  // test2()
}
