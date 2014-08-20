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

  def executeQuery(query: String): Unit = query match {
    case "Q1"   => query1()
    case "Q1_U" => query1_unoptimized()
    case "Q2"   => query2()
    case "Q3"   => query3()
    case "Q4"   => query4()
    case "Q5"   => query5()
    case "Q6"   => query6()
  }

  def query1_unoptimized() {
    val lq = new LiftedQueries()
    val block = lq.Q1_U

    // Lowering (e.g. case classes to records)
    val lowering = new LBLowering(lq.context, lq.context)
    val loweredBlock = lowering.lower(block)
    val parameterPromotion = new LBParameterPromotion(lq.context)
    val operatorlessBlock = parameterPromotion.optimize(loweredBlock)

    // DCE
    val dce = new DCE(lq.context)
    val dceBlock = dce.optimize(operatorlessBlock)

    // Convert Scala constructs to C
    val scalaToC = new ScalaConstructsToCTranformer(lq.context)
    val transformedBlock = scalaToC.transformBlock(dceBlock)
    val scalaToC2 = new ScalaCollectionsToGLibTransfomer(lq.context)
    val transformedBlock2 = scalaToC2.optimize(transformedBlock)

    val ir2Program = new { val IR = lq.context } with IRToProgram {}

    System.out.println(transformedBlock2)

    val finalProgram = ir2Program.createProgram(transformedBlock2)

    val LegoGenerator = new LegoCGenerator(2, true)
    LegoGenerator.apply(finalProgram)
  }

  def compileQuery(lq: LiftedQueries, block: pardis.ir.PardisBlock[Unit], number: Int, shallow: Boolean) {

    // println(block)
    // LegoGenerator.apply(block)

    val loweringContext = new LoweringLegoBase {}

    // val loweredBlock = lowering.transformProgram(block)
    val lowering = new LBLowering(lq.context, loweringContext)
    val loweredBlock = lowering.lower(block)
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
    val LegoGenerator = new LegoScalaGenerator(number, shallow)
    LegoGenerator.apply(finalProgram)
  }

  def query1() {
    val lq = new LiftedQueries()
    val block = lq.Q1
    compileQuery(lq, block, 1, false)
  }

  def query2() {
    val lq = new LiftedQueries()
    val block = lq.Q2
    compileQuery(lq, block, 2, false)
  }

  def query3() {
    val lq = new LiftedQueries()
    val block = lq.Q3
    compileQuery(lq, block, 3, false)
  }

  def query4() {
    val lq = new LiftedQueries()
    val block = lq.Q4
    compileQuery(lq, block, 4, false)
  }

  def query5() {
    val lq = new LiftedQueries()
    val block = lq.Q5
    compileQuery(lq, block, 5, false)
  }

  def query6() {
    val lq = new LiftedQueries()
    val block = lq.Q6
    compileQuery(lq, block, 6, false)
  }

  // test1()
  // test2()
}
