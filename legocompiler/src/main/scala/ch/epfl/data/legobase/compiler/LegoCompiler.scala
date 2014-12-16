package ch.epfl.data
package legobase
package compiler

import deep._
import prettyprinter._
import optimization._
import pardis.optimization._
import pardis.ir._
import pardis.types.PardisTypeImplicits._
import pardis.types._
import pardis.compiler._

class LegoCompiler(val DSL: LoweringLegoBase, val hashMapToArray: Boolean, val removeUnusedFields: Boolean, val number: Int, val generateCCode: Boolean) extends Compiler[LoweringLegoBase] {
  object MultiMapOptimizations extends TransformerHandler {
    def apply[Lang <: Base, T: PardisType](context: Lang)(block: context.Block[T]): context.Block[T] = {
      new pardis.deep.scalalib.collection.MultiMapOptimalTransformation(context.asInstanceOf[LoweringLegoBase]).optimize(block)
    }
  }

  def outputFile: String = "Q" + number

  def compile[T: PardisType](program: => Expression[T]): Unit = compile[T](program, outputFile)

  pipeline += LBLowering(generateCCode, removeUnusedFields)
  pipeline += ParameterPromotion
  pipeline += DCE
  pipeline += PartiallyEvaluate

  if (generateCCode) {
    //pipeline += ColumnStoreTransformer
  }

  // pipeline += PartiallyEvaluate
  // pipeline += HashMapHoist
  // pipeline += HashMapToArrayTransformer(generateCCode)
  //pipeline += MemoryManagementTransfomer //NOTE FIX TOPOLOGICAL SORT :-(

  pipeline += TreeDumper

  pipeline += MultiMapOptimizations
  pipeline += HashMapToSetTransformation

  // pipeline += PartiallyEvaluate
  pipeline += DCE

  pipeline += SetLinkedListTransformation

  pipeline += AssertTransformer(TypeAssertion(t => !t.isInstanceOf[DSL.SetType[_]]))

  pipeline += ContainerFlatTransformer

  // pipeline += SetArrayTransformation

  //pipeline += ParameterPromotion

  //pipeline += DCE

  // pipeline += PartiallyEvaluate
  pipeline += SingletonArrayToValueTransformer
  // pipeline += PartiallyEvaluate

  if (generateCCode) pipeline += CTransformersPipeline

  pipeline += DCECLang //NEVER REMOVE!!!!

  val codeGenerator =
    if (generateCCode)
      new LegoCGenerator(false, outputFile, false)
    else
      new LegoScalaGenerator(false, outputFile)

}

object TreeDumper extends TransformerHandler {
  def apply[Lang <: Base, T: PardisType](context: Lang)(block: context.Block[T]): context.Block[T] = {
    val pw = new java.io.PrintWriter(new java.io.File("tree_debug_dump.txt"))
    pw.println(block.toString)
    pw.flush()
    block
  }
}
