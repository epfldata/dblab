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

  pipeline += LBLowering(removeUnusedFields)
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

  pipeline += MultiMapOptimizations
  pipeline += HashMapToSetTransformation

  // pipeline += PartiallyEvaluate
  pipeline += DCE

  // pipeline += SetLinkedListTransformation

  // // pipeline += ContainerFlatTransformer
  // pipeline += ContainerLowering

  pipeline += SetArrayTransformation

  pipeline += AssertTransformer(TypeAssertion(t => !t.isInstanceOf[DSL.SetType[_]]))

  //pipeline += ParameterPromotion

  pipeline += DCE

  pipeline += TreeDumper(false)

  pipeline += PartiallyEvaluate
  pipeline += SingletonArrayToValueTransformer
  // pipeline += new OptionToCTransformer(DSL)
  // pipeline += new Tuple2ToCTransformer(DSL)
  pipeline += new OptionToCTransformer(DSL) | new Tuple2ToCTransformer(DSL)

  // pipeline += PartiallyEvaluate
  // pipeline += DCE

  // pipeline += ColumnStorePartitioner

  // pipeline += ParameterPromotion
  // pipeline += PartiallyEvaluate
  // pipeline += DCE
  // pipeline += ParameterPromotion
  // pipeline += DCE

  if (generateCCode) pipeline += CTransformersPipeline

  pipeline += DCECLang //NEVER REMOVE!!!!

  val codeGenerator =
    if (generateCCode)
      new LegoCGenerator(false, outputFile, true)
    else
      // new LegoScalaGenerator(false, outputFile)
      new LegoScalaASTGenerator(DSL, false, outputFile)

}

object TreeDumper {
  def apply(pretty: Boolean) = new TransformerHandler {
    def apply[Lang <: Base, T: PardisType](context: Lang)(block: context.Block[T]): context.Block[T] = {
      if (pretty) {
        val cg = new LegoScalaGenerator(false, "tree_debug_dump.txt")
        val pw = new java.io.PrintWriter("tree_debug_dump.txt")
        val doc = cg.blockToDocument(block)
        doc.format(40, pw)
        pw.flush()
      } else {
        val pw = new java.io.PrintWriter(new java.io.File("tree_debug_dump.txt"))
        pw.println(block.toString)
        pw.flush()
      }

      block
    }
  }
}
