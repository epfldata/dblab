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

class Settings(val args: List[String]) {
  import Settings._
  def validate(targetIsC: Boolean): Unit = {
    if (!hashMapLowering && targetIsC) {
      throw new Exception(s"C code generator for HashMap and MultiMap is not supported yet! Consider adding $hm2set.")
    }
    if (!(setToArray || setToLinkedList) && targetIsC) {
      throw new Exception("C code generator for Set is not supported yet! Consider adding $set2arr or $set2ll.")
    }
    if (!hashMapLowering && (setToArray || setToLinkedList || containerFlattenning))
      throw new Exception("It's impossible to lower Sets without lowering HashMap and MultiMap!")
  }
  def hashMapLowering: Boolean = args.exists(_ == hm2set)
  def setToArray: Boolean = args.exists(_ == set2arr)
  def setToLinkedList: Boolean = args.exists(_ == set2ll)
  def containerFlattenning: Boolean = args.exists(_ == contFlat)
}

object Settings {
  val hm2set = "+hm2set"
  val set2arr = "+set2arr"
  val set2ll = "+set2ll"
  val contFlat = "+cont-flat"
}

class LegoCompiler(val DSL: LoweringLegoBase, val removeUnusedFields: Boolean, val number: Int, val generateCCode: Boolean, val settings: Settings) extends Compiler[LoweringLegoBase] {
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

  pipeline += StringCompressionTransformer

  if (settings.hashMapLowering) {
    pipeline += MultiMapOptimizations
    pipeline += HashMapToSetTransformation
    // pipeline += PartiallyEvaluate
    pipeline += DCE

    if (settings.setToLinkedList) {
      pipeline += SetLinkedListTransformation
      if (settings.containerFlattenning) {
        pipeline += ContainerFlatTransformer
      }
      pipeline += ContainerLowering
    }

    if (settings.setToArray) {
      pipeline += SetArrayTransformation
    }
    if (settings.setToLinkedList || settings.setToArray) {
      pipeline += AssertTransformer(TypeAssertion(t => !t.isInstanceOf[DSL.SetType[_]]))
      //pipeline += ParameterPromotion
      pipeline += DCE
      pipeline += PartiallyEvaluate
      pipeline += new OptionToCTransformer(DSL) | new Tuple2ToCTransformer(DSL)
    }

  }

  pipeline += TreeDumper(false)

  pipeline += SingletonArrayToValueTransformer

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
      // new LegoCGenerator(false, outputFile, true)
      new LegoCASTGenerator(DSL, false, outputFile, true)
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
