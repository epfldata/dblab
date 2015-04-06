package ch.epfl.data
package dblab.legobase
package compiler

import deep._
import prettyprinter._
import optimization._
import optimization.c._
import sc.pardis.optimization._
import sc.pardis.ir._
import sc.pardis.types.PardisTypeImplicits._
import sc.pardis.types._
import sc.pardis.compiler._

class LegoCompiler(val DSL: LoweringLegoBase, val number: Int, val scalingFactor: Double, val generateCCode: Boolean, val settings: Settings) extends Compiler[LoweringLegoBase] {
  object MultiMapOptimizations extends TransformerHandler {
    def apply[Lang <: Base, T: PardisType](context: Lang)(block: context.Block[T]): context.Block[T] = {
      new sc.pardis.deep.scalalib.collection.MultiMapOptimalTransformation(context.asInstanceOf[LoweringLegoBase]).optimize(block)
    }
  }

  def outputFile: String = {
    def queryWithNumber =
      if (settings.isSynthesized)
        settings.queryName
      else
        "Q" + number
    def argsString = settings.args.filter(_.startsWith("+")).map(_.drop(1)).sorted.mkString("_")
    if (settings.nameIsWithFlag)
      argsString + "_" + queryWithNumber
    else
      queryWithNumber
  }

  val reportCompilationTime: Boolean = true

  override def compile[T: PardisType](program: => Expression[T], outputFile: String): Unit = {
    if (reportCompilationTime) {
      val block = utils.Utilities.time(DSL.reifyBlock(program), "Reification")
      val optimizedBlock = utils.Utilities.time(optimize(block), "Optimization")
      val irProgram = irToPorgram.createProgram(optimizedBlock)
      utils.Utilities.time(codeGenerator.generate(irProgram, outputFile), "Code Generation")
    } else {
      super.compile(program, outputFile)
    }
  }

  override def irToPorgram = if (generateCCode) {
    IRToCProgram(DSL)
  } else {
    IRToProgram(DSL)
  }

  def compile[T: PardisType](program: => Expression[T]): Unit = compile[T](program, outputFile)
  /**
   * If MultiMap is remaining without being converted to something which doesn't have set,
   * the field removal causes the program to be wrong
   */
  def shouldRemoveUnusedFields = (settings.hashMapPartitioning ||
    (
      settings.hashMapLowering && (settings.setToArray || settings.setToLinkedList))) && !settings.noFieldRemoval
  // pipeline += TreeDumper(false)
  pipeline += LBLowering(shouldRemoveUnusedFields)
  pipeline += ParameterPromotion
  pipeline += DCE
  pipeline += PartiallyEvaluate

  // pipeline += PartiallyEvaluate
  pipeline += HashMapHoist
  if (!settings.noSingletonHashMap)
    pipeline += SingletonHashMapToValueTransformer

  if (settings.hashMapPartitioning) {

    if (number == 18) {
      pipeline += ConstSizeArrayToLocalVars
      pipeline += DCE
      pipeline += TreeDumper(true)
      pipeline += new HashMapTo1DArray(DSL)
    }
    pipeline += new HashMapPartitioningTransformer(DSL, number, scalingFactor)

    pipeline += ParameterPromotion
    pipeline += PartiallyEvaluate
    pipeline += DCE
  }

  if (settings.stringCompression) pipeline += new StringCompressionTransformer(DSL, number)
  pipeline += TreeDumper(false)

  if (settings.hashMapLowering || settings.hashMapNoCollision) {
    if (settings.hashMapLowering) {
      pipeline += MultiMapOptimizations
      pipeline += new HashMapToSetTransformation(DSL, number)
    }
    if (settings.hashMapNoCollision) {
      pipeline += new HashMapNoCollisionTransformation(DSL, number)
      // pipeline += TreeDumper(false)
    }
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
    if (settings.setToLinkedList || settings.setToArray || settings.hashMapNoCollision) {
      pipeline += AssertTransformer(TypeAssertion(t => !t.isInstanceOf[DSL.SetType[_]]))
      //pipeline += ParameterPromotion
      pipeline += DCE
      pipeline += PartiallyEvaluate
      pipeline += new OptionToCTransformer(DSL) | new Tuple2ToCTransformer(DSL)
      pipeline += ParameterPromotion
    }

    pipeline += new BlockFlattening(DSL) // should not be needed!
  }

  if (settings.partitioning && number == 3) {
    pipeline += new WhileToRangeForeachTransformer(DSL)
    pipeline += new ArrayPartitioning(DSL, number)
    pipeline += DCE
  }

  // pipeline += PartiallyEvaluate
  // pipeline += DCE

  if (settings.constArray) {
    pipeline += ConstSizeArrayToLocalVars
    // pipeline += SingletonArrayToValueTransformer
  }

  if (settings.columnStore) {

    pipeline += new ColumnStoreTransformer(DSL, number, settings)
    // if (settings.hashMapPartitioning) {
    //   pipeline += new ColumnStore2DTransformer(DSL, number)
    // }
    pipeline += ParameterPromotion
    pipeline += PartiallyEvaluate

    pipeline += DCE
    if (settings.partitioning && number == 6) {
      pipeline += new PartitionTransformer(DSL)
    }
    pipeline += ParameterPromotion
    pipeline += PartiallyEvaluate
    pipeline += DCE
    pipeline += ParameterPromotion
    pipeline += DCE
  }

  if (settings.mallocHoisting) {
    pipeline += new MemoryAllocationHoist(DSL, number, scalingFactor)
  }

  if (settings.stringOptimization) {
    pipeline += new StringOptimization(DSL)
  }

  if (settings.largeOutputHoisting(generateCCode, number)) {
    pipeline += new LargeOutputPrintHoister(DSL)
  }

  if (generateCCode) pipeline += new CTransformersPipeline(settings)

  pipeline += DCECLang //NEVER REMOVE!!!!

  val codeGenerator =
    if (generateCCode) {
      if (settings.noLetBinding)
        new LegoCASTGenerator(DSL, false, outputFile, true)
      else
        new LegoCGenerator(false, outputFile, true)
    } else {
      if (settings.noLetBinding)
        new LegoScalaASTGenerator(DSL, false, outputFile)
      else
        new LegoScalaGenerator(false, outputFile)
    }

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
