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
  def validate(targetIsC: Boolean, tpchQuery: Int): Unit = {
    for (arg <- args.filter(arg => !ALL_FLAGS.contains(arg))) {
      println(s"${Console.YELLOW}Warning: flag $arg is not defined!${Console.BLACK}")
    }
    if (!hashMapLowering && targetIsC) {
      throw new Exception(s"C code generator for HashMap and MultiMap is not supported yet! Consider adding $hm2set.")
    }
    if (!(setToArray || setToLinkedList) && targetIsC) {
      throw new Exception(s"C code generator for Set is not supported yet! Consider adding $set2arr or $set2ll.")
    }
    if (!hashMapLowering && (setToArray || setToLinkedList || containerFlattenning))
      throw new Exception("It's impossible to lower Sets without lowering HashMap and MultiMap!")
    val SUPPORTED_CS = (1 to 22).toList diff (List(13))
    if ((columnStore || partitioning) && (!SUPPORTED_CS.contains(tpchQuery)))
      throw new Exception(s"$cstore and $part only work for the Queries ${SUPPORTED_CS.mkString(" & ")} for the moment!")
  }
  @inline def hasFlag(flag: String): Boolean = args.exists(_ == flag)
  def hashMapLowering: Boolean = hasFlag(hm2set)
  def setToArray: Boolean = hasFlag(set2arr)
  def setToLinkedList: Boolean = hasFlag(set2ll)
  def containerFlattenning: Boolean = hasFlag(contFlat)
  def columnStore: Boolean = hasFlag(cstore)
  def partitioning: Boolean = hasFlag(part)
  def hashMapPartitioning: Boolean = hasFlag(hmPart)
  def mallocHoisting: Boolean = hasFlag(mallocHoist)
  def constArray: Boolean = hasFlag(constArr)
  def stringCompression: Boolean = hasFlag(comprStrings)
}

object Settings {
  val hm2set = "+hm2set"
  val set2arr = "+set2arr"
  val set2ll = "+set2ll"
  val contFlat = "+cont-flat"
  val cstore = "+cstore"
  val part = "+part"
  val hmPart = "+hm-part"
  val mallocHoist = "+malloc-hoist"
  val constArr = "+const-arr"
  val comprStrings = "+comprStrings"
  val ALL_FLAGS = List(hm2set, set2arr, set2ll, contFlat, cstore, part, hmPart, mallocHoist, constArr, comprStrings)
}

class LegoCompiler(val DSL: LoweringLegoBase, val removeUnusedFields: Boolean, val number: Int, val generateCCode: Boolean, val settings: Settings) extends Compiler[LoweringLegoBase] {
  object MultiMapOptimizations extends TransformerHandler {
    def apply[Lang <: Base, T: PardisType](context: Lang)(block: context.Block[T]): context.Block[T] = {
      new pardis.deep.scalalib.collection.MultiMapOptimalTransformation(context.asInstanceOf[LoweringLegoBase]).optimize(block)
    }
  }

  def outputFile: String = "Q" + number

  val reportCompilationTime: Boolean = true

  override def compile[T: PardisType](program: => Expression[T], outputFile: String): Unit = {
    if (reportCompilationTime) {
      val block = utils.Utilities.time(DSL.reifyBlock(program), "Reification")
      val optimizedBlock = utils.Utilities.time(optimize(block), "Optimization")
      val irProgram = IRToProgram(DSL).createProgram(optimizedBlock)
      utils.Utilities.time(codeGenerator.generate(irProgram, outputFile), "Code Generation")
    } else {
      super.compile(program, outputFile)
    }
  }

  def compile[T: PardisType](program: => Expression[T]): Unit = compile[T](program, outputFile)
  /**
   * If MultiMap is remaining without being converted to something which doesn't have set,
   * the field removal causes the program to be wrong
   */
  def shouldRemoveUnusedFields = removeUnusedFields && (settings.hashMapPartitioning ||
    (
      settings.hashMapLowering && (settings.setToArray || settings.setToLinkedList)))

  pipeline += LBLowering(shouldRemoveUnusedFields)
  pipeline += ParameterPromotion
  pipeline += DCE
  pipeline += PartiallyEvaluate

  // if (generateCCode) {
  //   //pipeline += ColumnStoreTransformer
  // }

  // pipeline += PartiallyEvaluate
  pipeline += HashMapHoist
  pipeline += SingletonHashMapToValueTransformer
  // pipeline += HashMapToArrayTransformer(generateCCode)
  //pipeline += MemoryManagementTransfomer //NOTE FIX TOPOLOGICAL SORT :-(
  // pipeline += SingletonArrayToValueTransformer

  if (settings.hashMapPartitioning) {
    pipeline += new HashMapPartitioningTransformer(DSL, number)
    pipeline += ParameterPromotion
    pipeline += PartiallyEvaluate
    pipeline += DCE
  }

  if (settings.stringCompression) pipeline += StringCompressionTransformer
  pipeline += TreeDumper(false)

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

  // pipeline += PartiallyEvaluate
  // pipeline += DCE

  // if (settings.constArray) {
  //   pipeline += ConstSizeArrayToLocalVars
  // }

  if (settings.columnStore) {
    pipeline += new ColumnStoreTransformer(DSL, number)
    // if (settings.hashMapPartitioning) {
    //   pipeline += new ColumnStore2DTransformer(DSL, number)
    // }
    pipeline += ParameterPromotion
    pipeline += PartiallyEvaluate
    pipeline += DCE
    if (settings.partitioning) {
      pipeline += new PartitionTransformer(DSL)
    }
    pipeline += ParameterPromotion
    pipeline += PartiallyEvaluate
    pipeline += DCE
    pipeline += ParameterPromotion
    pipeline += DCE
  }

  if (settings.mallocHoisting) {
    pipeline += MemoryAllocationHoist
  }

  if (generateCCode) pipeline += CTransformersPipeline

  pipeline += DCECLang //NEVER REMOVE!!!!

  // pipeline += TreeDumper(false)

  val codeGenerator =
    if (generateCCode)
      // new LegoCGenerator(false, outputFile, true)
      new LegoCASTGenerator(DSL, false, outputFile, true)
    else
      //new LegoScalaGenerator(false, outputFile)
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
