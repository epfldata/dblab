package ch.epfl.data
package dblab.legobase
package compiler

import Config._
import schema._
import deep._
import prettyprinter._
import optimization._
import optimization.c._
import sc.pardis.optimization._
import sc.pardis.ir._
import sc.pardis.types.PardisTypeImplicits._
import sc.pardis.types._
import sc.pardis.compiler._

/**
 * The class which is responsible for wiring together different parts of the compilation pipeline
 * such as program reification, optimization pipeline, and code generation.
 *
 * @param DSL the polymorphic embedding trait which contains the reified program.
 * This object takes care of online partial evaluation
 * @param number specifies the TPCH query number (TODO should be removed)
 * @param settings the compiler settings provided as command line arguments
 */
class LegoCompiler(val DSL: LoweringLegoBase, val number: Int, val settings: Settings, val schema: Schema) extends Compiler[LoweringLegoBase] {
  def outputFile: String = {
    def queryWithNumber =
      settings.queryName
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

  override def irToPorgram = if (settings.targetLanguage == CCodeGeneration) {
    IRToCProgram(DSL)
  } else {
    IRToProgram(DSL)
  }

  def compile[T: PardisType](program: => Expression[T]): Unit = compile[T](program, outputFile)
  /**
   * If MultiMap is remaining without being converted to something which doesn't have set,
   * the field removal causes the program to be wrong
   */
  //TODO-GEN Remove gen and make string compression transformer dependant on removing unnecessary fields.
  def shouldRemoveUnusedFields = settings.stringCompression || (settings.hashMapPartitioning ||
    (
      settings.hashMapLowering && (settings.setToArray || settings.setToLinkedList))) && !settings.noFieldRemoval

  pipeline += new StatisticsEstimator(DSL, schema)

  pipeline += LBLowering(shouldRemoveUnusedFields)
  // pipeline += TreeDumper(false)
  pipeline += ParameterPromotion
  pipeline += DCE
  pipeline += PartiallyEvaluate

  // pipeline += PartiallyEvaluate
  pipeline += HashMapHoist
  if (!settings.noSingletonHashMap)
    pipeline += SingletonHashMapToValueTransformer

  if (settings.hashMapToArray) {
    pipeline += ConstSizeArrayToLocalVars
    pipeline += DCE
    // pipeline += TreeDumper(true)
    pipeline += new HashMapTo1DArray(DSL)
  }

  if (settings.hashMapPartitioning) {
    pipeline += new HashMapPartitioningTransformer(DSL, schema)
    pipeline += ParameterPromotion
    pipeline += PartiallyEvaluate
    pipeline += DCE
  }

  if (settings.stringCompression) pipeline += new StringDictionaryTransformer(DSL, schema)
  // pipeline += TreeDumper(false)

  if (settings.hashMapLowering || settings.hashMapNoCollision) {
    if (settings.hashMapLowering) {
      pipeline += new sc.pardis.deep.scalalib.collection.MultiMapOptimalTransformation(DSL)
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
      pipeline += new SetArrayTransformation(DSL, schema)
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

  if (settings.partitioning) {
    pipeline += TreeDumper(false)
    pipeline += new WhileToRangeForeachTransformer(DSL)
    pipeline += new ArrayPartitioning(DSL, schema)
    pipeline += DCE
  }

  // pipeline += PartiallyEvaluate
  // pipeline += DCE

  if (settings.constArray) {
    pipeline += ConstSizeArrayToLocalVars
    // pipeline += SingletonArrayToValueTransformer
  }

  if (settings.columnStore) {

    pipeline += new ColumnStoreTransformer(DSL, settings)
    pipeline += ParameterPromotion
    pipeline += PartiallyEvaluate

    pipeline += DCE
    pipeline += ParameterPromotion
    pipeline += PartiallyEvaluate
    pipeline += DCE
    pipeline += ParameterPromotion
    pipeline += DCE
  }

  if (settings.mallocHoisting) {
    pipeline += new MemoryAllocationHoist(DSL, schema)
  }

  if (settings.stringOptimization) {
    pipeline += new StringOptimization(DSL)
  }

  if (settings.largeOutputHoisting && !settings.onlyLoading) {
    pipeline += new LargeOutputPrintHoister(DSL, schema)
  }

  if (settings.targetLanguage == CCodeGeneration) pipeline += new CTransformersPipeline(settings)

  pipeline += DCECLang //NEVER REMOVE!!!!

  val codeGenerator =
    if (settings.targetLanguage == CCodeGeneration) {
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
