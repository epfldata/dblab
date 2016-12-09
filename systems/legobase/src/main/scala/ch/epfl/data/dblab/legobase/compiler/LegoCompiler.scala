package ch.epfl.data
package dblab
package legobase
package compiler

import config.Config._
import schema._
import deep._
import prettyprinter._
import transformers._
import transformers.c._
import transformers.monad._
import sc.pardis.optimization._
import sc.pardis.ir._
import sc.pardis.types.PardisTypeImplicits._
import sc.pardis.types._
import sc.pardis.compiler._
import sc.pardis.language._

/**
 * The class which is responsible for wiring together different parts of the compilation pipeline
 * such as program reification, optimization pipeline, and code generation.
 *
 * @param DSL the polymorphic embedding trait which contains the reified program.
 * This object takes care of online partial evaluation
 * @param settings the compiler settings provided as command line arguments
 * @param schema the given schema information
 * @param runnerClassName the name of the runner class which is used in Scala code generation
 */
class LegoCompiler(val DSL: LegoBaseQueryEngineExp,
                   val settings: Settings,
                   val schema: Schema,
                   val runnerClassName: String) extends Compiler[LegoBaseQueryEngineExp] {
  def outputFile: String =
    if (settings.nameIsWithFlag)
      settings.args.filter(_.startsWith("+")).map(_.drop(1)).sorted.mkString("_") + "_" + settings.queryName
    else if (settings.hasOptimizationLevel)
      settings.queryName + "_L" + settings.getOptimizationLevel
    else
      settings.queryName

  val reportCompilationTime: Boolean = true

  override def compile[T: PardisType](program: => Expression[T], outputFile: String): Unit = {
    if (reportCompilationTime) {
      utils.Utilities.time({
        val block = utils.Utilities.time(DSL.reifyBlock(program), "Reification")
        val optimizedBlock = utils.Utilities.time(optimize(block), "Optimization")
        val irProgram = irToProgram.createProgram(optimizedBlock)
        utils.Utilities.time(codeGenerator.generate(irProgram, outputFile), "Code Generation")
      }, "Total compilation time")
    } else {
      super.compile(program, outputFile)
    }
  }

  override def irToProgram = if (settings.targetLanguage == CCoreLanguage) {
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
  def shouldRemoveUnusedFields = settings.forceFieldRemoval || settings.stringCompression || (settings.queryMonadLowering || settings.hashMapPartitioning ||
    (
      settings.hashMapLowering && (settings.setToArray || settings.setToLinkedList))) && !settings.noFieldRemoval

  pipeline += new StatisticsEstimator(DSL, schema)

  // pipeline += TreeDumper(false)
  val recordLowering = RecordLowering(DSL, shouldRemoveUnusedFields, settings.forceCompliant)
  pipeline += recordLowering
  // pipeline += TreeDumper(false)
  pipeline += ParameterPromotion
  pipeline += DCE
  pipeline += PartiallyEvaluate

  if (settings.queryMonadLowering) {
    if (settings.queryMonadOptimization) {
      pipeline += new QueryMonadOptimization(settings.queryMonadHoisting)
    }
    pipeline += DCE
    val queryMonadLowering = new QueryMonadLowering(schema, DSL, recordLowering.recordLowering)
    pipeline += new QueryMonadNoHorizontalVerifyer(DSL)
    if (settings.queryMonadCPS) {
      pipeline += new QueryMonadCPSLowering(schema, DSL, queryMonadLowering)
    } else if (settings.queryMonadIterator) {
      // these should be together
      pipeline += new QueryMonadIteratorLowering(schema, DSL, queryMonadLowering, settings.queryMonadIteratorBadFilter)
      // this should be alone
      // pipeline += new QueryMonadUnfoldLowering(schema, DSL, queryMonadLowering)
    } else if (settings.queryMonadStream) {
      pipeline += new QueryMonadStreamLowering(schema, DSL, settings.queryMonadStreamChurch, queryMonadLowering)
    } else {
      pipeline += queryMonadLowering
      pipeline += ParameterPromotion
    }
    if (!settings.queryMonadNoEscape) {
      pipeline += new CoreLanguageToC(DSL)
      pipeline += DCE
      pipeline += new ParameterPromotionWithVar(DSL)
      pipeline += DCE
      pipeline += PartiallyEvaluate
      pipeline += new ParameterPromotionWithVar(DSL)
    }
    pipeline += DCE
    pipeline += PartiallyEvaluate
  } else {
    // pipeline += PartiallyEvaluate
  }

  pipeline += HashMapHoist

  if (!settings.noSingletonHashMap)
    //pipeline += SingletonHashMapToValueTransformer
    pipeline += SingletonHashMapToValueTransformer2
  //pipeline += TreeDumper(true)

  if (settings.hashMapToArray) {
    pipeline += ConstSizeArrayToLocalVars
    pipeline += DCE
    // pipeline += TreeDumper(false)
    pipeline += new HashMapTo1DArray(DSL)
  }

  if (settings.hashMapPartitioning) {
    pipeline += new HashMapGrouping(DSL, schema, settings.forceCompliant)
    pipeline += ParameterPromotion
    pipeline += PartiallyEvaluate
    pipeline += DCE
    // if (settings.queryMonadLowering) {
    //   pipeline += new ScalaArrayToCCommon(DSL)
    //   pipeline += DCE
    // }
  }

  // pipeline += TreeDumper(true)

  if (settings.hashMapLowering || settings.hashMapNoCollision) {
    if (settings.hashMapLowering) {
      pipeline += new MultiMapToSetTransformation(DSL, schema)
      if (!settings.noDSHoist)
        pipeline += new HashMapToSetTransformation(DSL, schema)
      else
        pipeline += new HashMapToSetWithMallocTransformation(DSL, schema)
    }
    if (settings.hashMapNoCollision) {
      pipeline += new HashMapNoCollisionTransformation(DSL, schema)
      // pipeline += TreeDumper(false)
    }
    // pipeline += PartiallyEvaluate
    pipeline += DCE

    if (settings.setToLinkedList) {
      pipeline += new SetToLinkedListTransformation(DSL)
      if (settings.containerFlattenning) {
        pipeline += ContainerFlatTransformer
      }
      pipeline += ContainerLowering
    }

    if (settings.setToArray) {
      pipeline += new SetToArrayTransformation(DSL, schema)
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

  if (settings.stringCompression) pipeline += new StringDictionaryTransformer(DSL, schema)
  // pipeline += TreeDumper(false)

  if (settings.whileToForLoop) {
    pipeline += new WhileToRangeForeachTransformer(DSL)
    pipeline += DCE
  }

  if (settings.partitioning) {
    // pipeline += TreeDumper(false)
    pipeline += new WhileToRangeForeachTransformer(DSL)
    pipeline += new IntroduceHashIndexForRangeLookup(DSL, schema)
    pipeline += DCE
  }

  // pipeline += PartiallyEvaluate
  // pipeline += DCE

  if (settings.constArray) {
    pipeline += ConstSizeArrayToLocalVars
  }

  if (settings.columnStore) {
    pipeline += new ColumnStoreTransformer(DSL)
    pipeline += ParameterPromotion
    pipeline += PartiallyEvaluate

    pipeline += DCE
    pipeline += ParameterPromotion
    pipeline += PartiallyEvaluate
    pipeline += DCE
    pipeline += ParameterPromotion
    pipeline += DCE
  }

  if (settings.relationColumn) {
    pipeline += new RelationColumnarLayoutTransformer(DSL)
    pipeline += DCE
    // pipeline += TreeDumper(true)
    pipeline += new ParameterPromotionWithVar(DSL)
    pipeline += PartiallyEvaluate
    pipeline += DCE
  }

  if (settings.queryMonadLowering) {
    if (!settings.mallocHoisting) {
      pipeline += new ScalaArrayToCCommon(DSL)
      pipeline += DCE
    }
    pipeline += new Tuple2Lowering(DSL)
  }

  if (settings.mallocHoisting) {
    pipeline += new ScalaArrayToCCommon(DSL)
    pipeline += DCE
    pipeline += new MemoryAllocationHoist(DSL, schema)
  }

  if (settings.stringOptimization) {
    pipeline += new StringOptimization(DSL)
  }

  if (settings.largeOutputHoisting && !settings.onlyLoading) {
    pipeline += new LargeOutputPrintHoister(DSL, schema)
  }

  // pipeline += TreeDumper(false)

  if (settings.targetLanguage == CCoreLanguage) pipeline += new CTransformersPipeline(settings.cSettings)

  pipeline += DCECLang //NEVER REMOVE!!!!

  // pipeline += TreeDumper(true)

  val codeGenerator =
    if (settings.targetLanguage == CCoreLanguage) {
      if (settings.noLetBinding)
        new QueryEngineCASTGenerator(DSL, outputFile, settings.papiProfile, true)
      else
        new QueryEngineCGenerator(outputFile, settings.papiProfile, true)
    } else {
      if (settings.noLetBinding)
        new QueryEngineScalaASTGenerator(DSL, false, outputFile, runnerClassName)
      else
        new QueryEngineScalaGenerator(false, outputFile, runnerClassName)
    }

}
