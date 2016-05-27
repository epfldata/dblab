package ch.epfl.data
package dblab
package transformers
package c

import deep._
import sc.pardis.types._
import sc.pardis.ir._
import deep.dsls.QueryEngineExp
import sc.pardis.optimization._

/**
 * A pipeline of transformations necessary to lower the program into an
 * equivalent program in C programming language.
 *
 * @param settings the compiler settings provided as command line arguments (TODO should be removed)
 */
class CTransformersPipeline(val settings: CTransformersPipelineSettings) extends TransformerHandler {
  def apply[Lang <: Base, T: PardisType](context: Lang)(block: context.Block[T]): context.Block[T] = {
    apply[T](context.asInstanceOf[QueryEngineExp], block)
  }
  def apply[A: PardisType](context: QueryEngineExp, b: PardisBlock[A]) = {
    val pipeline = new TransformerPipeline()
    pipeline += new GenericEngineToCTransformer(context, settings)
    pipeline += new ScalaScannerToCmmapTransformer(context, settings)
    // pipeline += new ScalaScannerToCFScanfTransformer(context)
    pipeline += new ScalaArrayToCCommon(context)
    if (settings.oldCArrayHandling) {
      pipeline += new ScalaArrayToCStructTransformer(context)
    } else {
      if (settings.pointerStore) {
        pipeline += new ScalaArrayToPointerBadRecordTransformer(context)
      } else {
        pipeline += new ScalaArrayToPointerTransformer(context, settings)
      }

      pipeline += new ScalaStructToMallocTransformer(context)
    }

    pipeline += new sc.cscala.deep.ManualGLibMultiMapTransformation(context)
    pipeline += new ScalaCollectionsToGLibTransfomer(context)
    pipeline += new Tuple2ToCTransformer(context)
    pipeline += new OptionToCTransformer(context)
    pipeline += new HashEqualsFuncsToCTransformer(context)
    pipeline += new OptimalStringToCTransformer(context)
    pipeline += new RangeToCTransformer(context)
    pipeline += new ScalaConstructsToCTranformer(context, settings.ifAggressive)
    pipeline += new BlockFlattening(context)
    if (settings.mallocProfile) {
      pipeline += new MallocProfiler(context)
    }
    pipeline(context)(b)
  }
}

case class CTransformersPipelineSettings(ifAggressive: Boolean, onlyLoading: Boolean, mallocProfile: Boolean,
                                         papiProfile: Boolean, oldCArrayHandling: Boolean, pointerStore: Boolean,
                                         containerFlattenning: Boolean)
