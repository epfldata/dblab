package ch.epfl.data
package dblab.legobase
package optimization
package c

import deep._
import sc.pardis.types._
import sc.pardis.ir._
import sc.pardis.optimization._

/**
 * A pipeline of transformations necessary to lower the program into an
 * equivalent program in C programming language.
 *
 * @param settings the compiler settings provided as command line arguments (TODO should be removed)
 */
class CTransformersPipeline(val settings: compiler.Settings) extends TransformerHandler {
  def apply[Lang <: Base, T: PardisType](context: Lang)(block: context.Block[T]): context.Block[T] = {
    apply[T](context.asInstanceOf[LegoBaseExp], block)
  }
  def apply[A: PardisType](context: LegoBaseExp, b: PardisBlock[A]) = {
    val pipeline = new TransformerPipeline()
    pipeline += new GenericEngineToCTransformer(context, settings)
    pipeline += new ScalaScannerToCmmapTransformer(context, settings)
    // pipeline += new ScalaScannerToCFScanfTransformer(context)
    // pipeline += compiler.TreeDumper(false)
    pipeline += new ScalaArrayToCCommon(context)
    if (settings.oldCArrayHandling) {
      pipeline += new ScalaArrayToCStructTransformer(context)
    } else {
      if (settings.pointerStore) {
        pipeline += new ScalaArrayToPointerBadRecordTransformer(context, settings)
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
    pipeline(context)(b)
  }
}
