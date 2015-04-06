package ch.epfl.data
package dblab.legobase
package optimization
package c

import deep._
import sc.pardis.types._
import sc.pardis.ir._
import sc.pardis.optimization._

class CTransformersPipeline(val settings: compiler.Settings) extends TransformerHandler {
  def apply[Lang <: Base, T: PardisType](context: Lang)(block: context.Block[T]): context.Block[T] = {
    apply[T](context.asInstanceOf[LoweringLegoBase], block)
  }
  def apply[A: PardisType](context: LoweringLegoBase, b: PardisBlock[A]) = {
    val pipeline = new TransformerPipeline()
    pipeline += new GenericEngineToCTransformer(context, settings)
    pipeline += new ScalaScannerToCmmapTransformer(context, settings)
    // pipeline += new ScalaScannerToCFScanfTransformer(context)
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
    // pipeline += compiler.TreeDumper(false)
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
