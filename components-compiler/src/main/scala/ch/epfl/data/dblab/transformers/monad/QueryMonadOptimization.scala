package ch.epfl.data
package dblab
package transformers
package monad

import scala.language.implicitConversions
import sc.pardis.ir._
import deep.dsls.QueryEngineExp
import reflect.runtime.universe.{ TypeTag, Type }
import sc.pardis.optimization._
import deep._
import sc.pardis.types._
import sc.pardis.types.PardisTypeImplicits._
import sc.pardis.shallow.utils.DefaultValue

/**
 * Query monad optimizations.
 */
class QueryMonadOptimization(val queryMonadHoisting: Boolean) extends TransformerHandler {
  def apply[Lang <: Base, T: PardisType](context: Lang)(block: context.Block[T]): context.Block[T] = {
    apply[T](context.asInstanceOf[QueryEngineExp], block)
  }
  def apply[A: PardisType](context: QueryEngineExp, b: PardisBlock[A]) = {
    val pipeline = new TransformerPipeline()
    pipeline += new QueryMonadVerticalFusion(context)
    pipeline += DCE
    pipeline += new QueryMonadVerticalFusion(context)
    pipeline += DCE

    if (queryMonadHoisting) {
      pipeline += new QueryMonadHoisting(context)
      pipeline += DCE
      pipeline += new QueryMonadPostHoisting(context)
      pipeline += DCE
    }

    pipeline += new QueryMonadHorizontalFusion(context)
    pipeline += DCE

    pipeline += new QueryMonadVerticalFusion(context)
    pipeline += DCE
    pipeline += new QueryMonadVerticalFusion(context)
    pipeline += DCE

    pipeline(context)(b)
  }
}
