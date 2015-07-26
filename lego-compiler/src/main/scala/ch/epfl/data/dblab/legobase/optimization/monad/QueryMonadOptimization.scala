package ch.epfl.data
package dblab.legobase
package optimization
package monad

import scala.language.implicitConversions
import sc.pardis.ir._
import reflect.runtime.universe.{ TypeTag, Type }
import sc.pardis.optimization._
import deep._
import sc.pardis.types._
import sc.pardis.types.PardisTypeImplicits._
import sc.pardis.shallow.utils.DefaultValue
import compiler._

/**
 * Query monad optimizations.
 */
class QueryMonadOptimization extends TransformerHandler {
  def apply[Lang <: Base, T: PardisType](context: Lang)(block: context.Block[T]): context.Block[T] = {
    apply[T](context.asInstanceOf[LegoBaseExp], block)
  }
  def apply[A: PardisType](context: LegoBaseExp, b: PardisBlock[A]) = {
    val pipeline = new TransformerPipeline()
    pipeline += new QueryMonadVerticalFusion(context)
    pipeline += DCE
    pipeline += new QueryMonadVerticalFusion(context)
    pipeline += DCE

    pipeline += new QueryMonadHoisting(context)
    pipeline += DCE
    pipeline += new QueryMonadPostHoisting(context)
    pipeline += DCE

    pipeline += new QueryMonadHorizontalFusion(context)
    pipeline += DCE

    pipeline += new QueryMonadVerticalFusion(context)
    pipeline += DCE
    pipeline += TreeDumper(true)
    pipeline += new QueryMonadVerticalFusion(context)
    pipeline += DCE

    pipeline(context)(b)
  }
}
