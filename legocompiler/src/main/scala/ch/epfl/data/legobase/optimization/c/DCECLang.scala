package ch.epfl.data
package legobase
package optimization
package c

import scala.language.implicitConversions
import pardis.ir._
import reflect.runtime.universe.{ TypeTag, Type }
import pardis.optimization._
import deep._
import pardis.types._
import pardis.types.PardisTypeImplicits._

object DCECLang extends TransformerHandler {
  def apply[Lang <: Base, T: PardisType](context: Lang)(block: context.Block[T]): context.Block[T] = {
    new DCECLang(context.asInstanceOf[LoweringLegoBase]).optimize(block)
  }
}

class DCECLang(override val IR: LoweringLegoBase) extends DCE[LoweringLegoBase](IR) with CTransformer
