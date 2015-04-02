package ch.epfl.data
package dblab.legobase
package optimization
package c

import scala.language.implicitConversions
import sc.pardis.ir._
import reflect.runtime.universe.{ TypeTag, Type }
import sc.pardis.optimization._
import deep._
import sc.pardis.types._
import sc.pardis.types.PardisTypeImplicits._

object DCECLang extends TransformerHandler {
  def apply[Lang <: Base, T: PardisType](context: Lang)(block: context.Block[T]): context.Block[T] = {
    new DCECLang(context.asInstanceOf[LoweringLegoBase]).optimize(block)
  }
}

class DCECLang(override val IR: LoweringLegoBase) extends DCE[LoweringLegoBase](IR) with CTransformer
