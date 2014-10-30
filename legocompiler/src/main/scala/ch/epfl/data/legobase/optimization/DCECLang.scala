package ch.epfl.data
package legobase
package optimization

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

class DCECLang(override val IR: LoweringLegoBase) extends DCE[LoweringLegoBase](IR) with deep.CTransformer
// {
//   import IR._
//   import CNodes._

//   override def transformExp[T: TypeRep, S: TypeRep](exp: Rep[T]): Rep[S] = exp match {
//     case t: typeOf[_] => typeOf()(apply(t.tp)).asInstanceOf[Rep[S]]
//     case _            => super.transformExp[T, S](exp)
//   }
// }
