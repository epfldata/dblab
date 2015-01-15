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
import pardis.deep.scalalib.collection._

object HashMapToSetTransformation extends TransformerHandler {
  def apply[Lang <: Base, T: PardisType](context: Lang)(block: context.Block[T]): context.Block[T] = {
    new HashMapToSetTransformation(context.asInstanceOf[LoweringLegoBase]).optimize(block)
  }
}

class HashMapToSetTransformation(val LB: LoweringLegoBase) extends HashMapOptimalTransformation(LB) {
  import LB._
  override def hashMapExtractKey[A, B](self: Rep[HashMap[A, B]], value: Rep[B])(implicit typeA: TypeRep[A], typeB: TypeRep[B]): Rep[A] = typeB match {
    case t if t.isRecord && t.name.startsWith("AGGRecord") => {
      // class C
      // implicit val typeC = t.asInstanceOf[TypeRep[C]]
      field[A](value, "key")
    }
    case t => throw new Exception(s"does not know how to extract key for type $t")
  }
}
