package ch.epfl.data
package legobase
package optimization

import scala.language.implicitConversions
import pardis.ir._
import reflect.runtime.universe.{ TypeTag, Type }
import pardis.optimization._
import deep._
import ch.epfl.data.pardis.ir.pardisTypeImplicits._

class HashMapToArrayTransformer(override val IR: LoweringLegoBase) extends Optimizer[LoweringLegoBase](IR) {
  import IR._
  import CNodes._
  import CTypes._

  sealed trait Kind
  case object AggOpCase extends Kind
  case object OtherCase extends Kind

  val hashMapKinds = collection.mutable.Map[Sym[Any], Kind]()

  def optimize[T: TypeRep](node: Block[T]): to.Block[T] = {
    traverseBlock(node)
    System.out.println(hashMapKinds.mkString("\n"))
    transformProgram(node)
  }

  override def traverseDef(node: Def[_]): Unit = node match {
    case ArrayBufferAppend(Def(HashMapGetOrElseUpdate(hm, _, _)), _) => {
      // this will override the value written by the next one
      hashMapKinds(hm.asInstanceOf[Sym[Any]]) = OtherCase
    }
    case HashMapGetOrElseUpdate(hm, _, _) => {
      hashMapKinds(hm.asInstanceOf[Sym[Any]]) = AggOpCase
    }
    case _ => super.traverseDef(node)
  }

  override def transformDef[T: PardisType](node: Def[T]): to.Def[T] = (node match {
    case HashMapGetOrElseUpdate(hm, key, value) if (hashMapKinds(hm.asInstanceOf[Sym[Any]]) == OtherCase) => {
      val res = __ifThenElse(hashMapContains(hm, key), hashMapApply(hm, key), {
        val v = toAtom(value)
        hashMapUpdate(hm, key, v)
        v
      })
      ReadVal(res)
    }
    case _ => super.transformDef(node)
  }).asInstanceOf[to.Def[T]]
}
