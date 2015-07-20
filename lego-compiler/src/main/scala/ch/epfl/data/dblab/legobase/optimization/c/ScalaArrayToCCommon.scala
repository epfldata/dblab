package ch.epfl.data
package dblab.legobase
package optimization
package c

import deep._
import sc.pardis.optimization._
import sc.pardis.ir._
import sc.pardis.types.PardisTypeImplicits._
import sc.pardis.types._
import scala.language.existentials

/**
 * Lowers Scala Array operations to more low-level operations.
 * The result low-level operations are understandable by the C transformer.
 *
 * @param IR the polymorphic embedding trait which contains the reified program.
 */
class ScalaArrayToCCommon(override val IR: LegoBaseExp) extends RuleBasedTransformer[LegoBaseExp](IR) with CTransformer {
  import IR._
  import CNodes._
  import CTypes._

  rewrite += rule {
    case n @ ArrayApplyObject(Def(LiftedSeq(elems))) =>
      class T
      val tp = n.tp.typeArguments(0)
      implicit val typeT = tp.asInstanceOf[TypeRep[T]]
      // TODO generalize
      assert(tp == DoubleType)
      val array = __newArray[T](unit(elems.size))
      for (i <- 0 until elems.size) {
        array(unit(i.toInt)) = elems(i.toInt).asInstanceOf[Rep[T]]
      }
      array.asInstanceOf[Rep[Any]]
  }

  val dropRightArrays = scala.collection.mutable.Map[Rep[Any], ArrayDropRight[Any]]()

  rewrite += statement {
    case sym -> (node @ ArrayDropRight(array, num)) =>
      dropRightArrays(sym) = node
      array
  }

  rewrite += rule {
    case ArrayLength(array) if dropRightArrays.contains(array) =>
      val originalArray = apply(array)
      dropRightArrays(array).num match {
        // TODO needs checking arr2 == originalArray
        case Def(Int$minus1(Def(ArrayLength(arr2)), num2)) => num2
        case num => originalArray.length - num
      }

  }
}
