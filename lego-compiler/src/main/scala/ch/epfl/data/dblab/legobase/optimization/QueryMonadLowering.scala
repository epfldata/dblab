package ch.epfl.data
package dblab.legobase
package optimization

import scala.language.implicitConversions
import sc.pardis.ir._
import reflect.runtime.universe.{ TypeTag, Type }
import sc.pardis.optimization._
import deep._
import sc.pardis.types._
import sc.pardis.types.PardisTypeImplicits._
import sc.pardis.shallow.utils.DefaultValue

/**
 * Lowers query monad operations.
 */
class QueryMonadLowering(override val IR: LegoBaseExp) extends RuleBasedTransformer[LegoBaseExp](IR) {
  import IR._

  def array_filter[T: TypeRep](array: Rep[Array[T]], p: Rep[T => Boolean]): Rep[Array[T]] = {
    val Def(Lambda(pred, _, _)) = p
    val size = __newVarNamed[Int](unit(0), "arraySize")
    array_foreach(array, (elem: Rep[T]) => __ifThenElse(pred(elem),
      __assign(size, readVar(size) + unit(1)), unit()))
    val resultSize = readVar(size)
    val resultArray = __newArray[T](resultSize)
    val counter = __newVarNamed[Int](unit(0), "arrayCounter")
    array_foreach(array, (elem: Rep[T]) => {
      __ifThenElse(pred(elem),
        resultArray(readVar(counter)) = elem,
        unit())
      __assign(counter, readVar(counter) + unit(1))
    })
    resultArray
  }

  rewrite += rule {
    case QueryNew2(array) =>
      array
  }

  rewrite += rule {
    case QueryFilter(monad, p) =>
      val array = apply(monad).asInstanceOf[Rep[Array[Any]]]
      array_filter(array, p)(array.tp.typeArguments(0).asInstanceOf[TypeRep[Any]])
  }

  // TODO needs quasi lifting legobase.
  rewrite += rule {
    case OptimalStringStartsWith(str, Def(GenericEngineParseStringObject(Constant(str2: String)))) =>
      str2.toCharArray.zipWithIndex.foldLeft(unit(true))((acc, curr) => acc && (str(curr._2) __== unit(curr._1)))
  }
}
