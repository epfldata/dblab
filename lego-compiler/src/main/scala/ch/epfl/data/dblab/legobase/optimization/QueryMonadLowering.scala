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
      __ifThenElse(pred(elem), {
        resultArray(readVar(counter)) = elem
        __assign(counter, readVar(counter) + unit(1))
      },
        unit())

    })
    resultArray
  }

  def array_dropRight[T: TypeRep](array: Rep[Array[T]], keepNum: Rep[Int]): Rep[Array[T]] = {
    val resultSize = keepNum
    val resultArray = __newArray[T](resultSize)
    val counter = __newVarNamed[Int](unit(0), "arrayCounter")
    Range(unit(0), resultSize).foreach {
      __lambda { i =>
        resultArray(i) = array(i)
      }
    }
    resultArray
  }

  def array_map[T: TypeRep, S: TypeRep](array: Rep[Array[T]], f: Rep[T => S]): Rep[Array[S]] = {
    val Def(Lambda(func, _, _)) = f
    val size = __newVarNamed[Int](unit(0), "arraySize")
    val resultArray = __newArray[S](array.length)
    val counter = __newVarNamed[Int](unit(0), "arrayCounter")
    array_foreach(array, (elem: Rep[T]) => {
      resultArray(readVar(counter)) = func(elem)
      __assign(counter, readVar(counter) + unit(1))
    })
    resultArray
  }

  def array_sum[T: TypeRep](array: Rep[Array[T]]): Rep[T] = {
    // TODO handle the generic case using numeric
    assert(typeRep[T] == DoubleType)
    val sumResult = __newVarNamed[Double](unit(0.0), "sumResult")
    array_foreach(array, (elem: Rep[T]) => {
      __assign(sumResult, readVar(sumResult) + elem.asInstanceOf[Rep[Double]])
    })
    readVar(sumResult).asInstanceOf[Rep[T]]
  }

  def array_avg[T: TypeRep](array: Rep[Array[T]]): Rep[T] = {
    // TODO handle the generic case using numeric
    assert(typeRep[T] == DoubleType)
    (array_sum(array).asInstanceOf[Rep[Double]] / array.length).asInstanceOf[Rep[T]]
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

  rewrite += rule {
    case QueryMap(monad, f) =>
      val array = apply(monad).asInstanceOf[Rep[Array[Any]]]
      array_map(array, f)(array.tp.typeArguments(0).asInstanceOf[TypeRep[Any]], f.tp.typeArguments(1).asInstanceOf[TypeRep[Any]])
  }

  rewrite += rule {
    case QueryForeach(monad, f) =>
      val array = apply(monad).asInstanceOf[Rep[Array[Any]]]
      val Def(Lambda(func, _, _)) = f
      array_foreach(array, func)(array.tp.typeArguments(0).asInstanceOf[TypeRep[Any]])
  }

  rewrite += rule {
    case QuerySum(monad) =>
      val array = apply(monad).asInstanceOf[Rep[Array[Any]]]
      array_sum(array)(array.tp.typeArguments(0).asInstanceOf[TypeRep[Any]])
  }

  rewrite += rule {
    case QueryCount(monad) =>
      val array = apply(monad).asInstanceOf[Rep[Array[Any]]]
      array.length
  }

  rewrite += rule {
    case QueryAvg(monad) =>
      val array = apply(monad).asInstanceOf[Rep[Array[Any]]]
      array_avg(array)(array.tp.typeArguments(0).asInstanceOf[TypeRep[Any]])
  }

  // rewrite += rule {
  //   case QuerySortBy(monad, f) =>
  //     val array = apply(monad).asInstanceOf[Rep[Array[Any]]]

  // }

  rewrite += remove {
    case QueryGroupBy(_, _) => ()
  }

  rewrite += rule {
    case GroupedQueryMapValues(Def(QueryGroupBy(monad, parNode)), f) =>
      class K
      class V
      class S
      implicit val typeK = parNode.tp.typeArguments(1).asInstanceOf[TypeRep[K]]
      implicit val typeV = parNode.tp.typeArguments(0).asInstanceOf[TypeRep[V]]
      implicit val typeS = f.tp.typeArguments(1).asInstanceOf[TypeRep[S]]
      // val Def(Lambda(func: (Rep[Array[V]] => Rep[S]), _, _)) = f
      // val Def(Lambda(par: (Rep[V] => Rep[K]), _, _)) = parNode
      val func = f.asInstanceOf[Rep[Array[V] => S]]
      val par = parNode.asInstanceOf[Rep[V => K]]
      // val Def(Lambda(par: (Rep[V] => Rep[K]), _, _)) = parNode
      val originalArray = apply(monad).asInstanceOf[Rep[Array[V]]]
      val MAX_SIZE = unit(4)
      val keyIndex = __newHashMap[K, Int]()
      val keyRevertIndex = __newArray[K](MAX_SIZE)
      val lastIndex = __newVarNamed(unit(0), "lastIndex")
      val array = __newArray[Array[V]](MAX_SIZE)
      val eachBucketSize = __newArray[Int](MAX_SIZE)
      Range(unit(0), MAX_SIZE).foreach {
        __lambda { i =>
          array(i) = __newArray[V](originalArray.length)
          eachBucketSize(i) = unit(0)
        }
      }

      // printf(unit("start!"))
      array_foreach(originalArray, (elem: Rep[V]) => {
        // val key = par(elem)
        val key = inlineFunction(par, elem)
        val bucket = keyIndex.getOrElseUpdate(key, {
          keyRevertIndex(readVar(lastIndex)) = key
          __assign(lastIndex, readVar(lastIndex) + unit(1))
          readVar(lastIndex) - unit(1)
        })
        array(bucket)(eachBucketSize(bucket)) = elem
        eachBucketSize(bucket) += unit(1)
      })
      val resultArray = __newArray[(K, S)](MAX_SIZE)
      // Range(unit(0), array.length).foreach {
      //   __lambda { i =>
      //     array(i) = __newArray[V](originalArray.length)
      //   }
      // }
      Range(unit(0), array.length).foreach {
        __lambda { i =>
          val arr = array_dropRight(array(i), eachBucketSize(i))
          // System.out.println(s"arr size ${arr.size} bucket size ${eachBucketSize(i)}")
          val key = keyRevertIndex(i)
          val newValue = inlineFunction(func, arr)
          resultArray(i) = Tuple2(key, newValue)
        }
      }
      resultArray.asInstanceOf[Rep[Any]]
  }

  // TODO needs quasi lifting legobase.
  rewrite += rule {
    case OptimalStringStartsWith(str, Def(GenericEngineParseStringObject(Constant(str2: String)))) =>
      str2.toCharArray.zipWithIndex.foldLeft(unit(true))((acc, curr) => acc && (str(curr._2) __== unit(curr._1)))
  }
}
