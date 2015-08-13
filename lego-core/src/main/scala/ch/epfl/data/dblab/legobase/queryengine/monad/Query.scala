package ch.epfl.data
package dblab.legobase
package queryengine
package monad

import sc.pardis.annotations.{ deep, noImplementation, needsCircular, dontLift, needs, reflect, pure }
import sc.pardis.shallow.{ Record, DynamicCompositeRecord }
import push.MultiMap
import scala.collection.mutable.MultiMap
import scala.collection.mutable.ArrayBuffer

@deep
@noImplementation
@needs[(List[_], Array[_])]
@needsCircular[GroupedQuery[_, _]]
class Query[T](private val underlying: List[T]) {
  def this(arr: Array[T]) = this(arr.toList)
  @pure def map[S](f: T => S): Query[S] =
    new Query(underlying.map(f))
  @pure def filter(p: T => Boolean): Query[T] =
    new Query(underlying.filter(p))
  @pure def foldLeft[S](z: S)(f: (S, T) => S): S =
    underlying.foldLeft(z)(f)
  def foreach(f: T => Unit): Unit =
    underlying.foreach(f)
  @pure def sum(implicit num: Numeric[T]): T =
    underlying.sum
  @pure def count: Int =
    underlying.size
  @pure def avg(implicit num: Fractional[T]): T =
    num.div(sum, num.fromInt(count))
  @pure def groupBy[K](par: T => K): GroupedQuery[K, T] =
    new GroupedQuery(underlying.groupBy(par))
  @pure def filteredGroupBy[K](pred: T => Boolean, par: T => K): GroupedQuery[K, T] =
    new GroupedQuery(underlying.filter(pred).groupBy(par))
  // def groupByMapValues[K, S](par: T => K)(f: Query[T] => S): Query[(K, T)] = {
  //   groupBy(par).mapValues(f)
  // }
  @pure def sortBy[S](f: T => S)(implicit ord: Ordering[S]): Query[T] =
    new Query(underlying.sortBy(f))

  @pure def sortByReverse[S](f: T => S)(implicit ord: Ordering[S]): Query[T] =
    new Query(underlying.sortBy(f).reverse)

  @pure def take(i: Int): Query[T] =
    new Query(underlying.take(i))

  @pure def minBy[S](f: T => S)(implicit ord: Ordering[S]): T =
    underlying.minBy(f)

  @pure def getList: List[T] = underlying
}

@deep
@noImplementation
@needs[(Query[_], List[_])]
@needsCircular[GroupedQuery[_, _]]
class JoinableQuery[T <: Record](private val underlying: List[T]) {
  def hashJoin[S <: Record, R](q2: Query[S])(leftHash: T => R)(rightHash: S => R)(joinCond: (T, S) => Boolean): Query[DynamicCompositeRecord[T, S]] = {
    /* Naive implementation */
    // new Query(underlying.flatMap(e1 =>
    //   q2.getList.flatMap(e2 =>
    //     if (joinCond(e1, e2))
    //       List(e1.concatenateDynamic(e2))
    //     else
    //       Nil)))
    /* Implementation using MultiMap */
    val res = ArrayBuffer[DynamicCompositeRecord[T, S]]()
    val hm = MultiMap[R, T]
    for (elem <- underlying) {
      hm.addBinding(leftHash(elem), elem)
    }
    for (elem <- q2.getList) {
      val k = rightHash(elem)
      hm.get(k) foreach { tmpBuffer =>
        tmpBuffer foreach { bufElem =>
          if (joinCond(bufElem, elem)) {
            res += bufElem.concatenateDynamic(elem)
          }
        }
      }
    }
    new Query(res.toList)
  }
}

@deep
@noImplementation
@needs[(Tuple2[_, _], Query[_], Map[_, _], List[_])]
class GroupedQuery[K, V](private val underlying: Map[K, List[V]]) {
  @pure def mapValues[S](f: Query[V] => S): Query[(K, S)] =
    new Query(underlying.mapValues(l => f(new Query(l))).toList)
}
