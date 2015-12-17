package ch.epfl.data
package dblab.legobase
package queryengine
package monad

import sc.pardis.annotations.{ deep, needsCircular, dontLift, needs, reflect, pure, transformation }
import sc.pardis.shallow.{ Record, DynamicCompositeRecord }
import push.MultiMap
import scala.collection.mutable.MultiMap
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.TreeSet
import scala.language.implicitConversions

// @reflect[Query[_]]
// @transformation
abstract class QueryCPS[T] {
  @pure def map[S](f: T => S): QueryCPS[S] = (k: S => Unit) => {
    foreach(e => k(f(e)))
  }
  @pure def filter(p: T => Boolean): QueryCPS[T] = (k: T => Unit) => {
    foreach(e => if (p(e)) k(e))
  }
  @pure def foldLeft[S](z: S)(f: (S, T) => S): S = {
    var res = z
    foreach(e => res = f(res, e))
    res
  }
  def foreach(f: T => Unit): Unit
  @pure def sum(implicit num: Numeric[T]): T = {
    var res = num.zero
    foreach(e => res = num.plus(res, e))
    res
  }
  @pure def count: Int = {
    var size = 0
    foreach(e => size += 1)
    size
  }
  @pure def avg(implicit num: Fractional[T]): T =
    num.div(sum, num.fromInt(count))
  @pure def groupBy[K](par: T => K): GroupedQueryCPS[K, T] =
    new GroupedQueryCPS(this, par)
  @pure def filteredGroupBy[K](pred: T => Boolean, par: T => K): GroupedQueryCPS[K, T] =
    filter(pred).groupBy(par)
  // def groupByMapValues[K, S](par: T => K)(f: Query[T] => S): Query[(K, T)] = {
  //   groupBy(par).mapValues(f)
  // }
  @dontLift def sortBy[S](f: T => S)(implicit ord: Ordering[S]): QueryCPS[T] = (k: T => Unit) => {
    val sortedTree = new TreeSet()(
      new Ordering[T] {
        def compare(o1: T, o2: T) = ord.compare(f(o1), f(o2))
      })
    foreach(e => sortedTree += e)
    while (sortedTree.size != 0) {
      val elem = sortedTree.head
      sortedTree -= elem
      k(elem)
    }
  }

  // @pure def sortByReverse[S](f: T => S)(implicit ord: Ordering[S]): Query[T] =
  //   new Query(underlying.sortBy(f).reverse)

  @pure def take(i: Int): QueryCPS[T] = (k: T => Unit) => {
    var count = 0
    foreach(e => {
      if (count < i) {
        k(e)
        count += 1
      }
    })
  }

  @pure def minBy[S](f: T => S)(implicit ord: Ordering[S]): T = {
    var minResult: T = null.asInstanceOf[T]
    foreach(e => {
      if (minResult == null || ord.compare(f(minResult), f(e)) > 0) {
        minResult = e
      }
    })
    minResult
  }

  def printRows(printFunc: T => Unit, limit: Int): Unit = {
    var rows = 0
    if (limit == -1) {
      for (e <- this) {
        // printf(format, elems.map(_(e)): _*)
        printFunc(e)
        rows += 1
      }
    } else {
      for (e <- this) {
        if (rows < limit) {
          printFunc(e)
          rows += 1
        }
      }
    }
    printf("(%d rows)\n", rows)
  }

  // @pure def getList: List[T] = ???
}

object QueryCPS {
  // def apply[T](underlying: List[T]): QueryCPS[T] = new Query(underlying)
  @dontLift def apply[T](underlying: Array[T]): QueryCPS[T] = (k: T => Unit) => {
    underlying.foreach(k)
  }
  @dontLift implicit def apply[T](k: (T => Unit) => Unit): QueryCPS[T] = new QueryCPS[T] {
    def foreach(f: T => Unit): Unit = k(f)
  }
}

object JoinableQueryCPS {

}

class JoinableQueryCPS[T <: Record](private val underlying: QueryCPS[T]) {
  def hashJoin[S <: Record, R](q2: QueryCPS[S])(leftHash: T => R)(rightHash: S => R)(joinCond: (T, S) => Boolean): QueryCPS[DynamicCompositeRecord[T, S]] = (k: DynamicCompositeRecord[T, S] => Unit) => {
    val hm = MultiMap[R, T]
    for (elem <- underlying) {
      hm.addBinding(leftHash(elem), elem)
    }
    for (elem <- q2) {
      val key = rightHash(elem)
      hm.get(key) foreach { tmpBuffer =>
        tmpBuffer foreach { bufElem =>
          if (joinCond(bufElem, elem)) {
            val newElem = bufElem.concatenateDynamic(elem)
            k(newElem)
          }
        }
      }
    }
  }

  def leftHashSemiJoin[S <: Record, R](q2: QueryCPS[S])(leftHash: T => R)(rightHash: S => R)(joinCond: (T, S) => Boolean): QueryCPS[T] = (k: T => Unit) => {
    val hm = MultiMap[R, S]
    for (elem <- q2) {
      hm.addBinding(rightHash(elem), elem)
    }
    for (elem <- underlying) {
      val key = leftHash(elem)
      hm.get(key) foreach { tmpBuffer =>
        if (tmpBuffer.exists(bufElem => joinCond(elem, bufElem)))
          k(elem)
      }
    }
  }
}

class GroupedQueryCPS[K, V](underlying: QueryCPS[V], par: V => K) {
  @pure def mapValues[S](f: QueryCPS[V] => S): QueryCPS[(K, S)] = (k: ((K, S)) => Unit) => {
    val hm = MultiMap[K, V]
    for (elem <- underlying) {
      hm.addBinding(par(elem), elem)
    }
    hm.foreach {
      case (key, set) =>
        val value = f(QueryCPS(set.asInstanceOf[scala.collection.mutable.HashSet[Any]].toArray.asInstanceOf[Array[V]]))
        k((key, value))
    }
  }
}
