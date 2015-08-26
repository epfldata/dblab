package ch.epfl.data
package dblab.legobase
package queryengine
package monad

import sc.pardis.annotations.{ deep, noImplementation, needsCircular, dontLift, needs, reflect, pure }
import sc.pardis.shallow.{ Record, DynamicCompositeRecord }
import push.MultiMap
import scala.collection.mutable.MultiMap
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.TreeSet

abstract class QueryCPS[T] { self =>
  @pure def map[S](f: T => S): QueryCPS[S] =
    new QueryCPS[S] {
      def foreach(fe: S => Unit): Unit = self.foreach(e => fe(f(e)))
    }
  @pure def filter(p: T => Boolean): QueryCPS[T] =
    new QueryCPS[T] {
      def foreach(fe: T => Unit): Unit = self.foreach(e => if (p(e)) fe(e))
    }
  // @pure def foldLeft[S](z: S)(f: (S, T) => S): S =
  //   underlying.foldLeft(z)(f)
  def foreach(f: T => Unit): Unit
  @pure def sum(implicit num: Numeric[T]): T = {
    var res = num.zero
    self.foreach(e => res = num.plus(res, e))
    res
  }
  @pure def count: Int = {
    var size = 0
    self.foreach(e => size += 1)
    size
  }
  @pure def avg(implicit num: Fractional[T]): T =
    num.div(sum, num.fromInt(count))
  @pure def groupBy[K](par: T => K): GroupedQueryCPS[K, T] =
    new GroupedQueryCPS(this, par)
  // @pure def filteredGroupBy[K](pred: T => Boolean, par: T => K): GroupedQuery[K, T] =
  //   new GroupedQuery(underlying.filter(pred).groupBy(par))
  // def groupByMapValues[K, S](par: T => K)(f: Query[T] => S): Query[(K, T)] = {
  //   groupBy(par).mapValues(f)
  // }
  @pure def sortBy[S](f: T => S)(implicit ord: Ordering[S]): QueryCPS[T] =
    new QueryCPS[T] {
      def foreach(fe: T => Unit): Unit = {
        val sortedTree = new TreeSet()(
          new Ordering[T] {
            def compare(o1: T, o2: T) = ord.compare(f(o1), f(o2))
          })
        self.foreach(e => sortedTree += e)
        while (sortedTree.size != 0) {
          val elem = sortedTree.head
          sortedTree -= elem
          fe(elem)
        }
      }
    }

  // @pure def sortByReverse[S](f: T => S)(implicit ord: Ordering[S]): Query[T] =
  //   new Query(underlying.sortBy(f).reverse)

  @pure def take(i: Int): QueryCPS[T] =
    new QueryCPS[T] {
      def foreach(fe: T => Unit): Unit = {
        var count = 0
        self.foreach(e => {
          if (count < i) {
            fe(e)
            count += 1
          }
        })
      }
    }

  // @pure def minBy[S](f: T => S)(implicit ord: Ordering[S]): T =
  //   underlying.minBy(f)

  // @pure def getList: List[T] = ???
}

object QueryCPS {
  // def apply[T](underlying: List[T]): QueryCPS[T] = new Query(underlying)
  def apply[T](underlying: Array[T]): QueryCPS[T] = new QueryCPS[T] {
    def foreach(f: T => Unit): Unit = underlying.foreach(f)
  }
}

object JoinableQueryCPS {

}

class JoinableQueryCPS[T <: Record](private val underlying: QueryCPS[T]) {
  def hashJoin[S <: Record, R](q2: QueryCPS[S])(leftHash: T => R)(rightHash: S => R)(joinCond: (T, S) => Boolean): QueryCPS[DynamicCompositeRecord[T, S]] = {
    new QueryCPS[DynamicCompositeRecord[T, S]] {
      def foreach(f: DynamicCompositeRecord[T, S] => Unit): Unit = {
        val hm = MultiMap[R, T]
        for (elem <- underlying) {
          hm.addBinding(leftHash(elem), elem)
        }
        for (elem <- q2) {
          val k = rightHash(elem)
          hm.get(k) foreach { tmpBuffer =>
            tmpBuffer foreach { bufElem =>
              if (joinCond(bufElem, elem)) {
                val newElem = bufElem.concatenateDynamic(elem)
                f(newElem)
              }
            }
          }
        }
      }
    }
  }

  // def leftHashSemiJoin[S <: Record, R](q2: Query[S])(leftHash: T => R)(rightHash: S => R)(joinCond: (T, S) => Boolean): Query[T] = {
  //   val res = ArrayBuffer[T]()
  //   val hm = MultiMap[R, S]
  //   for (elem <- q2.getList) {
  //     hm.addBinding(rightHash(elem), elem)
  //   }
  //   for (elem <- underlying) {
  //     val k = leftHash(elem)
  //     hm.get(k) foreach { tmpBuffer =>
  //       if (tmpBuffer.exists(bufElem => joinCond(elem, bufElem)))
  //         res += elem
  //     }
  //   }
  //   new Query(res.toList)
  // }
}

class GroupedQueryCPS[K, V](underlying: QueryCPS[V], par: V => K) {
  @pure def mapValues[S](f: QueryCPS[V] => S): QueryCPS[(K, S)] =
    new QueryCPS[(K, S)] {
      def foreach(fe: ((K, S)) => Unit): Unit = {
        val hm = MultiMap[K, V]
        for (elem <- underlying) {
          hm.addBinding(par(elem), elem)
        }
        hm.foreach {
          case (key, set) =>
            val value = f(QueryCPS(set.asInstanceOf[scala.collection.mutable.HashSet[Any]].toArray.asInstanceOf[Array[V]]))
            fe((key, value))
        }
      }
    }
}
