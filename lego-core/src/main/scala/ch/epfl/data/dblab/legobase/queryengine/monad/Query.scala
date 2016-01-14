package ch.epfl.data
package dblab.legobase
package queryengine
package monad

import sc.pardis.annotations.{ deep, needsCircular, dontLift, needs, reflect, pure, direct, :: }
import sc.pardis.shallow.{ Record, DynamicCompositeRecord }
import push.MultiMap
import scala.collection.mutable.MultiMap
import scala.collection.mutable.ArrayBuffer

@deep
@needs[List[_] :: Array[_]]
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

  // for verification purposes
  @dontLift def isSortedBy[S](f: T => S)(implicit ord: Ordering[S]): Boolean = {
    underlying match {
      case Nil => true
      case hd :: tail =>
        var current = hd
        var rest = tail
        while (rest.nonEmpty) {
          val tmp = rest.head
          if (ord.compare(f(current), f(tmp)) > 0) {
            // println(s"caused by $current and $tmp")
            return false
          }
          current = tmp
          rest = rest.tail
        }
        true
    }
  }

  @pure def sortByReverse[S](f: T => S)(implicit ord: Ordering[S]): Query[T] =
    new Query(underlying.sortBy(f).reverse)

  @pure def take(i: Int): Query[T] =
    new Query(underlying.take(i))

  @pure def minBy[S](f: T => S)(implicit ord: Ordering[S]): T =
    underlying.minBy(f)

  @pure def getList: List[T] = underlying

  def printRows(printFunc: T => Unit, limit: Int): Unit = {
    var rows = 0
    if (limit == -1) {
      for (e <- this) {
        // printf(format, elems.map(_(e)): _*)
        printFunc(e)
        rows += 1
      }
    } else {
      while (rows < limit && rows < underlying.size) {
        val e = underlying(rows)
        // printf(format, elems.map(_(e)): _*)
        printFunc(e)
        rows += 1
      }
    }
    printf("(%d rows)\n", rows)
  }
}

object Query {
  @direct def apply[T](underlying: List[T]): Query[T] = new Query(underlying)
  @direct def apply[T](underlying: Array[T]): Query[T] = new Query(underlying)
}

@deep
@needs[Query[_] :: List[_]]
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
  def mergeJoin[S <: Record](q2: Query[S])(ord: (T, S) => Int)(joinCond: (T, S) => Boolean): Query[DynamicCompositeRecord[T, S]] = {
    val res = ArrayBuffer[DynamicCompositeRecord[T, S]]()
    val leftArr = underlying.asInstanceOf[List[Record]].toArray.asInstanceOf[Array[T]]
    val leftSize = leftArr.length
    var leftIndex = 0
    def leftElem = leftArr(leftIndex)
    val rightArr = q2.getList.asInstanceOf[List[Record]].toArray.asInstanceOf[Array[S]]
    val rightSize = rightArr.length
    var rightIndex = 0
    def rightElem = rightArr(rightIndex)
    while (leftIndex < leftSize && rightIndex < rightSize) {
      val cmp = ord(leftElem, rightElem)
      if (cmp < 0) {
        leftIndex += 1
      } else if (cmp > 0) {
        rightIndex += 1
      } else {
        // assert(joinCond(leftElem, rightElem))
        // res += leftElem.concatenateDynamic(rightElem)
        // leftIndex += 1
        // var hitNumber = 0
        // rightIndex += 1
        val le = leftElem
        val re = rightElem
        // leftIndex += 1
        val leftBucket = ArrayBuffer[T]()
        while (leftIndex < leftSize && joinCond(leftElem, re)) {
          leftBucket += leftElem
          leftIndex += 1
          // hitNumber += 1
        }

        // rightIndex += 1
        val rightBucket = ArrayBuffer[S]()
        while (rightIndex < rightSize && joinCond(le, rightElem)) {
          rightBucket += rightElem
          rightIndex += 1
          // hitNumber += 1
        }
        // assert(hitNumber == 2)
        for (x1 <- leftBucket) {
          for (x2 <- rightBucket) {
            assert(joinCond(x1, x2))
            res += x1.concatenateDynamic(x2)
          }
        }
      }
    }
    new Query(res.toList)
  }

  def leftHashSemiJoin[S <: Record, R](q2: Query[S])(leftHash: T => R)(rightHash: S => R)(joinCond: (T, S) => Boolean): Query[T] = {
    val res = ArrayBuffer[T]()
    val hm = MultiMap[R, S]
    for (elem <- q2.getList) {
      hm.addBinding(rightHash(elem), elem)
    }
    for (elem <- underlying) {
      val k = leftHash(elem)
      hm.get(k) foreach { tmpBuffer =>
        if (tmpBuffer.exists(bufElem => joinCond(elem, bufElem)))
          res += elem
      }
    }
    new Query(res.toList)
  }
}

@deep
@needs[Tuple2[_, _] :: Query[_] :: Map[_, _] :: List[_]]
class GroupedQuery[K, V](private val underlying: Map[K, List[V]]) {
  @pure def mapValues[S](f: Query[V] => S): Query[(K, S)] =
    new Query(underlying.mapValues(l => f(new Query(l))).toList)
}
