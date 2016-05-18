package ch.epfl.data
package dblab
package queryengine
package monad

import sc.pardis.annotations.{ deep, needsCircular, dontLift, needs, reflect, pure, transformation }
import sc.pardis.shallow.{ Record, DynamicCompositeRecord }
import push.MultiMap
import scala.collection.mutable.MultiMap
import scala.collection.mutable.HashMap
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.TreeSet
import scala.language.implicitConversions
import Stream.buildS

// sealed trait Stream[+T] {
//   def map[S](f: T => S): Stream[S] = flatMap(x => Stream(f(x)))
//   def filter(p: T => Boolean): Stream[T] = flatMap(x => if (p(x)) Stream(x) else Skip)
//   def flatMap[S](f: T => Stream[S]): Stream[S] = semiFold[Stream[S]](() => Done, () => Skip, f)
//   def foreach(f: T => Unit): Unit = semiFold[Unit](() => (), () => (), f)
//   def semiFold[S](done: () => S, skip: () => S, f: T => S): S = this match {
//     case Done     => done()
//     case Skip     => skip()
//     case Yield(v) => f(v)
//   }
// }

case object Done extends Stream[Nothing]
case object Skip extends Stream[Nothing]
case class Yield[+T](value: T) extends Stream[T]
case class BuildStream[+T](builder: (() => _, () => _, T => _) => _) extends Stream[T]

sealed trait Stream[+T] { self =>
  def map[S](f: T => S): Stream[S] =
    buildS { (done, skip, f1) =>
      semiFold(done, skip, x => f1(f(x)))
    }
  def filter(p: T => Boolean): Stream[T] =
    buildS { (done, skip, f1) =>
      semiFold(done, skip, x => if (p(x)) f1(x) else skip())
    }
  def flatMap[S](f: T => Stream[S]): Stream[S] =
    buildS { (done, skip, yld) =>
      semiFold(done, skip, x => f(x).semiFold(done, skip, yld))
    }
  def foreach(f: T => Unit): Unit = semiFold[Unit](() => (), () => (), f)
  def materialize(): Stream[T] = semiFold(() => Done, () => Skip, Yield[T])
  def semiFold[S](done: () => S, skip: () => S, f: T => S): S =
    this match {
      case Done                 => done()
      case Skip                 => skip()
      case Yield(v)             => f(v)
      case BuildStream(builder) => builder(done, skip, f).asInstanceOf[S]
    }
}

object Stream {
  def apply[T](v: T): Stream[T] = Yield(v)
  def buildS[T](builder: (() => _, () => _, T => _) => _): Stream[T] = BuildStream(builder)
  // def buildS[T](builder: (() => _, () => _, T => _) => _): Stream[T] =
  //   builder.asInstanceOf[(() => Stream[T], () => Stream[T], T => Stream[T]) => Stream[T]](() => Done, () => Skip, e => Yield(e))
}

// @reflect[Query[_]]
// @transformation
abstract class QueryStream[T] { self =>
  def stream(): Stream[T]
  // def next(): Stream[T] = {
  //   var done = false
  //   var result: Stream[T] = Skip
  //   while (!done) {
  //     stream().semiFold(() => {
  //       done = true
  //       result = Done
  //     }, () => (), e => {
  //       done = true
  //       result = Yield(e)
  //     })
  //   }
  //   result
  // }
  def reset(): Unit
  @pure def map[S](f: T => S): QueryStream[S] = unstream { () =>
    stream().map(f)
  }
  @pure def filter(p: T => Boolean): QueryStream[T] = unstream { () =>
    stream().filter(p)
  }
  def foreach(f: T => Unit): Unit = {
    reset()
    var done = false
    while (!done) {
      stream().semiFold(() => done = true, () => (), f)
    }
  }

  @pure def foldLeft[S](z: S)(f: (S, T) => S): S = {
    var res = z
    for (e <- this) {
      res = f(res, e)
    }
    res
  }

  @pure def sum(implicit num: Numeric[T]): T = {
    var res = num.zero
    for (e <- this) {
      res = num.plus(res, e)
    }
    res
  }
  @pure def count: Int = {
    var size = 0
    for (e <- this) {
      size += 1
    }
    size
  }
  @pure def avg(implicit num: Fractional[T]): T =
    num.div(sum, num.fromInt(count))
  @pure def groupBy[K](par: T => K): GroupedQueryStream[K, T] =
    new GroupedQueryStream(this, par)
  @pure def filteredGroupBy[K](pred: T => Boolean, par: T => K): GroupedQueryStream[K, T] =
    filter(pred).groupBy(par)

  def sortBy[S](f: T => S)(implicit ord: Ordering[S]): QueryStream[T] = {
    val (treeSet, size) = {
      val treeSet = new TreeSet()(
        new Ordering[T] {
          def compare(o1: T, o2: T) = {
            val res = ord.compare(f(o1), f(o2))
            if (res == 0 && o1 != o2) {
              -1
            } else {
              res
            }
          }
        })
      for (elem <- this) {
        treeSet += elem
      }
      (treeSet, treeSet.size)
    }
    var index = 0
    unstream { () =>
      buildS { (done, skip, yld) =>
        if (index >= size)
          done()
        else {
          index += 1
          val elem = treeSet.head
          treeSet -= elem
          yld(elem)
        }
      }
    }
  }

  // // @pure def sortByReverse[S](f: T => S)(implicit ord: Ordering[S]): Query[T] =
  // //   new Query(underlying.sortBy(f).reverse)

  // // @pure def take(i: Int): QueryStream[T] = (k: T => Unit) => {
  // //   var count = 0
  // //   foreach(e => {
  // //     if (count < i) {
  // //       k(e)
  // //       count += 1
  // //     }
  // //   })
  // // }

  @pure def minBy[S](f: T => S)(implicit ord: Ordering[S]): T = {
    var minResult: T = null.asInstanceOf[T]
    for (e <- this) {
      if (minResult == null || ord.compare(f(minResult), f(e)) > 0) {
        minResult = e
      }
    }
    minResult
  }

  def printRows(printFunc: T => Unit, limit: Int): Unit = {
    var rows = 0
    if (limit == -1) {
      for (e <- this) {
        printFunc(e)
        rows += 1
      }
    } else {
      var done = false
      while (rows < limit && !done) {
        stream().semiFold({ () =>
          done = true
        }, { () =>
          ()
        }, { e =>
          printFunc(e)
          rows += 1
        })
      }

    }
    printf("(%d rows)\n", rows)
  }

  @pure def materialize: QueryStream[T] = {
    val arr = getList.asInstanceOf[List[Any]].toArray.asInstanceOf[Array[T]]
    QueryStream(arr)
  }

  @pure def getList: List[T] = {
    var res = ArrayBuffer[T]()
    for (e <- this) {
      res += e
    }
    res.toList
  }

  def unstream[T](n: () => Stream[T]): QueryStream[T] = new QueryStream[T] {
    def stream(): Stream[T] = n()
    def reset(): Unit = self.reset()
  }
}

object QueryStream {
  @dontLift def apply[T](arr: Array[T]): QueryStream[T] = new QueryStream[T] {
    var index = 0
    def stream(): Stream[T] = buildS { (done, skip, fun) =>
      if (index >= arr.length)
        done()
      else {
        index += 1
        fun(arr(index - 1))
      }
    }
    def reset(): Unit = {
      index = 0
    }
  }
  def apply[T](set: scala.collection.mutable.Set[T]): SetStream[T] = new SetStream[T](set)
}

class SetStream[T](set: scala.collection.mutable.Set[T]) extends QueryStream[T] { self =>
  var currentSet: scala.collection.mutable.Set[T] = set

  def reset() = currentSet = set
  def stream() = buildS { (done, skip, fun) =>
    if (currentSet.isEmpty) {
      done()
    } else {
      val elem = currentSet.head
      currentSet = currentSet.tail
      fun(elem)
    }
  }

  def withFilter(p: T => Boolean): SetStream[T] = new SetStream[T](set) {
    val underlying = self.filter(p)
    override def stream() = underlying.stream()
  }
}

class JoinableQueryStream[T <: Record](private val underlying: QueryStream[T]) {
  def hashJoin[S <: Record, R](q2: QueryStream[S])(leftHash: T => R)(rightHash: S => R)(
    joinCond: (T, S) => Boolean): QueryStream[DynamicCompositeRecord[T, S]] = {
    val hm = MultiMap[R, T]
    for (elem <- underlying) {
      hm.addBinding(leftHash(elem), elem)
    }

    q2.unstream { () =>
      buildS { (done, skip, yld) =>
        q2.stream().semiFold(
          () => done(),
          () => skip(),
          t => {
            val k = rightHash(t)
            var elem1: T = null.asInstanceOf[T]
            val found = hm.get(k) exists { tmpBuffer =>
              val leftElem = tmpBuffer find (bufElem => joinCond(bufElem, t))
              // Only to check if it is not the N-M case
              if (tmpBuffer.filter(bufElem => joinCond(bufElem, t)).size > 1) {
                throw new Exception("This join is for the N-M case")
              }
              leftElem match {
                case Some(le) =>
                  elem1 = le
                  true
                case None =>
                  false
              }
            }
            if (found)
              yld(elem1.concatenateDynamic(t))
            else
              skip()
          })
      }
    }
  }

  def mergeJoin[S <: Record](q2: QueryStream[S])(
    ord: (T, S) => Int)(joinCond: (T, S) => Boolean): QueryStream[DynamicCompositeRecord[T, S]] = {
    var elem1: Stream[T] = null
    var elem2: Stream[S] = null
    var atEnd: Boolean = false
    var leftShouldProceed: Boolean = false
    var init: Boolean = false
    underlying.unstream { () =>
      if (leftShouldProceed || !init) {
        elem1 = underlying.stream().materialize()
      }
      if (!leftShouldProceed || !init) {
        elem2 = q2.stream().materialize()
      }
      init = true
      buildS { (done, skip, yld) =>
        if (atEnd) {
          done()
        } else {
          leftShouldProceed = false
          elem1.semiFold(
            () => {
              atEnd = true
              done()
            },
            () => {
              leftShouldProceed = true
              skip()
            },
            ne1 => {
              elem2.semiFold(
                () => {
                  atEnd = true
                  done()
                },
                () => {
                  skip()
                },
                ne2 => {
                  val cmp = ord(ne1, ne2)
                  if (cmp < 0) {
                    leftShouldProceed = true
                    skip()
                  } else {
                    if (cmp == 0) {
                      yld(ne1.concatenateDynamic(ne2))
                    } else {
                      skip()
                    }
                  }
                })
            })
        }
      }
    }
  }

  def leftHashSemiJoin[S <: Record, R](q2: QueryStream[S])(leftHash: T => R)(rightHash: S => R)(
    joinCond: (T, S) => Boolean): QueryStream[T] = {
    val hm = MultiMap[R, S]
    for (elem <- q2) {
      hm.addBinding(rightHash(elem), elem)
    }
    val leftIterator = underlying.filter(t => {
      val k = leftHash(t)
      hm.get(k).exists(buf =>
        buf.exists(e => joinCond(t, e)))
    })

    underlying.unstream { () =>
      leftIterator.stream()
    }
  }
}

class GroupedQueryStream[K, V](underlying: QueryStream[V], par: V => K) {
  def getGroupByResult: GroupByResult[K, V] = {
    val max_partitions = 50
    // Q3
    // val max_partitions = 150000
    // Q9
    // val max_partitions = 25 * 7
    val MAX_SIZE = max_partitions
    val keyIndex = new HashMap[K, Int]()
    val keyRevertIndex = new Array[Any](MAX_SIZE).asInstanceOf[Array[K]]
    var lastIndex = 0
    val array = new Array[Array[Any]](MAX_SIZE).asInstanceOf[Array[Array[V]]]
    val eachBucketSize = new Array[Int](MAX_SIZE)
    // val thisSize = 1 << 25
    val thisSize = 1 << 22
    val arraySize = thisSize / MAX_SIZE * 8
    Range(0, MAX_SIZE).foreach { i =>
      // val arraySize = originalArray.length
      // val arraySize = 128
      array(i) = new Array[Any](arraySize).asInstanceOf[Array[V]] // discovered a funny scalac bug!
      eachBucketSize(i) = 0
    }
    GroupByResult(array, keyRevertIndex, eachBucketSize, MAX_SIZE, keyIndex)
    // ???
  }

  @pure def mapValues[S](func: QueryStream[V] => S): QueryStream[(K, S)] = {

    val (groupByResult, partitions) = {
      val groupByResult = getGroupByResult
      val GroupByResult(array, keyRevertIndex, eachBucketSize, _, keyIndex) =
        groupByResult
      var lastIndex = 0

      for (elem <- underlying) {
        val key = par(elem)
        val bucket = keyIndex.getOrElseUpdate(key, {
          keyRevertIndex(lastIndex) = key
          lastIndex = lastIndex + 1
          lastIndex - 1
        })
        array(bucket)(eachBucketSize(bucket)) = elem
        eachBucketSize(bucket) += 1
      }
      (groupByResult, lastIndex)
    }

    var index: Int = 0

    underlying.unstream { () =>
      buildS { (done, skip, yld) =>
        if (index >= partitions) {
          done()
        } else {
          val GroupByResult(array, keyRevertIndex, eachBucketSize, _, _) =
            groupByResult
          val i = index
          index += 1
          val arr = array(i).dropRight(array(i).length - eachBucketSize(i))
          val key = keyRevertIndex(i)
          val newValue = func(QueryStream(arr))
          yld(key -> newValue)
        }
      }
    }
  }
}
