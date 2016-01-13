package ch.epfl.data
package dblab.legobase
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

// @reflect[Query[_]]
// @transformation
abstract class QueryIterator[T, Source] { self =>
  def source: Source
  def atEnd(s: Source): Boolean
  def next(s: Source): (T, Source)

  @pure def map[S](f: T => S): QueryIterator[S, Source] = new QueryIterator[S, Source] {
    def source = self.source
    def atEnd(s: Source): Boolean = self.atEnd(s)
    def next(s: Source): (S, Source) = self.next(s) match {
      case (e, sp) => (f(e), sp)
    }
  }
  @pure def filter(p: T => Boolean): QueryIterator[T, Source] = new QueryIterator[T, Source] {
    def zeroValue[TP]: TP = null.asInstanceOf[TP]
    var hd = zeroValue[T]
    var curTail = zeroValue[Source]
    var tmpAtEnd = false
    def source = self.source
    def atEnd(s: Source): Boolean = self.atEnd(s) || tmpAtEnd || {
      var tmpSource = s
      val nextAndRest = self.next(tmpSource)
      var tmpHd = nextAndRest._1
      var tmpRest = nextAndRest._2
      while (!(tmpAtEnd || p(tmpHd))) {
        tmpSource = tmpRest
        if (self.atEnd(tmpSource)) {
          tmpAtEnd = true
          tmpHd = zeroValue[T]
          tmpRest = zeroValue[Source]
        } else {
          val nextAndRest2 = self.next(tmpSource)
          tmpHd = nextAndRest2._1
          tmpRest = nextAndRest2._2
        }
      }
      hd = tmpHd
      curTail = tmpRest
      tmpAtEnd
    }
    def next(s: Source): (T, Source) = (hd, curTail)
  }
  // @pure def foldLeft[S](z: S)(f: (S, T) => S): S = {
  //   var res = z
  //   foreach(e => res = f(res, e))
  //   res
  // }
  def foreach(f: T => Unit): Unit = {
    var s = source
    while (!atEnd(s)) {
      val n = next(s)
      f(n._1)
      s = n._2
    }
  }

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
  @pure def groupBy[K](par: T => K): GroupedQueryIterator[K, T, Source] =
    new GroupedQueryIterator(this, par)
  // @pure def filteredGroupBy[K](pred: T => Boolean, par: T => K): GroupedQueryIterator[K, T] =
  //   filter(pred).groupBy(par)
  @dontLift def sortBy[S](f: T => S)(implicit ord: Ordering[S]): QueryIterator[T, Int] = new QueryIterator[T, Int] {
    val (treeSet, size) = {
      val treeSet = new TreeSet()(
        new Ordering[T] {
          def compare(o1: T, o2: T) = ord.compare(f(o1), f(o2))
        })
      self.foreach((elem: T) => {
        treeSet += elem
      })
      (treeSet, treeSet.size)
    }

    def source = 0

    def atEnd(s: Int): Boolean = s >= size
    def next(s: Int): (T, Int) = {
      val elem = treeSet.head
      treeSet -= elem
      Tuple2(elem, s + 1)
    }
  }

  // @pure def sortByReverse[S](f: T => S)(implicit ord: Ordering[S]): Query[T] =
  //   new Query(underlying.sortBy(f).reverse)

  // @pure def take(i: Int): QueryIterator[T] = (k: T => Unit) => {
  //   var count = 0
  //   foreach(e => {
  //     if (count < i) {
  //       k(e)
  //       count += 1
  //     }
  //   })
  // }

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
      var s = source
      while (!atEnd(s) && rows < limit) {
        val n = next(s)
        printFunc(n._1)
        s = n._2
        rows += 1
      }
    }
    printf("(%d rows)\n", rows)
  }

  // @pure def getList: List[T] = ???
}

object QueryIterator {
  // def apply[T](underlying: List[T]): QueryIterator[T] = new Query(underlying)
  @dontLift def apply[T](arr: Array[T]): QueryIterator[T, Int] = new QueryIterator[T, Int] {
    def source = 0

    def atEnd(ts: Int) = ts >= arr.length
    def next(ts: Int) = Tuple2(arr(ts), ts + 1)

  }
  def apply[T](set: scala.collection.mutable.Set[T]): SetIterator[T] = new SetIterator[T](set)
  // @dontLift implicit def apply[T](k: (T => Unit) => Unit): QueryIterator[T] = new QueryIterator[T] {
  //   def foreach(f: T => Unit): Unit = k(f)
  // }
}

class SetIterator[T](set: scala.collection.mutable.Set[T]) extends QueryIterator[T, Unit] { self =>
  var currentSet: scala.collection.mutable.Set[T] = set
  def source = ()

  def atEnd(ts: Unit) = currentSet.isEmpty
  def next(ts: Unit) = {
    val elem = currentSet.head
    currentSet = currentSet.tail
    Tuple2(elem, ())
  }

  def withFilter(p: T => Boolean): SetIterator[T] = new SetIterator[T](set) {
    val underlying = self.filter(p)
    override def atEnd(ts: Unit) = underlying.atEnd(())
    override def next(ts: Unit) = underlying.next(())
  }
}

// object JoinableQueryIterator {

// }

class JoinableQueryIterator[T <: Record, Source1](private val underlying: QueryIterator[T, Source1]) {
  def hashJoin[S <: Record, R, Source2](q2: QueryIterator[S, Source2])(leftHash: T => R)(rightHash: S => R)(
    joinCond: (T, S) => Boolean): QueryIterator[DynamicCompositeRecord[T, S], Source2] = new QueryIterator[DynamicCompositeRecord[T, S], Source2] {
    def source = q2.source

    val hm = MultiMap[R, T]
    for (elem <- underlying) {
      hm.addBinding(leftHash(elem), elem)
    }

    var nextSource: Source2 = null.asInstanceOf[Source2]
    var tmpAtEnd = false
    var iterator: SetIterator[T] = null
    var leftElem = null.asInstanceOf[T]
    var rightElem = null.asInstanceOf[S]

    def atEnd(ts: Source2) = /*q2.atEnd(ts) || */ tmpAtEnd || {
      if (iterator == null || iterator.atEnd(())) {
        var tmpSource = ts
        var leftElemFound = false
        while (!tmpAtEnd && !leftElemFound) {
          if (q2.atEnd(tmpSource)) {
            tmpAtEnd = true
          } else {
            val nextAndRest2 = q2.next(tmpSource)
            val tmpHd = nextAndRest2._1
            tmpSource = nextAndRest2._2
            val elem = tmpHd
            rightElem = elem
            val key = rightHash(elem)
            hm.get(key) foreach { tmpBuffer =>
              iterator = QueryIterator(tmpBuffer.filter(bufElem => joinCond(bufElem, elem)))
              //QueryIterator(tmpBuffer).withFilter(bufElem => joinCond(bufElem, elem))
              // println(s"set iterator $iterator")
              if (!iterator.atEnd(())) {
                leftElemFound = true
                leftElem = iterator.next(())._1
              }
            }
          }
        }
        // println(s"got left elem $leftElem")
        nextSource = tmpSource
        tmpAtEnd
      } else {
        // println(s"got left elem next $leftElem")
        leftElem = iterator.next(())._1
        false
      }
    }
    def next(ts: Source2) = leftElem.concatenateDynamic(rightElem) -> nextSource
  }

  def mergeJoin[S <: Record, Source2](q2: QueryIterator[S, Source2])(
    ord: (T, S) => Int)(joinCond: (T, S) => Boolean): QueryIterator[DynamicCompositeRecord[T, S], (Source1, Source2)] =
    new QueryIterator[DynamicCompositeRecord[T, S], (Source1, Source2)] {
      val q1 = underlying
      def source = (q1.source, q2.source)
      var ts1: Source1 = _
      var ts2: Source2 = _
      var nextJoinElem: DynamicCompositeRecord[T, S] = _
      var iterator: SetIterator[DynamicCompositeRecord[T, S]] = null
      // var tmpAtEnd = false // keeps if the two sources are at the end or not
      def atEnd(ts: (Source1, Source2)) = {
        ts1 = ts._1
        ts2 = ts._2
        q1.atEnd(ts1) || q2.atEnd(ts2) || {
          if (iterator == null || iterator.atEnd(())) {
            var found = false
            while (!q1.atEnd(ts1) && !q2.atEnd(ts2) && !found) {
              val ((ne1, ns1), (ne2, ns2)) = q1.next(ts1) -> q2.next(ts2)
              val cmp = ord(ne1, ne2)
              if (cmp < 0) {
                ts1 = ns1
              } else if (cmp > 0) {
                ts2 = ns2
              } else {
                val le = ne1
                val re = ne2
                // leftIndex += 1
                val leftBucket = ArrayBuffer[T]()
                var leftElem = le
                while (!q1.atEnd(ts1) && joinCond(leftElem, re)) {
                  leftBucket += leftElem
                  val (_1, _2) = q1.next(ts1)
                  leftElem = _1
                  ts1 = _2
                  // hitNumber += 1
                }

                // rightIndex += 1
                val rightBucket = ArrayBuffer[S]()
                var rightElem = re
                while (!q2.atEnd(ts2) && joinCond(le, rightElem)) {
                  rightBucket += rightElem
                  val (_1, _2) = q2.next(ts2)
                  rightElem = _1
                  ts2 = _2
                  // hitNumber += 1
                }
                // assert(hitNumber == 2)
                val res = scala.collection.mutable.Set[DynamicCompositeRecord[T, S]]()
                for (x1 <- leftBucket) {
                  for (x2 <- rightBucket) {
                    assert(joinCond(x1, x2))
                    res += x1.concatenateDynamic(x2)
                  }
                }
                found = true
                iterator = QueryIterator(res)
                nextJoinElem = iterator.next(())._1
              }
            }
            !found
          } else {
            nextJoinElem = iterator.next(())._1
            false
          }
        }
      }
      def next(ts: (Source1, Source2)) = {
        nextJoinElem -> (ts1 -> ts2)
      }
    }

  def leftHashSemiJoin[S <: Record, R, Source2](q2: QueryIterator[S, Source2])(leftHash: T => R)(rightHash: S => R)(
    joinCond: (T, S) => Boolean): QueryIterator[T, Source1] = new QueryIterator[T, Source1] {
    val hm = MultiMap[R, S]
    for (elem <- q2) {
      hm.addBinding(rightHash(elem), elem)
    }

    val leftIterator = underlying.filter(t => {
      val k = leftHash(t)
      hm.get(k).exists(buf =>
        buf.exists(e => joinCond(t, e)))
    })

    def source = leftIterator.source
    def atEnd(ts: Source1): Boolean = leftIterator.atEnd(ts)
    def next(ts: Source1) = leftIterator.next(ts)
    // for (elem <- underlying) {
    //   val key = leftHash(elem)
    //   hm.get(key) foreach { tmpBuffer =>
    //     if (tmpBuffer.exists(bufElem => joinCond(elem, bufElem)))
    //       k(elem)
    //   }
    // }
  }

  // def leftHashSemiJoin[S <: Record, R](q2: QueryIterator[S])(leftHash: T => R)(rightHash: S => R)(joinCond: (T, S) => Boolean): QueryIterator[T] = (k: T => Unit) => {
  //   val hm = MultiMap[R, S]
  //   for (elem <- q2) {
  //     hm.addBinding(rightHash(elem), elem)
  //   }
  //   for (elem <- underlying) {
  //     val key = leftHash(elem)
  //     hm.get(key) foreach { tmpBuffer =>
  //       if (tmpBuffer.exists(bufElem => joinCond(elem, bufElem)))
  //         k(elem)
  //     }
  //   }
  // }
}

case class GroupByResult[K, V](partitionedArray: Array[Array[V]], keyRevertIndex: Array[K],
                               eachBucketSize: Array[Int], partitions: Int, keyIndex: HashMap[K, Int])

class GroupedQueryIterator[K, V, Source1](underlying: QueryIterator[V, Source1], par: V => K) {
  def getGroupByResult: GroupByResult[K, V] = {
    // val max_partitions = 50
    // Q3
    // val max_partitions = 150000
    // Q9
    val max_partitions = 25 * 7
    val MAX_SIZE = max_partitions
    val keyIndex = new HashMap[K, Int]()
    val keyRevertIndex = new Array[Any](MAX_SIZE).asInstanceOf[Array[K]]
    var lastIndex = 0
    val array = new Array[Array[Any]](MAX_SIZE).asInstanceOf[Array[Array[V]]]
    val eachBucketSize = new Array[Int](MAX_SIZE)
    val thisSize = 1 << 25
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

  @pure def mapValues[S](func: QueryIterator[V, Int] => S): QueryIterator[(K, S), Int] = new QueryIterator[(K, S), Int] {

    val (groupByResult, partitions) = {
      val groupByResult = getGroupByResult
      val GroupByResult(array, keyRevertIndex, eachBucketSize, _, keyIndex) =
        groupByResult
      var lastIndex = 0

      underlying.foreach((elem: V) => {
        val key = par(elem)
        val bucket = keyIndex.getOrElseUpdate(key, {
          keyRevertIndex(lastIndex) = key
          lastIndex = lastIndex + 1
          lastIndex - 1
        })
        array(bucket)(eachBucketSize(bucket)) = elem
        eachBucketSize(bucket) += 1
      })
      (groupByResult, lastIndex)
    }

    def source: Int = 0

    def atEnd(s: Int): Boolean = s >= partitions
    def next(s: Int): ((K, S), Int) = {
      val GroupByResult(array, keyRevertIndex, eachBucketSize, _, _) =
        groupByResult
      val i = s
      val arr = array(i).dropRight(array(i).length - eachBucketSize(i))
      val key = keyRevertIndex(i)
      val newValue = func(QueryIterator(arr))
      Tuple2(Tuple2(key, newValue), s + 1)
    }
  }
}
