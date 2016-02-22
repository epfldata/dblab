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
import QueryUnfold.destroy

// @reflect[Query[_]]
// @transformation
abstract class QueryUnfold[T] { self =>
  def NULL[S]: S = null.asInstanceOf[S]
  def next(): T

  @pure def map[S](f: T => S): QueryUnfold[S] = destroy { () =>
    val elem = next()
    if (elem == NULL)
      NULL
    else
      f(elem)
  }
  @pure def filter(p: T => Boolean): QueryUnfold[T] = destroy { () =>
    var elem: T = NULL
    var found = false
    while (!found && {
      elem = next()
      elem
    } != NULL) {
      if (p(elem)) {
        found = true
      }
    }
    elem
  }
  // @pure def filter(p: T => Boolean): QueryUnfold[T, Source] = new QueryUnfold[T, Source] {
  //   def zeroValue[TP]: TP = null.asInstanceOf[TP]
  //   var hd = zeroValue[T]
  //   var curTail = zeroValue[Source]
  //   var tmpAtEnd = false
  //   def source = self.source
  //   def atEnd(s: Source): Boolean = self.atEnd(s) || tmpAtEnd || {
  //     var tmpSource = s
  //     val nextAndRest = self.next(tmpSource)
  //     var tmpHd = nextAndRest._1
  //     var tmpRest = nextAndRest._2
  //     while (!(tmpAtEnd || p(tmpHd))) {
  //       tmpSource = tmpRest
  //       if (self.atEnd(tmpSource)) {
  //         tmpAtEnd = true
  //         tmpHd = zeroValue[T]
  //         tmpRest = zeroValue[Source]
  //       } else {
  //         val nextAndRest2 = self.next(tmpSource)
  //         tmpHd = nextAndRest2._1
  //         tmpRest = nextAndRest2._2
  //       }
  //     }
  //     hd = tmpHd
  //     curTail = tmpRest
  //     tmpAtEnd
  //   }
  //   def next(s: Source): (T, Source) = (hd, curTail)
  // }
  // @pure def foldLeft[S](z: S)(f: (S, T) => S): S = {
  //   var res = z
  //   foreach(e => res = f(res, e))
  //   res
  // }
  def foreach(f: T => Unit): Unit = {
    var elem: T = NULL
    while ({
      elem = next()
      elem
    } != NULL) {
      f(elem)
    }
  }

  @pure def sum(implicit num: Numeric[T]): T = {
    var res = num.zero
    for (e <- this) {
      res = num.plus(res, e)
    }
    res
  }
  // @pure def count: Int = {
  //   var size = 0
  //   foreach(e => size += 1)
  //   size
  // }
  // @pure def avg(implicit num: Fractional[T]): T =
  //   num.div(sum, num.fromInt(count))
  // @pure def groupBy[K](par: T => K): GroupedQueryUnfold[K, T, Source] =
  //   new GroupedQueryUnfold(this, par)
  // // @pure def filteredGroupBy[K](pred: T => Boolean, par: T => K): GroupedQueryUnfold[K, T] =
  // //   filter(pred).groupBy(par)
  // @dontLift def sortBy[S](f: T => S)(implicit ord: Ordering[S]): QueryUnfold[T, Int] = new QueryUnfold[T, Int] {
  //   val (treeSet, size) = {
  //     val treeSet = new TreeSet()(
  //       new Ordering[T] {
  //         def compare(o1: T, o2: T) = {
  //           val res = ord.compare(f(o1), f(o2))
  //           if (res == 0 && o1 != o2) {
  //             -1
  //           } else {
  //             res
  //           }
  //         }
  //       })
  //     // var count = 0
  //     self.foreach((elem: T) => {
  //       treeSet += elem
  //       // count += 1
  //     })
  //     // println(count)
  //     (treeSet, treeSet.size)
  //   }

  //   def source = 0

  //   // var lastIndex = -1
  //   // var lastElem: T = _
  //   // println(size)

  //   def atEnd(s: Int): Boolean = s >= size
  //   def next(s: Int): (T, Int) = {
  //     // if (lastIndex != s) {
  //     val elem = treeSet.head
  //     treeSet -= elem
  //     // lastElem = elem
  //     // lastIndex = s
  //     Tuple2(elem, s + 1)
  //     // } else {
  //     //   Tuple2(lastElem, s + 1)
  //     // }
  //   }
  // }

  // @pure def sortByReverse[S](f: T => S)(implicit ord: Ordering[S]): Query[T] =
  //   new Query(underlying.sortBy(f).reverse)

  // @pure def take(i: Int): QueryUnfold[T] = (k: T => Unit) => {
  //   var count = 0
  //   foreach(e => {
  //     if (count < i) {
  //       k(e)
  //       count += 1
  //     }
  //   })
  // }

  // @pure def minBy[S](f: T => S)(implicit ord: Ordering[S]): T = {
  //   var minResult: T = null.asInstanceOf[T]
  //   foreach(e => {
  //     if (minResult == null || ord.compare(f(minResult), f(e)) > 0) {
  //       minResult = e
  //     }
  //   })
  //   minResult
  // }

  // def printRows(printFunc: T => Unit, limit: Int): Unit = {
  //   var rows = 0
  //   if (limit == -1) {
  //     for (e <- this) {
  //       // printf(format, elems.map(_(e)): _*)
  //       printFunc(e)
  //       rows += 1
  //     }
  //   } else {
  //     var s = source
  //     while (!atEnd(s) && rows < limit) {
  //       val n = next(s)
  //       printFunc(n._1)
  //       s = n._2
  //       rows += 1
  //     }
  //   }
  //   printf("(%d rows)\n", rows)
  // }

  // @pure def getList: List[T] = {
  //   var res = ArrayBuffer[T]()
  //   for (e <- this) {
  //     res += e
  //   }
  //   res.toList
  // }
}

object QueryUnfold {
  @dontLift def apply[T](arr: Array[T]): QueryUnfold[T] = new QueryUnfold[T] {
    var index = 0
    def next(): T =
      if (index >= arr.length)
        NULL
      else {
        index += 1
        arr(index - 1)
      }
  }
  def destroy[T](n: () => T): QueryUnfold[T] = new QueryUnfold[T] {
    def next(): T = n()
  }
  // def apply[T](set: scala.collection.mutable.Set[T]): SetIterator[T] = new SetIterator[T](set)
}

// class SetIterator[T](set: scala.collection.mutable.Set[T]) extends QueryUnfold[T, Unit] { self =>
//   var currentSet: scala.collection.mutable.Set[T] = set
//   def source = ()

//   def atEnd(ts: Unit) = currentSet.isEmpty
//   def next(ts: Unit) = {
//     val elem = currentSet.head
//     currentSet = currentSet.tail
//     Tuple2(elem, ())
//   }

//   def withFilter(p: T => Boolean): SetIterator[T] = new SetIterator[T](set) {
//     val underlying = self.filter(p)
//     override def atEnd(ts: Unit) = underlying.atEnd(())
//     override def next(ts: Unit) = underlying.next(())
//   }
// }

// class JoinableQueryUnfold[T <: Record, Source1](private val underlying: QueryUnfold[T, Source1]) {
//   def hashJoin[S <: Record, R, Source2](q2: QueryUnfold[S, Source2])(leftHash: T => R)(rightHash: S => R)(
//     joinCond: (T, S) => Boolean): QueryUnfold[DynamicCompositeRecord[T, S], Source2] = new QueryUnfold[DynamicCompositeRecord[T, S], Source2] {
//     def source = q2.source

//     val hm = MultiMap[R, T]
//     for (elem <- underlying) {
//       hm.addBinding(leftHash(elem), elem)
//     }

//     var nextSource: Source2 = null.asInstanceOf[Source2]
//     var tmpAtEnd = false
//     var iterator: SetIterator[T] = null
//     var leftElem = null.asInstanceOf[T]
//     var rightElem = null.asInstanceOf[S]

//     def atEnd(ts: Source2) = /*q2.atEnd(ts) || */ tmpAtEnd || {
//       if (iterator == null || iterator.atEnd(())) {
//         var tmpSource = ts
//         var leftElemFound = false
//         while (!tmpAtEnd && !leftElemFound) {
//           if (q2.atEnd(tmpSource)) {
//             tmpAtEnd = true
//           } else {
//             val nextAndRest2 = q2.next(tmpSource)
//             val tmpHd = nextAndRest2._1
//             tmpSource = nextAndRest2._2
//             val elem = tmpHd
//             rightElem = elem
//             val key = rightHash(elem)
//             hm.get(key) foreach { tmpBuffer =>
//               iterator = QueryUnfold(tmpBuffer.filter(bufElem => joinCond(bufElem, elem)))
//               //QueryUnfold(tmpBuffer).withFilter(bufElem => joinCond(bufElem, elem))
//               // println(s"set iterator $iterator")
//               if (!iterator.atEnd(())) {
//                 leftElemFound = true
//                 leftElem = iterator.next(())._1
//               }
//             }
//           }
//         }
//         // println(s"got left elem $leftElem")
//         nextSource = tmpSource
//         tmpAtEnd
//       } else {
//         // println(s"got left elem next $leftElem")
//         leftElem = iterator.next(())._1
//         false
//       }
//     }
//     def next(ts: Source2) = leftElem.concatenateDynamic(rightElem) -> nextSource
//   }

//   def mergeJoin[S <: Record, Source2](q2: QueryUnfold[S, Source2])(
//     ord: (T, S) => Int)(joinCond: (T, S) => Boolean): QueryUnfold[DynamicCompositeRecord[T, S], (Source1, Source2)] =
//     new QueryUnfold[DynamicCompositeRecord[T, S], (Source1, Source2)] {
//       val q1 = underlying
//       def source = (q1.source, q2.source)
//       var ts1: Source1 = _
//       var ts2: Source2 = _
//       var elem1: T = _
//       var elem2: S = _
//       var leftEnd: Boolean = false
//       var rightEnd: Boolean = false
//       var nextJoinElem: DynamicCompositeRecord[T, S] = _
//       var tmpAtEnd = false // keeps if the two sources are at the end or not
//       def proceedLeft(): Unit = {
//         if (q1.atEnd(ts1)) {
//           leftEnd = true
//         } else {
//           val (ne1, ns1) = q1.next(ts1)
//           elem1 = ne1
//           ts1 = ns1
//         }
//       }
//       def proceedRight(): Unit = {
//         if (q2.atEnd(ts2)) {
//           rightEnd = true
//         } else {
//           val (ne2, ns2) = q2.next(ts2)
//           elem2 = ne2
//           ts2 = ns2
//         }
//       }
//       def atEnd(ts: (Source1, Source2)) = {
//         tmpAtEnd || {
//           if (elem1 == null && elem2 == null) {
//             ts1 = q1.source
//             ts2 = q2.source
//             proceedLeft()
//             proceedRight()
//           }
//           var found = false
//           while (!tmpAtEnd && !found) {
//             if (leftEnd || rightEnd) {
//               tmpAtEnd = true
//             } else {
//               val (ne1, ne2) = elem1 -> elem2
//               val cmp = ord(ne1, ne2)
//               if (cmp < 0) {
//                 proceedLeft()
//               } else if (cmp > 0) {
//                 proceedRight()
//               } else {
//                 val le = ne1
//                 val re = ne2
//                 nextJoinElem = le.concatenateDynamic(re)
//                 proceedRight()
//                 found = true
//               }
//             }
//           }
//           !found
//         }
//       }
//       def next(ts: (Source1, Source2)) = {
//         nextJoinElem -> (ts1 -> ts2)
//       }
//     }

//   def leftHashSemiJoin[S <: Record, R, Source2](q2: QueryUnfold[S, Source2])(leftHash: T => R)(rightHash: S => R)(
//     joinCond: (T, S) => Boolean): QueryUnfold[T, Source1] = new QueryUnfold[T, Source1] {
//     val hm = MultiMap[R, S]
//     for (elem <- q2) {
//       hm.addBinding(rightHash(elem), elem)
//     }

//     val leftIterator = underlying.filter(t => {
//       val k = leftHash(t)
//       hm.get(k).exists(buf =>
//         buf.exists(e => joinCond(t, e)))
//     })

//     def source = leftIterator.source
//     def atEnd(ts: Source1): Boolean = leftIterator.atEnd(ts)
//     def next(ts: Source1) = leftIterator.next(ts)
//   }
// }

// case class GroupByResult[K, V](partitionedArray: Array[Array[V]], keyRevertIndex: Array[K],
//                                eachBucketSize: Array[Int], partitions: Int, keyIndex: HashMap[K, Int])

// class GroupedQueryUnfold[K, V, Source1](underlying: QueryUnfold[V, Source1], par: V => K) {
//   def getGroupByResult: GroupByResult[K, V] = {
//     // val max_partitions = 50
//     // Q3
//     // val max_partitions = 150000
//     // Q9
//     val max_partitions = 25 * 7
//     val MAX_SIZE = max_partitions
//     val keyIndex = new HashMap[K, Int]()
//     val keyRevertIndex = new Array[Any](MAX_SIZE).asInstanceOf[Array[K]]
//     var lastIndex = 0
//     val array = new Array[Array[Any]](MAX_SIZE).asInstanceOf[Array[Array[V]]]
//     val eachBucketSize = new Array[Int](MAX_SIZE)
//     val thisSize = 1 << 25
//     val arraySize = thisSize / MAX_SIZE * 8
//     Range(0, MAX_SIZE).foreach { i =>
//       // val arraySize = originalArray.length
//       // val arraySize = 128
//       array(i) = new Array[Any](arraySize).asInstanceOf[Array[V]] // discovered a funny scalac bug!
//       eachBucketSize(i) = 0
//     }
//     GroupByResult(array, keyRevertIndex, eachBucketSize, MAX_SIZE, keyIndex)
//     // ???
//   }

//   @pure def mapValues[S](func: QueryUnfold[V, Int] => S): QueryUnfold[(K, S), Int] = new QueryUnfold[(K, S), Int] {

//     val (groupByResult, partitions) = {
//       val groupByResult = getGroupByResult
//       val GroupByResult(array, keyRevertIndex, eachBucketSize, _, keyIndex) =
//         groupByResult
//       var lastIndex = 0

//       underlying.foreach((elem: V) => {
//         val key = par(elem)
//         val bucket = keyIndex.getOrElseUpdate(key, {
//           keyRevertIndex(lastIndex) = key
//           lastIndex = lastIndex + 1
//           lastIndex - 1
//         })
//         array(bucket)(eachBucketSize(bucket)) = elem
//         eachBucketSize(bucket) += 1
//       })
//       (groupByResult, lastIndex)
//     }

//     def source: Int = 0

//     def atEnd(s: Int): Boolean = s >= partitions
//     def next(s: Int): ((K, S), Int) = {
//       val GroupByResult(array, keyRevertIndex, eachBucketSize, _, _) =
//         groupByResult
//       val i = s
//       val arr = array(i).dropRight(array(i).length - eachBucketSize(i))
//       val key = keyRevertIndex(i)
//       val newValue = func(QueryUnfold(arr))
//       Tuple2(Tuple2(key, newValue), s + 1)
//     }
//   }
// }
