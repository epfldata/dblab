package ch.epfl.data
package dblab
package queryengine
package monad

import sc.pardis.annotations.{ deep, needsCircular, dontLift, needs, reflect }
import push.MultiMap
import scala.collection.mutable.MultiMap
import scala.collection.mutable.HashMap

class QueryOptimized[T: Manifest](val array: Array[T]) {
  def map[S: Manifest](f: T => S): QueryOptimized[S] = {
    val size = array.size
    val newArray = new Array[S](size)
    var i = 0
    while (i < size) {
      newArray(i) = f(array(i))
      i += 1
    }
    new QueryOptimized(newArray)
    // (new QueryOptimized(array.map(f)))
  }
  def filter(p: T => Boolean): QueryOptimized[T] =
    new QueryOptimized(array.filter(p))
  def foreach(f: T => Unit): Unit =
    array.foreach(f)
  def sum(implicit num: Numeric[T]): T =
    array.sum
  def count: Int =
    array.size
  def avg(implicit num: Fractional[T]): T =
    num.div(sum, num.fromInt(count))
  def groupBy[K: Manifest](par: T => K): GroupedQueryOptimized1[K, T] = {
    val hm = MultiMap[K, T]
    for (elem <- array) {
      hm.addBinding(par(elem), elem)
    }
    new GroupedQueryOptimized1(hm)
  }
  // def groupBy[K: Manifest](par: T => K): GroupedQueryOptimized2[K, T] = {
  //   new GroupedQueryOptimized2(this, par)
  // }

  def sortBy[S](f: T => S)(implicit ord: Ordering[S]): QueryOptimized[T] =
    new QueryOptimized(array.sortBy(f))
}

class GroupedQueryOptimized1[K: Manifest, V: Manifest](val hm: MultiMap[K, V]) {
  def mapValues[S: Manifest](f: QueryOptimized[V] => S): QueryOptimized[(K, S)] = {
    val array = new Array[(K, S)](hm.size)
    var i = 0
    hm.foreach {
      case (key, set) =>
        val value = f(new QueryOptimized(set.toArray))
        array(i) = key -> value
        i += 1
    }
    new QueryOptimized(array)
  }
}

class GroupedQueryOptimized2[K: Manifest, V: Manifest](val query: QueryOptimized[V], val par: V => K) {
  def mapValues[S: Manifest](f: QueryOptimized[V] => S): QueryOptimized[(K, S)] = {
    val MAX_SIZE = 4
    val keyIndex = new HashMap[K, Int]
    var lastIndex = 0
    val array = Array.ofDim[V](MAX_SIZE, query.array.size)
    val eachBucketSize = new Array[Int](MAX_SIZE)
    for (elem <- query.array) {
      val key = par(elem)
      val bucket = keyIndex.getOrElseUpdate(key, {
        lastIndex += 1
        lastIndex - 1
      })
      array(bucket)(eachBucketSize(bucket)) = elem
      eachBucketSize(bucket) += 1
    }
    val resultArray = new Array[(K, S)](MAX_SIZE)
    for (i <- 0 until array.size) {
      val arr = array(i).dropRight(query.array.size - eachBucketSize(i))
      // System.out.println(s"arr size ${arr.size} bucket size ${eachBucketSize(i)}")
      val key = keyIndex.find(_._2 == i).get._1
      resultArray(i) = key -> f(new QueryOptimized(arr))
    }
    new QueryOptimized(resultArray)
  }

  // def mapValues[S: Manifest](f: QueryOptimized[V] => S): QueryOptimized[(K, S)] = {
  //   val MAX_SIZE = 4
  //   val keyIndex = new HashMap[K, Int]
  //   var lastIndex = 0
  //   val array = new Array[Array[V]](MAX_SIZE)
  //   val eachBucketSize = new Array[Int](MAX_SIZE)
  //   for (elem <- query.array) {
  //     val key = par(elem)
  //     val bucket = keyIndex.getOrElseUpdate(key, {
  //       lastIndex += 1
  //       lastIndex - 1
  //     })
  //     array(bucket)(eachBucketSize(bucket)) = elem
  //     eachBucketSize(bucket) += 1
  //   }
  //   val resultArray = new Array[(K, S)](MAX_SIZE)
  //   for (i <- 0 until array.size) {
  //     val arr = array(i).dropRight(query.array.size - eachBucketSize(i))
  //     // System.out.println(s"arr size ${arr.size} bucket size ${eachBucketSize(i)}")
  //     val key = keyIndex.find(_._2 == i).get._1
  //     resultArray(i) = key -> f(new QueryOptimized(arr))
  //   }
  //   new QueryOptimized(resultArray)
  // }
}
