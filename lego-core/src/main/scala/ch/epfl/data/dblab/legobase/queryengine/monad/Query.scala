package ch.epfl.data
package dblab.legobase
package queryengine
package monad

import sc.pardis.annotations.{ deep, noImplementation, needsCircular, dontLift, needs, reflect, pure }

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
}

@deep
@noImplementation
@needs[(Tuple2[_, _], Query[_], Map[_, _], List[_])]
class GroupedQuery[K, V](private val underlying: Map[K, List[V]]) {
  @pure def mapValues[S](f: Query[V] => S): Query[(K, S)] =
    new Query(underlying.mapValues(l => f(new Query(l))).toList)
}
