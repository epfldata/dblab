package ch.epfl.data
package dblab.legobase
package queryengine
package monad

import sc.pardis.annotations.{ deep, noImplementation, needsCircular, dontLift, needs, reflect }

@deep
@noImplementation
@needs[List[_]]
@needsCircular[GroupedQuery[_, _]]
class Query[T](private val underlying: List[T]) {
  def map[S](f: T => S): Query[S] =
    new Query(underlying.map(f))
  def filter(p: T => Boolean): Query[T] =
    new Query(underlying.filter(p))
  def foreach(f: T => Unit): Unit =
    underlying.foreach(f)
  def sum(implicit num: Numeric[T]): T =
    underlying.sum
  def count: Int =
    underlying.size
  def avg(implicit num: Fractional[T]): T =
    num.div(sum, num.fromInt(count))
  def groupBy[K](par: T => K): GroupedQuery[K, T] =
    new GroupedQuery(underlying.groupBy(par))
  def sortBy[S](f: T => S)(implicit ord: Ordering[S]): Query[T] =
    new Query(underlying.sortBy(f))
}

// HACK
@deep
@reflect[List[_]]
class MList[A]

@deep
@reflect[Map[_, _]]
class MMap[A, B]

@deep
@noImplementation
@needs[(Tuple2[_, _], Query[_], Map[_, _], List[_])]
class GroupedQuery[K, V](private val underlying: Map[K, List[V]]) {
  def mapValues[S](f: Query[V] => S): Query[(K, S)] =
    new Query(underlying.mapValues(l => f(new Query(l))).toList)
}
