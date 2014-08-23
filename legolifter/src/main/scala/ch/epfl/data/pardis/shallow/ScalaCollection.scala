package scala
package collection
package mutable

import ch.epfl.data._
import autolifter.annotations._
import generic._

@reflect[HashMap[_, _]]
class MirrorHashMap[A, B](contents: HashTable.Contents[A, DefaultEntry[A, B]]) {
  def this() = this(null)
  def getOrElseUpdate(key: A, op: => B): B = ???
  @write
  def clear(): Unit = ???
  @pure
  def size: Int = ???
  @pure
  def contains(key: A): Boolean = ???
  @pure
  def apply(key: A): B = ???
  @write
  def update(key: A, value: B): Unit = ???
  @write
  def remove(key: A): Option[B] = ???
  @pure
  def keySet: Set[A] = ???
}

@reflect[Set[_]]
trait MirrorSet[A] {
  @pure
  def head: A = ???
  @pure
  def apply(elem: A): Boolean = ???
  @pure
  def toSeq: Seq[A] = ???
  @write
  def remove(elem: A): Boolean = ???
}

object MirrorSet {
  def apply[T](elems: Seq[T]): Set[T] = ???
  def apply[T](): Set[T] = ???
}

@reflect[TreeSet[_]]
class MirrorTreeSet[A]()( /*implicit */ val ordering: Ordering[A]) {
  @pure
  def head: A = ???
  @pure
  def size: Int = ???
  @write
  def -=(elem: A): TreeSet[A] = ???
  @write
  def +=(elem: A): TreeSet[A] = ???
}

@reflect[DefaultEntry[_, _]]
final class MirrorDefaultEntry[A, B](val key: A, var value: B) {
  def chainString: String = ???
}

@reflect[ArrayBuffer[_]]
final class MirrorArrayBuffer[A](protected val initialSize: Int) {
  def this() = this(16)
  @pure def size: Int = ???
  @pure def apply(i: Int): A = ???
  @write def update(i: Int, x: A): Unit = ???
  def indexWhere(p: A => Boolean): Int = ???
  def clear(): Unit = ???
  def minBy[B](f: A => B)(implicit cmp: Ordering[B]): A = ???
  def foldLeft[B](z: B)(op: (B, A) => B): B = ???
  @write def append(elem: A): Unit = ???
}

object MirrorArrayBuffer {
  def apply[T](): MirrorArrayBuffer[T] = ???
}