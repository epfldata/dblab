// package scala
// package collection
// package mutable

// import ch.epfl.data._
// import autolifter.annotations._
// import generic._

// @reflect[HashMap[_, _]]
// class MirrorHashMap[A, B](contents: HashTable.Contents[A, DefaultEntry[A, B]]) {
//   def this() = this(null)
//   def getOrElseUpdate(key: A, op: => B): B = ???
//   @write
//   def clear(): Unit = ???
//   @read
//   def size: Int = ???
//   @read
//   def contains(key: A): Boolean = ???
//   @read
//   def apply(key: A): B = ???
//   @write
//   def update(key: A, value: B): Unit = ???
//   @write
//   def remove(key: A): Option[B] = ???
//   @pure
//   def keySet: Set[A] = ???
// }

// @reflect[Set[_]]
// trait MirrorSet[A] {
//   @read
//   def head: A = ???
//   @read
//   def apply(elem: A): Boolean = ???
//   @read
//   def toSeq: Seq[A] = ???
//   @write
//   def remove(elem: A): Boolean = ???
//   @read
//   def +(elem: A): Set[A] = ???
//   @write
//   def +=(elem: A): Set[A] = ???
// }

// object MirrorSet {
//   def apply[T](elems: Seq[T]): Set[T] = ???
//   def apply[T](): Set[T] = ???
// }

// @reflect[TreeSet[_]]
// class MirrorTreeSet[A]()( /*implicit */ val ordering: Ordering[A]) {
//   @read
//   def head: A = ???
//   @read
//   def size: Int = ???
//   @write
//   def -=(elem: A): TreeSet[A] = ???
//   @write
//   def +=(elem: A): TreeSet[A] = ???
// }

// @reflect[DefaultEntry[_, _]]
// final class MirrorDefaultEntry[A, B](val key: A, var value: B) {
//   def chainString: String = ???
// }

// @reflect[ArrayBuffer[_]]
// final class MirrorArrayBuffer[A](protected val initialSize: Int) {
//   def this() = this(16)
//   @read
//   def size: Int = ???
//   @read
//   def apply(i: Int): A = ???
//   @write def update(i: Int, x: A): Unit = ???
//   def indexWhere(p: A => Boolean): Int = ???
//   @write
//   def clear(): Unit = ???
//   def minBy[B](f: A => B)(implicit cmp: Ordering[B]): A = ???
//   def foldLeft[B](z: B)(op: (B, A) => B): B = ???
//   @write def append(elem: A): Unit = ???
//   @write def remove(n: Int): A = ???
//   @read
//   def isEmpty: Boolean = ???
// }

// object MirrorArrayBuffer {
//   def apply[T](): MirrorArrayBuffer[T] = ???
// }

// @reflect[Range]
// class MirrorRange(val start: Int, val end: Int, val step: Int) {
//   def foreach[U](f: Int => U): Unit = ???
// }
