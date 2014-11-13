package ch.epfl.data
package legobase
package shallow
package c
package collections

import pardis.annotations._
import pardis.shallow.c.CLang._
import pardis.shallow.c.CLangTypes._
import pardis.shallow.c.LGListHeader._
import pardis.shallow.c.GLibTypes._

@metadeep(
  "legocompiler/src/main/scala/ch/epfl/data/legobase/deep",
  """
package ch.epfl.data
package legobase
package deep

import pardis.ir._
import pardis.types.PardisTypeImplicits._
import pardis.deep.scalalib._
import pardis.deep.scalalib.collection._
import pardis.shallow.c.CLangTypes._
import pardis.shallow.c.GLibTypes._
import pardis.optimization._
import CTypes._
""",
  "",
  "DeepDSL")
class MetaInfo2

// Manual changes required:
//  - replace this with self
//  - replace SetNew with SetNew2
//  - remove all implicit evidence
//  - specify type args for __app
//  - add implicit def ctp[T] = null.asInstanceOf[CType[T]]
//  - fix the constructor body

@deep
@reflect[scala.collection.mutable.Set[_]]
@transformation
class SetGLib[T: CType] {
  var gListHead: LPointer[LGList[T]] = NULL[LGList[T]]

  //def this() = {
  //  this(NULL[LGList[T]])
  //}

  def +=(x: T) = {
    gListHead = g_list_append(gListHead, &(x))
    this
  }

  def foreach(f: T => Unit): Unit = {
    var current = gListHead
    while (current != NULL[LGList[T]]) {
      val e = g_list_nth_data(current, 0)
      f(*(e))
      current = g_list_next(current)
    }
  }

  def retain(p: T => Boolean): Unit = {
    var current = gListHead
    while (current != NULL[LGList[T]]) {
      val e = g_list_nth_data(current, 0)
      val prev = current
      current = g_list_next(current)
      if (p(*(e))) gListHead = g_list_remove_link(gListHead, prev)
    }
  }
}

//@deep
//case class Cont[T](elem: T, var next: Cont[T])
//
//@deep
//@reflect[scala.collection.mutable.Set[_]]
//@transformation
//class SetLinkedList[T](var head: Cont[T]) {
//  def this() = this(null)
//
//  def +=(elem: T) = {
//    head = Cont(elem, head)
//  }
//
//  def foreach(f: T => Unit): Unit = {
//    var current = head
//    while (current != null) {
//      f(current.elem)
//      current = current.next
//    }
//  }
//
//  def retain(p: T => Boolean): Unit = {
//    var prev: Cont[T] = null
//    var current = head
//    while (current != null) {
//      //  if (p(current.elem)) {
//      //    if (prev == null) head = current.next
//      //    else prev.next = current.next
//      //  } else {
//      //    prev = current
//      //  }
//      //  current = current.next
//      //}
//
//      if (!p(current.elem)) {
//        if (head == current) head = null
//        if (prev != null) prev.next = null
//      } else {
//        if (prev != null) prev.next = current
//        if (head == null) head = current
//        prev = current
//      }
//      current = current.next
//    }
//  }
//}

//@deep
//@reflect[scala.collection.mutable.Set[_]]
//@transformation
//class SetArray[T: Manifest] {
//  //def this() = this(new Array[T](5), 0)
//  var array: Array[T] = new Array[T](5)
//
//  var size: Int = 0
//
//  def +=(elem: T) = {
//    array(size) = elem
//    size += 1
//  }
//
//  def foreach(f: T => Unit): Unit = {
//    for (i <- 0 until size) {
//      val elem = array(i)
//      f(elem)
//    }
//  }
//
//  def retain(p: T => Boolean): Unit = {
//    var lastIndex = 0
//    for (i <- 0 until size) {
//      val elem = array(i)
//      if (elem != null && p(elem)) {
//        array(i) = null.asInstanceOf[T]
//      } else {
//        array(lastIndex) = elem
//        lastIndex += 1
//      }
//    }
//    size = lastIndex
//  }
//}

