package ch.epfl.data
package legobase
package queryengine
package volcano

import scala.reflect.runtime.universe._
import scala.collection.mutable.HashMap
import scala.collection.mutable.Set
import GenericEngine._

abstract class Operator[+A: Manifest] {
  def open()
  def next(): A
  def close()
  def reset()
  def foreach(f: A => Unit) = {
    var exit = false
    while (exit != true) {
      val t = next
      if (t == NullDynamicRecord) exit = true
      else { f(t); () }
    }
  }
  def findFirst(cond: A => Boolean) = {
    var exit = false
    var res: A = NullDynamicRecord
    while (exit != true) {
      res = next
      if (res == NullDynamicRecord) exit = true
      else exit = cond(res)
    }
    res
  }

  val NullDynamicRecord = null.asInstanceOf[A]
  def NullDynamicRecord[D: Manifest] = null.asInstanceOf[D]
}

case class ScanOp[A: Manifest](table: Array[A]) extends Operator[A] {
  var i = 0
  def open() {}
  def next() = {
    if (i < table.length) {
      val v = table(i)
      i += 1
      v
    } else NullDynamicRecord
  }
  def close() = {}
  def reset() { i = 0 }
}

case class SelectOp[A: Manifest](parent: Operator[A])(selectPred: A => Boolean) extends Operator[A] {
  def open() = { parent.open; }
  def next() = parent findFirst selectPred
  def close() = {}
  def reset() { parent.reset }
}

case class AggOp[A: Manifest, B: Manifest](parent: Operator[A], numAggs: Int)(val grp: A => B)(val aggFuncs: Function2[A, Double, Double]*) extends Operator[AGGRecord[B]] {
  val mA = manifest[A]
  val mB = manifest[B]

  val hm = HashMap[B, Array[Double]]()
  var keySet = Set(hm.keySet.toSeq: _*)

  def open() {
    parent.open
    parent foreach { t: A =>
      val key = grp(t)
      val aggs = hm.getOrElseUpdate(key, new Array[Double](numAggs))
      var i: scala.Int = 0
      aggFuncs.foreach { aggFun =>
        aggs(i) = aggFun(t, aggs(i))
        i += 1
      }
    }
    keySet = Set(hm.keySet.toSeq: _*)
  }
  def next() = {
    if (hm.size != 0) {
      val key = keySet.head
      keySet.remove(key)
      val elem = hm.remove(key)
      newAGGRecord(key, elem.get)
    } else NullDynamicRecord
  }
  def close() {}
  def reset() { parent.reset; hm.clear; open }
}

case class SortOp[A: Manifest](parent: Operator[A])(orderingFunc: Function2[A, A, Int]) extends Operator[A] {
  val sortedTree = new scala.collection.mutable.TreeSet()(
    new Ordering[A] {
      def compare(o1: A, o2: A) = orderingFunc(o1, o2)
    })
  def open() = {
    parent.open
    parent.foreach { t: A => sortedTree += t }
  }
  def next() = {
    if (sortedTree.size != 0) {
      val elem = sortedTree.head
      sortedTree -= elem
      elem
    } else NullDynamicRecord
  }
  def close() = {}
  def reset() { parent.reset; open }
}

// Limited version of map -- touches data in place and does not create new data
case class MapOp[A: Manifest](parent: Operator[A])(aggFuncs: Function1[A, Unit]*) extends Operator[A] {
  def open() = { parent.open; }
  def next() = {
    val t: A = parent.next
    if (t != NullDynamicRecord) {
      aggFuncs foreach (agg => agg(t))
      t
    } else NullDynamicRecord
  }
  def close() = {}
  def reset { parent.reset }
}

case class PrintOp[A: Manifest](var parent: Operator[A])(printFunc: A => Unit, limit: () => Boolean = () => true) extends Operator[A] {
  var numRows = 0
  def open() = { parent.open; }
  def next() = {
    var exit = false
    while (exit == false) {
      val t = parent.next
      if (limit() == false || t == NullDynamicRecord) exit = true
      else { printFunc(t); numRows += 1 }
    }
    NullDynamicRecord
  }
  def close() = {}
  def reset() { parent.reset }
}
