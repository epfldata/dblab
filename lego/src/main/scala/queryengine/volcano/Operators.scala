package ch.epfl.data
package legobase
package queryengine
package volcano

import scala.reflect.runtime.universe._
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap
import scala.collection.mutable.Set
import GenericEngine._
import ch.epfl.data.autolifter.annotations.{ deep, metadeep }

// This is a temporary solution until we introduce dependency management and adopt policies. Not a priority now!
@metadeep(
  "legocompiler/src/main/scala/ch/epfl/data/legobase/deep",
  """
package ch.epfl.data
package legobase
package deep

import scalalib._
import pardis.ir._
""",
  """OperatorsComponent""")
class MetaInfo

@deep abstract class Operator[+A: Manifest] {
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

@deep case class ScanOp[A: Manifest](table: Array[A]) extends Operator[A] {
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

@deep case class SelectOp[A: Manifest](parent: Operator[A])(selectPred: A => Boolean) extends Operator[A] {
  def open() = { parent.open; }
  def next() = parent findFirst selectPred
  def close() = {}
  def reset() { parent.reset }
}

@deep case class AggOp[A: Manifest, B: Manifest](parent: Operator[A], numAggs: Int)(val grp: A => B)(val aggFuncs: Function2[A, Double, Double]*) extends Operator[AGGRecord[B]] {
  val mA = manifest[A]
  val mB = manifest[B]

  val hm = scala.collection.mutable.HashMap[B, Array[Double]]()
  var keySet = scala.collection.mutable.Set(hm.keySet.toSeq: _*)

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
    keySet = scala.collection.mutable.Set(hm.keySet.toSeq: _*)
  }
  def next() = {
    if (hm.size != 0) {
      val key = keySet.head
      keySet.remove(key)
      val elem = hm.remove(key)
      ch.epfl.data.legobase.queryengine.GenericEngine.newAGGRecord(key, elem.get)
    } else NullDynamicRecord
  }
  def close() {}
  def reset() { parent.reset; hm.clear; open }
}

@deep case class SortOp[A: Manifest](parent: Operator[A])(orderingFunc: Function2[A, A, Int]) extends Operator[A] {
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
@deep case class MapOp[A: Manifest](parent: Operator[A])(aggFuncs: Function1[A, Unit]*) extends Operator[A] {
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

@deep case class PrintOp[A: Manifest](var parent: Operator[A])(printFunc: A => Unit, limit: () => Boolean = () => true) extends Operator[A] {
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
/*
case class HashJoinOp[A: Manifest, B: Manifest, C: Manifest](val leftParent: Operator[A], val rightParent: Operator[B], leftAlias: String = "", rightAlias: String = "")(val joinCond: (A, B) => Boolean)(val leftHash: A => C)(val rightHash: B => C) extends Operator[CompositeRecord[A, B]] {
  var tmpCount = -1
  var tmpBuffer = ArrayBuffer[A]()
  val hm = HashMap[C, ArrayBuffer[A]]()
  var tmpLine = NullDynamicRecord[B]

  def open() = {
    leftParent.open
    rightParent.open
    leftParent foreach { t =>
      {
        val k = leftHash(t)
        val v = hm.getOrElseUpdate(k, ArrayBuffer[A]())
        v.append(t)
      }
    }
  }
  def next() = {
    if (tmpCount != -1) {
      while (tmpCount < tmpBuffer.size && !joinCond(tmpBuffer(tmpCount), tmpLine)) tmpCount += 1
      if (tmpCount == tmpBuffer.size) tmpCount = -1
    }
    if (tmpCount == -1) {
      tmpLine = rightParent findFirst { t =>
        {
          val k = rightHash(t)
          if (hm.contains(k)) {
            tmpBuffer = hm(k)
            tmpCount = tmpBuffer.indexWhere(e => joinCond(e, t))
            tmpCount != -1
          } else false
        }
      }
    }
    if (tmpLine != NullDynamicRecord[B] && tmpCount != -1) {
      val res = tmpBuffer(tmpCount).concatenate(tmpLine, leftAlias, rightAlias)
      tmpCount += 1
      res
    } else NullDynamicRecord[CompositeRecord[A, B]]
  }
  def close() {}
  def reset() { rightParent.reset; leftParent.reset; hm.clear; tmpLine = NullDynamicRecord[B]; tmpCount = 0; tmpBuffer.clear }
}

case class WindowOp[A: Manifest, B: Manifest, C: Manifest](parent: Operator[A])(val grp: Function1[A, B])(val wndf: ArrayBuffer[A] => C) extends Operator[Record { val key: B; val wnd: C }] {
  val hm = HashMap[B, ArrayBuffer[A]]()
  var keySet = hm.keySet

  def open() {
    parent.open
    parent foreach { t: Rep[A] =>
      val key = grp(t)
      val v = hm.getOrElseUpdate(key, ArrayBuffer[A]())
      v.append(t)
    }
    keySet = hm.keySet
  }
  def next() = {
    if (hm.size != 0) {
      val k = keySet.head
      keySet.remove(k)
      val elem = hashmap_remove(hm, k)
      new {
        val key = k
        val wnd = wndf(elem)
      }
    } else NullDynamicRecord
  }
  def close() {}
  def reset() { parent.reset; hm.clear; open }
}*/
