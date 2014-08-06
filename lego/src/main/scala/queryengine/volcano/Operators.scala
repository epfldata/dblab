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
import ch.epfl.data.pardis.shallow.{ AbstractRecord, DynamicCompositeRecord }

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

class MyHashMap[K: Manifest, V: Manifest] extends scala.collection.mutable.HashMap[K, V]() {
  override def elemHashCode(key: K) = {
    if (manifest[K] == manifest[Array[Byte]]) {
      val arr = key.asInstanceOf[Array[Byte]]
      arr.foldLeft(0)((sum, e) => sum + e)
    } else super.elemHashCode(key)
  }
  override def elemEquals(k1: K, k2: K) = {
    if (manifest[K] == manifest[Array[Byte]]) {
      val arr1 = k1.asInstanceOf[Array[Byte]]
      val arr2 = k2.asInstanceOf[Array[Byte]]
      arr1.sameElements(arr2)
    } else super.elemEquals(k1, k2)
  }
}

@deep abstract class Operator[+A: Manifest] {
  def getShallowHashMap[K: Manifest, V: Manifest]() = new MyHashMap[K, V]()

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

  val hm = getShallowHashMap[B, Array[Double]]()
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

case class HashJoinOp[A <: AbstractRecord: Manifest, B <: AbstractRecord: Manifest, C: Manifest](val leftParent: Operator[A], val rightParent: Operator[B], leftAlias: String = "", rightAlias: String = "")(val joinCond: (A, B) => Boolean)(val leftHash: A => C)(val rightHash: B => C) extends Operator[DynamicCompositeRecord[A, B]] {
  var tmpCount = -1
  var tmpBuffer = ArrayBuffer[A]()
  val hm = getShallowHashMap[C, ArrayBuffer[A]]()
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
      val res = tmpBuffer(tmpCount).concatenateDynamic(tmpLine, leftAlias, rightAlias)
      tmpCount += 1
      res
    } else NullDynamicRecord[DynamicCompositeRecord[A, B]]
  }
  def close() {}
  def reset() { rightParent.reset; leftParent.reset; hm.clear; tmpLine = NullDynamicRecord[B]; tmpCount = 0; tmpBuffer.clear }
}

case class WindowOp[A: Manifest, B: Manifest, C: Manifest](parent: Operator[A])(val grp: Function1[A, B])(val wndf: ArrayBuffer[A] => C) extends Operator[WindowRecord[B, C]] {
  val hm = getShallowHashMap[B, ArrayBuffer[A]]()
  var keySet = scala.collection.mutable.Set(hm.keySet.toSeq: _*)

  def open() {
    parent.open
    parent foreach { t: A =>
      val key = grp(t)
      val v = hm.getOrElseUpdate(key, ArrayBuffer[A]())
      v.append(t)
    }
    keySet = scala.collection.mutable.Set(hm.keySet.toSeq: _*)
  }
  def next() = {
    if (hm.size != 0) {
      val key = keySet.head
      keySet.remove(key)
      val elem = hm.remove(key).get // we're sure that it is Some(x)
      newWindowRecord(key, wndf(elem))
    } else NullDynamicRecord
  }
  def close() {}
  def reset() { parent.reset; hm.clear; open }
}

case class LeftHashSemiJoinOp[A: Manifest, B: Manifest, C: Manifest](leftParent: Operator[A], rightParent: Operator[B])(joinCond: (A, B) => Boolean)(leftHash: A => C)(rightHash: B => C) extends Operator[A] {
  val hm = getShallowHashMap[C, ArrayBuffer[B]]()
  def open() = {
    leftParent.open
    rightParent.open
    rightParent foreach { t: B =>
      val k = rightHash(t)
      val v = hm.getOrElseUpdate(k, ArrayBuffer[B]())
      v.append(t)
    }
  }
  def next() = {
    leftParent findFirst { t: A =>
      val k = leftHash(t)
      if (hm.contains(k)) {
        val tmpBuffer = hm(k)
        tmpBuffer.indexWhere(e => joinCond(t, e)) != -1
      } else false
    }
  }
  def close() {}
  def reset() { rightParent.reset; leftParent.reset; hm.clear; }
}

case class NestedLoopsJoinOp[A <: AbstractRecord: Manifest, B <: AbstractRecord: Manifest](leftParent: Operator[A], rightParent: Operator[B], leftAlias: String = "", rightAlias: String = "")(joinCond: (A, B) => Boolean) extends Operator[DynamicCompositeRecord[A, B]] {
  var leftTuple = NullDynamicRecord[A]
  var rightTuple = NullDynamicRecord[B]

  def open() {
    rightParent.open
    leftParent.open
    leftTuple = leftParent.next
  }
  var cnt = 1
  def next() = {
    var exit = false
    while (exit == false && leftTuple != NullDynamicRecord[A]) {
      rightTuple = rightParent findFirst (t => {
        joinCond(leftTuple, t)
      })
      if (rightTuple == NullDynamicRecord[B]) {
        rightParent.reset
        leftTuple = leftParent.next
      } else exit = true
    }
    if (leftTuple != NullDynamicRecord[A]) {
      leftTuple.concatenateDynamic(rightTuple, leftAlias, rightAlias)
    } else NullDynamicRecord
  }
  def close() = {}
  def reset() = { rightParent.reset; leftParent.reset; leftTuple = NullDynamicRecord[A]; }
}

case class SubquerySingleResult[A: Manifest](parent: Operator[A]) extends Operator[A] {
  def close() {
    throw new Exception("PULL ENGINE BUG:: Close function in SubqueryResult should never be called!!!!\n")
  }
  def open() {
    throw new Exception("PULL ENGINE BUG:: Open function in SubqueryResult should never be called!!!!\n")
  }
  def next() = {
    throw new Exception("PULL ENGINE BUG:: Next function in SubqueryResult should never be called!!!!\n")
  }
  def reset() {
    throw new Exception("PULL ENGINE BUG:: Reset function in SubqueryResult should never be called!!!!\n")
  }
  def getResult = { parent.open; parent.next; }
}

case class HashJoinAnti[A: Manifest, B: Manifest, C: Manifest](leftParent: Operator[A], rightParent: Operator[B])(joinCond: (A, B) => Boolean)(leftHash: A => C)(rightHash: B => C) extends Operator[A] {
  val hm = HashMap[C, ArrayBuffer[A]]()
  var keySet = hm.keySet

  def removeFromList(elemList: ArrayBuffer[A], e: A, idx: Int) = {
    elemList.remove(idx)
    if (elemList.size == 0) {
      val lh = leftHash(e)
      keySet -= lh
      hm.remove(lh)
      ()
    }
    e
  }

  def open() {
    leftParent.open
    rightParent.open
    // Step 1: Prepare a hash table for the FROM side of the join
    leftParent foreach { t: A =>
      {
        val k = leftHash(t)
        val v = hm.getOrElseUpdate(k, ArrayBuffer[A]())
        v.append(t)
      }
    }
    // Step 2: Scan the NOT IN table, removing the corresponding records 
    // from the hash table on each hash hit
    rightParent foreach { t: B =>
      {
        val k = rightHash(t)
        if (hm.contains(k)) {
          val elems = hm(k)
          // Sligtly complex logic here: we want to remove while
          // iterating. This is the simplest way to do it, while
          // making it easy to generate C code as well (otherwise we
          // could use filter in scala and assign the result to the hm)
          var removed = 0
          for (i <- 0 until elems.size) {
            var idx = i - removed
            val e = elems(idx)
            if (joinCond(e, t)) {
              removeFromList(elems, e, idx);
              removed += 1
            }
          }
        }
      }
    }
    keySet = hm.keySet
  }
  // Step 3: Return everything that left in the hash table
  def next() = {
    if (hm.size != 0) {
      val k = keySet.head
      val elemList = hm(k)
      removeFromList(elemList, elemList(0), 0)
    } else NullDynamicRecord
  }
  def close() {}
  def reset() { rightParent.reset; leftParent.reset; hm.clear; }
}

