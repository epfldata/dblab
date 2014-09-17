package ch.epfl.data
package legobase
package queryengine
package push

import scala.reflect.runtime.universe._
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap
import scala.collection.mutable.Set
import scala.collection.mutable.TreeSet
import GenericEngine._
import ch.epfl.data.autolifter.annotations.{ deep, metadeep }
import ch.epfl.data.pardis.shallow.{ Record, DynamicCompositeRecord }

// This is a temporary solution until we introduce dependency management and adopt policies. Not a priority now!
@metadeep(
  "legocompiler/src/main/scala/ch/epfl/data/legobase/deep/push",
  """
package ch.epfl.data
package legobase
package deep
package push

import scalalib._
import pardis.ir._
import pardis.ir.pardisTypeImplicits._
import pardis.deep.scalalib._
import pardis.effects._
""",
  """OperatorsComponent""")
class MetaInfo

@deep abstract class Operator[+A] {
  def open()
  def next()
  def reset()
  def consume(tuple: Record)
  var child: Operator[Any] = null
  var stop = false
  val expectedSize: Int
}

@deep class ScanOp[A](table: Array[A]) extends Operator[A] {
  var i = 0
  val expectedSize = table.length
  def open() {
    //printf("Scan operator commencing...\n")
  }
  def next() {
    while (!stop && i < table.length) {
      /*  for (j <- 0 until 16) {
        child.consume(table(i + j).asInstanceOf[Record])
      }
      i += 16*/
      child.consume(table(i).asInstanceOf[Record])
      i += 1
    }
  }
  def reset() { i = 0 }
  def consume(tuple: Record) { throw new Exception("PUSH ENGINE BUG:: Consume function in ScanOp should never be called!!!!\n") }
}

@deep class PrintOp[A](var parent: Operator[A])(printFunc: A => Unit, limit: () => Boolean) extends Operator[A] { self =>
  var numRows = (0)
  val expectedSize = parent.expectedSize
  def open() {
    parent.child = self; parent.open;
  }
  def next() = parent.next
  def consume(tuple: Record) {
    if (limit() == false) parent.stop = (true)
    else {
      printFunc(tuple.asInstanceOf[A]);
      numRows += 1
    }
  }
  def reset() { parent.reset }
}

@deep class SelectOp[A](parent: Operator[A])(selectPred: A => Boolean) extends Operator[A] {
  val expectedSize = parent.expectedSize / 2 // Assume 50% selectivity
  def open() {
    parent.child = this; parent.open
  }
  def next() = parent.next
  def reset() { parent.reset }
  def consume(tuple: Record) {
    if (selectPred(tuple.asInstanceOf[A])) child.consume(tuple)
  }
}

@deep class AggOp[A, B](parent: Operator[A], numAggs: Int)(val grp: Function1[A, B])(val aggFuncs: Function2[A, Double, Double]*) extends Operator[AGGRecord[B]] {
  val hm = HashMap[B, AGGRecord[B]]() //Array[Double]]()

  val expectedSize = 1024 //Assume 1024 aggregations
  def open() {
    parent.child = this; parent.open
  }
  def next() {
    parent.next
    var keySet = Set(hm.keySet.toSeq: _*)
    while (!stop && hm.size != 0) {
      val key = keySet.head
      keySet.remove(key)
      val elem = hm.remove(key)
      child.consume(elem.get)
    }
  }
  def reset() { parent.reset; hm.clear; open }
  def consume(tuple: Record) {
    val key = grp(tuple.asInstanceOf[A])
    val elem = hm.getOrElseUpdate(key, new AGGRecord(key, new Array[Double](numAggs)))
    val aggs = elem.aggs
    var i: scala.Int = 0
    aggFuncs.foreach { aggFun =>
      aggs(i) = aggFun(tuple.asInstanceOf[A], aggs(i))
      i += 1
    }
  }
}

@deep class MapOp[A](parent: Operator[A])(aggFuncs: Function1[A, Unit]*) extends Operator[A] {

  val expectedSize = parent.expectedSize
  def reset { parent.reset }
  def open() { parent.child = this; parent.open }
  def next() { parent.next }
  def consume(tuple: Record) {
    aggFuncs foreach (agg => agg(tuple.asInstanceOf[A]))
    child.consume(tuple)
  }
}

@deep class SortOp[A](parent: Operator[A])(orderingFunc: Function2[A, A, Int]) extends Operator[A] {

  val expectedSize = parent.expectedSize
  val sortedTree = new TreeSet()(
    new Ordering[A] {
      def compare(o1: A, o2: A) = orderingFunc(o1, o2)
    })
  def next() = {
    parent.next
    while (!stop && sortedTree.size != 0) {
      val elem = sortedTree.head
      sortedTree -= elem
      child.consume(elem.asInstanceOf[Record])
    }
  }
  def reset() { parent.reset; open }
  def open() { parent.child = this; parent.open }
  def consume(tuple: Record) { sortedTree += tuple.asInstanceOf[A] }
}

@deep
class HashJoinOp[A <: Record, B <: Record, C](val leftParent: Operator[A], val rightParent: Operator[B], leftAlias: String, rightAlias: String)(val joinCond: (A, B) => Boolean)(val leftHash: A => C)(val rightHash: B => C) extends Operator[DynamicCompositeRecord[A, B]] {
  def this(leftParent: Operator[A], rightParent: Operator[B])(joinCond: (A, B) => Boolean)(leftHash: A => C)(rightHash: B => C) = this(leftParent, rightParent, "", "")(joinCond)(leftHash)(rightHash)
  var mode: scala.Int = 0

  val expectedSize = leftParent.expectedSize * 10 // Assume 1 tuple from the left side joins with 10 from the right
  val hm = HashMap[C, ArrayBuffer[A]]()

  def reset() {
    rightParent.reset; leftParent.reset; hm.clear;
  }
  def open() = {
    leftParent.child = this
    rightParent.child = this
    leftParent.open
    rightParent.open
  }
  def next() {
    leftParent.next
    mode += 1
    rightParent.next
    mode += 1
  }
  def consume(tuple: Record) {
    if (mode == 0) {
      val k = leftHash(tuple.asInstanceOf[A])
      val v = hm.getOrElseUpdate(k, ArrayBuffer[A]())
      v.append(tuple.asInstanceOf[A])
    } else if (mode == 1) {
      val k = rightHash(tuple.asInstanceOf[B])
      if (hm.contains(k)) {
        val tmpBuffer = hm(k)
        var tmpCount = 0
        var break = false
        while ( /*!stop && */ !break) {
          val bufElem = tmpBuffer(tmpCount) // We know there is at least one element
          if (joinCond(bufElem, tuple.asInstanceOf[B])) {
            val res = bufElem.concatenateDynamic(tuple.asInstanceOf[B], leftAlias, rightAlias)
            child.consume(res)
          }
          tmpCount += 1
          if (tmpCount >= tmpBuffer.size) break = true
        }
      }
    }
  }
}

@deep class WindowOp[A, B, C](parent: Operator[A])(val grp: Function1[A, B])(val wndf: ArrayBuffer[A] => C) extends Operator[WindowRecord[B, C]] {
  val hm = HashMap[B, ArrayBuffer[A]]()

  val expectedSize = parent.expectedSize

  def open() {
    parent.child = this
    parent.open
  }
  def reset() { parent.reset; hm.clear; open }
  def next() {
    parent.next
    var keySet = Set(hm.keySet.toSeq: _*)
    while (!stop && hm.size != 0) {
      val k = keySet.head
      keySet.remove(k)
      val elem = hm.remove(k)
      child.consume(new WindowRecord[B, C](k, wndf(elem.get)))
    }
  }
  def consume(tuple: Record) {
    val t = tuple.asInstanceOf[A]
    val key = grp(t)
    val v = hm.getOrElseUpdate(key, ArrayBuffer[A]())
    v.append(t)
  }
}

@deep
class LeftHashSemiJoinOp[A, B, C](leftParent: Operator[A], rightParent: Operator[B])(joinCond: (A, B) => Boolean)(leftHash: A => C)(rightHash: B => C) extends Operator[A] {
  var mode: scala.Int = 0
  val hm = HashMap[C, ArrayBuffer[B]]()
  val expectedSize = leftParent.expectedSize

  def open() {
    leftParent.child = this
    rightParent.child = this
    leftParent.open
    rightParent.open
  }
  def reset() { rightParent.reset; leftParent.reset; hm.clear; }
  def next() {
    rightParent.next
    mode = 1
    leftParent.next
  }
  def consume(tuple: Record) {
    if (mode == 0) {
      val k = rightHash(tuple.asInstanceOf[B])
      val v = hm.getOrElseUpdate(k, ArrayBuffer[B]())
      v.append(tuple.asInstanceOf[B])
    } else {
      val k = leftHash(tuple.asInstanceOf[A])
      if (hm.contains(k)) {
        val tmpBuffer = hm(k)
        //        val idx = tmpBuffer.indexWhere(e => joinCond(tuple.asInstanceOf[A], e))
        var i = 0
        var found = false
        while (!found && i < tmpBuffer.size) {
          if (joinCond(tuple.asInstanceOf[A], tmpBuffer(i))) found = true
          else i += 1
        }

        if (found == true)
          child.consume(tuple.asInstanceOf[Record])
      }
    }
  }
}

@deep
class NestedLoopsJoinOp[A <: Record, B <: Record](leftParent: Operator[A], rightParent: Operator[B], leftAlias: String = "", rightAlias: String = "")(joinCond: (A, B) => Boolean) extends Operator[DynamicCompositeRecord[A, B]] {
  var mode: scala.Int = 0
  var leftTuple = null.asInstanceOf[A]
  val expectedSize = leftParent.expectedSize

  def open() {
    rightParent.child = this
    leftParent.child = this
    rightParent.open
    leftParent.open
  }
  def reset() = { rightParent.reset; leftParent.reset; leftTuple = null.asInstanceOf[A] }
  def next() { leftParent.next }
  def consume(tuple: Record) {
    if (mode == 0) {
      leftTuple = tuple.asInstanceOf[A]
      mode = 1
      rightParent.next
      mode = 0
      rightParent.reset
    } else {
      if (joinCond(leftTuple, tuple.asInstanceOf[B]))
        child.consume(leftTuple.concatenateDynamic(tuple.asInstanceOf[B], leftAlias, rightAlias))
    }
  }
}

@deep
class SubquerySingleResult[A](parent: Operator[A]) extends Operator[A] {
  var result = null.asInstanceOf[A]
  val expectedSize = 1
  def open() {
    throw new Exception("PUSH ENGINE BUG:: Open function in SubqueryResult should never be called!!!!\n")
  }
  def next() {
    throw new Exception("PUSH ENGINE BUG:: Next function in SubqueryResult should never be called!!!!\n")
  }
  def reset() {
    throw new Exception("PUSH ENGINE BUG:: Reset function in SubqueryResult should never be called!!!!\n")
  }
  def consume(tuple: Record) {
    result = tuple.asInstanceOf[A]
  }
  def getResult = {
    parent.child = this
    parent.open
    parent.next
    result
  }
}

@deep
class HashJoinAnti[A, B, C](leftParent: Operator[A], rightParent: Operator[B])(joinCond: (A, B) => Boolean)(leftHash: A => C)(rightHash: B => C) extends Operator[A] {
  var mode: scala.Int = 0
  val hm = HashMap[C, ArrayBuffer[A]]()
  var keySet = Set(hm.keySet.toSeq: _*)
  val expectedSize = leftParent.expectedSize

  def removeFromList(elemList: ArrayBuffer[A], e: A, idx: Int) {
    elemList.remove(idx)
    if (elemList.size == 0) {
      val lh = leftHash(e)
      keySet.remove(lh)
      hm.remove(lh)
      ()
    }
  }

  def open() {
    leftParent.child = this
    leftParent.open
    rightParent.child = this
    rightParent.open
  }
  def reset() { rightParent.reset; leftParent.reset; hm.clear; }
  def next() {
    leftParent.next
    mode = 1
    rightParent.next
    // Step 3: Return everything that left in the hash table
    keySet = Set(hm.keySet.toSeq: _*)
    while (!stop && hm.size != 0) {
      val key = keySet.head
      keySet.remove(key)
      val elems = hm.remove(key)
      var i = 0
      val len = elems.get.size
      val l = elems.get
      while (i < len) {
        val e = l(i)
        child.consume(e.asInstanceOf[Record])
        i += 1
      }
    }
  }
  def consume(tuple: Record) {
    // Step 1: Prepare a hash table for the FROM side of the join
    if (mode == 0) {
      val t = tuple.asInstanceOf[A]
      val k = leftHash(t)
      val v = hm.getOrElseUpdate(k, ArrayBuffer[A]())
      v.append(t)
    } else {
      val t = tuple.asInstanceOf[B]
      val k = rightHash(t)
      if (hm.contains(k)) {
        val elems = hm(k)
        // Sligtly complex logic here: we want to remove while
        // iterating. This is the simplest way to do it, while
        // making it easy to generate C code as well (otherwise we
        // could use filter in scala and assign the result to the hm)
        var removed = 0
        var i = 0;
        val len = elems.size
        while (i < len) {
          var idx = i - removed
          val e = elems(idx)
          if (joinCond(e, t)) {
            //removeFromList(elems, e, idx);
            removed += 1
          }
          i += 1
        }
      }
    }
  }
}

@deep
class ViewOp[A](parent: Operator[A]) extends Operator[A] {
  var idx = 0
  val table = ArrayBuffer[A]()
  val expectedSize = parent.expectedSize

  def open() {
    parent.child = this
    parent.open
  }
  def reset() {}
  def next() {
    parent.next
    val size = table.size
    while (!stop && idx < size) {
      val e = table(idx)
      idx += 1
      child.consume(e.asInstanceOf[Record])
    }
    idx = 0
  }
  def consume(tuple: Record) {
    table.append(tuple.asInstanceOf[A])
  }
}

@deep
class LeftOuterJoinOp[A <: Record, B <: Record: Manifest, C](val leftParent: Operator[A], val rightParent: Operator[B])(val joinCond: (A, B) => Boolean)(val leftHash: A => C)(val rightHash: B => C) extends Operator[DynamicCompositeRecord[A, B]] {
  var mode: scala.Int = 0
  val hm = HashMap[C, ArrayBuffer[B]]()
  val defaultB = Record.getDefaultRecord[B]()
  val expectedSize = leftParent.expectedSize

  def open() = {
    leftParent.child = this
    leftParent.open
    rightParent.child = this
    rightParent.open
  }
  def next() {
    rightParent.next
    mode = 1
    leftParent.next
  }
  def reset() { rightParent.reset; leftParent.reset; hm.clear; }
  def consume(tuple: Record) {
    if (mode == 0) {
      val k = rightHash(tuple.asInstanceOf[B])
      val v = hm.getOrElseUpdate(k, ArrayBuffer[B]())
      v.append(tuple.asInstanceOf[B])
    } else {
      val k = leftHash(tuple.asInstanceOf[A])
      if (hm.contains(k)) {
        val tmpBuffer = hm(k)
        var tmpCount = 0
        while (!stop && tmpCount < tmpBuffer.size) {
          val bufElem = tmpBuffer(tmpCount)
          val elem = {
            if (joinCond(tuple.asInstanceOf[A], bufElem))
              tuple.asInstanceOf[A].concatenateDynamic(bufElem, "", "")
            else tuple.asInstanceOf[A].concatenateDynamic(defaultB, "", "")
          }
          child.consume(elem)
          tmpCount += 1
        }
      } else child.consume(tuple.asInstanceOf[A].concatenateDynamic(defaultB, "", ""))
    }
  }
}
