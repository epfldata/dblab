package ch.epfl.data
package dblab.legobase
package queryengine
package push

import scala.reflect.runtime.universe._
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap
import scala.collection.mutable.Set
import scala.collection.mutable.TreeSet
import GenericEngine._
import sc.pardis.annotations.{ deep, metadeep, dontInline, needs }
import sc.pardis.shallow.{ Record, DynamicCompositeRecord }
import scala.reflect.ClassTag

@metadeep(
  folder = "",
  header = "import ch.epfl.data.dblab.legobase.deep.queryengine._",
  component = "OperatorsComponent",
  thisComponent = "ch.epfl.data.dblab.legobase.deep.DeepDSL")
class MetaInfo

/**
 * The common class for all query operators in push-engine.
 */
@deep abstract class Operator[+A] {
  def open()
  def next()
  def reset()
  def consume(tuple: Record)
  @inline var child: Operator[Any] = null
  var stop = false
  val expectedSize: Int
}

/**
 * Factory for creating MultiMap instances.
 */
object MultiMap {
  def apply[K, V] =
    sc.pardis.shallow.scalalib.collection.MultiMap[K, V]
}

/**
 * Scan Operator
 *
 * @param table an array of records which the scan operator receives
 * as input
 */
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

/* Amir (TODO): the following line removes the need for stop */
// class PrintOpStop extends Exception("STOP!")

/**
 * Print Operator
 *
 * @param parent the parent operator of this operator
 * @param printFunc the function that should be invoked whenever the output is printed
 * @param limit a thunk value which specifes if there is not longer any need to print more
 * results. It has the same effect as LIMIT clause in SQL.
 */
@deep class PrintOp[A](parent: Operator[A])(printFunc: A => Unit, limit: () => Boolean) extends Operator[A] { self =>
  var numRows = (0)
  val expectedSize = parent.expectedSize
  val printQueryOutput = Config.printQueryOutput // TODO: This should be moved to the config
  def open() {
    parent.child = self; parent.open;
  }
  def next() = {
    parent.next;
    /* Amir (TODO): the following line removes the need for stop */
    // try {
    //   parent.next
    // } catch {
    //   case ex: PrintOpStop =>
    // }
    if (printQueryOutput) printf("(%d rows)\n", numRows)
  }
  def consume(tuple: Record) {
    if (limit() == false) parent.stop = (true)
    /** Amir: the following line removes the need for stop */
    // if (limit() == false) throw new PrintOpStop
    else {
      if (printQueryOutput) printFunc(tuple.asInstanceOf[A]);
      numRows += 1
    }
  }
  def reset() { parent.reset }
}

/**
 * Select Operator
 *
 * @param parent the parent operator of this operator
 * @param selectPred the predicate which is used for filtering elements. It has
 * the same effect as WHERE clause in SQL.
 */
@deep class SelectOp[A](parent: Operator[A])(selectPred: A => Boolean) extends Operator[A] {
  val expectedSize = parent.expectedSize // Assume 100% selectivity
  def open() {
    parent.child = this; parent.open
  }
  def next() = parent.next
  def reset() { parent.reset }
  def consume(tuple: Record) {
    if (selectPred(tuple.asInstanceOf[A])) child.consume(tuple)
  }
}

/**
 * Aggregate Operator
 *
 * @param parent the parent operator of this operator
 * @param numAggs the number of aggregate functions
 * @param grp the function for converting a record to a key which will be used in the
 * HashMap of the aggregate operator
 * @param aggFuncs the aggregate functions used in the aggregate operator
 */
@needs[(HashMap[Any, Any], AGGRecord[_])]
@deep class AggOp[A, B](parent: Operator[A], numAggs: Int)(val grp: Function1[A, B])(val aggFuncs: Function2[A, Double, Double]*) extends Operator[AGGRecord[B]] {
  val hm = HashMap[B, AGGRecord[B]]() //Array[Double]]()
  // val hm = new pardis.shallow.scalalib.collection.HashMapOptimal[B, AGGRecord[B]]() {
  //   override def extractKey(value: AGGRecord[B]): B = value.key
  // }

  val expectedSize = 33554432 // Assume a huge aggregation number just to be sure
  def open() {
    parent.child = this; parent.open
  }
  def next() {
    parent.next
    // var keySet = Set(hm.keySet.toSeq: _*)
    // while (!stop && hm.size != 0) {
    //   val key = keySet.head
    //   keySet.remove(key)
    //   val elem = hm.remove(key)
    //   child.consume(elem.get)
    // }
    // var i = 0
    // hm.foreach { pair =>
    //   i += 1
    // }
    // println(s"size $i")
    hm.foreach { pair =>
      child.consume(pair._2)
      // hm.remove(pair._2.key)
    }
  }
  def reset() { parent.reset; /*hm.clear;*/ open }
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

/**
 * Map Operator
 *
 * @param parent the parent operator of this operator
 * @param mapFuncs the mapping functions used in the map operator
 */
@deep class MapOp[A](parent: Operator[A])(mapFuncs: Function1[A, Unit]*) extends Operator[A] {
  val expectedSize = parent.expectedSize
  def reset { parent.reset }
  def open() { parent.child = this; parent.open }
  def next() { parent.next }
  def consume(tuple: Record) {
    mapFuncs foreach (mf => mf(tuple.asInstanceOf[A]))
    if (child != null) child.consume(tuple)
  }
}

/**
 * Sort Operator
 *
 * @param parent the parent operator of this operator
 * @param orderingFunc the function which specifies the total ordering between to elements
 */
@needs[TreeSet[Any]]
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

/**
 * Hash Join Operator
 *
 * @param leftParent the left parent operator of this operator
 * @param rightParent the right parent operator of this operator
 * @param leftAlias the String that should be prepended to the field names of the records
 * of the left operator
 * @param leftAlias the String that should be prepended to the field names of the records
 * of the right operator
 * @param joinCond the join condition
 * @param leftHash the hashing function used to convert the values of the records of the
 * left operator to the key that is used by the MultiMap of hash-join operator
 * @param rightHash the hashing function used to convert the values of the records of the
 * right operator to the key that is used by the MultiMap of hash-join operator
 */
@needs[scala.collection.mutable.MultiMap[Any, Any]]
@deep
class HashJoinOp[A <: Record, B <: Record, C](val leftParent: Operator[A], val rightParent: Operator[B], leftAlias: String, rightAlias: String)(val joinCond: (A, B) => Boolean)(val leftHash: A => C)(val rightHash: B => C) extends Operator[DynamicCompositeRecord[A, B]] {
  def this(leftParent: Operator[A], rightParent: Operator[B])(joinCond: (A, B) => Boolean)(leftHash: A => C)(rightHash: B => C) = this(leftParent, rightParent, "", "")(joinCond)(leftHash)(rightHash)
  @inline var mode: scala.Int = 0

  val expectedSize = leftParent.expectedSize * 100 // Assume 1 tuple from the left side joins with 100 from the right
  val hm = MultiMap[C, A]

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
      hm.addBinding(k, tuple.asInstanceOf[A])
    } else if (mode == 1) {
      val k = rightHash(tuple.asInstanceOf[B])
      hm.get(k) foreach { tmpBuffer =>
        tmpBuffer foreach { bufElem =>
          if (joinCond(bufElem, tuple.asInstanceOf[B])) {
            val res = bufElem.concatenateDynamic(tuple.asInstanceOf[B], leftAlias, rightAlias)
            child.consume(res)
          }
        }
      }
    }
  }
}

/**
 * Window Operator
 *
 * @param parent the parent operator of this operator
 * @param grp the function which converts the values to a key that is used
 * in the MultiMap of the window operator.
 */
// @deep class WindowOp[A, B, C](parent: Operator[A])(val grp: Function1[A, B])(val wndf: MultiMap.Set[A] => C) extends Operator[WindowRecord[B, C]] {
@needs[(scala.collection.mutable.MultiMap[Any, Any], Set[_])]
@deep class WindowOp[A, B, C](parent: Operator[A])(val grp: Function1[A, B])(val wndf: Set[A] => C) extends Operator[WindowRecord[B, C]] {
  val hm = MultiMap[B, A]

  val expectedSize = parent.expectedSize

  def open() {
    parent.child = this
    parent.open
  }
  def reset() { parent.reset; hm.clear; open }
  def next() {
    parent.next
    hm.foreach { pair =>
      val elem = pair._2
      val wnd = wndf(elem)
      val key = grp(elem.head)
      child.consume(new WindowRecord[B, C](key, wnd))
    }
  }
  def consume(tuple: Record) {
    val key = grp(tuple.asInstanceOf[A])
    hm.addBinding(key, tuple.asInstanceOf[A])
  }
}

/**
 * Left Hash Semi Join Operator
 *
 * @param leftParent the left parent operator of this operator
 * @param rightParent the right parent operator of this operator
 * @param joinCond the join condition
 * @param leftHash the hashing function used to convert the values of the records of the
 * left operator to the key that is used by the MultiMap of left-hash-semi-join operator
 * @param rightHash the hashing function used to convert the values of the records of the
 * right operator to the key that is used by the MultiMap of left-hash-semi-join operator
 */
@needs[scala.collection.mutable.MultiMap[Any, Any]]
@deep
class LeftHashSemiJoinOp[A, B, C](leftParent: Operator[A], rightParent: Operator[B])(joinCond: (A, B) => Boolean)(leftHash: A => C)(rightHash: B => C) extends Operator[A] {
  @inline var mode: scala.Int = 0
  val hm = MultiMap[C, B]
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
      hm.addBinding(k, tuple.asInstanceOf[B])
    } else {
      val k = leftHash(tuple.asInstanceOf[A])
      hm.get(k).foreach { tmpBuffer =>
        if (tmpBuffer.exists(elem => joinCond(tuple.asInstanceOf[A], elem)))
          child.consume(tuple.asInstanceOf[Record])
      }
    }
  }
}

/**
 * Nested Loop Join Operator
 *
 * @param leftParent the left parent operator of this operator
 * @param rightParent the right parent operator of this operator
 * @param leftAlias the String that should be prepended to the field names of the records
 * of the left operator
 * @param leftAlias the String that should be prepended to the field names of the records
 * of the right operator
 * @param joinCond the join condition
 */
@deep
class NestedLoopsJoinOp[A <: Record, B <: Record](leftParent: Operator[A], rightParent: Operator[B], leftAlias: String = "", rightAlias: String = "")(joinCond: (A, B) => Boolean) extends Operator[DynamicCompositeRecord[A, B]] {
  @inline var mode: scala.Int = 0
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

/**
 * Subquery Result Operator
 *
 * @param parent the parent operator of this operator
 */
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

/**
 * Anti Hash Join Operator
 *
 * @param leftParent the left parent operator of this operator
 * @param rightParent the right parent operator of this operator
 * @param joinCond the join condition
 * @param leftHash the hashing function used to convert the values of the records of the
 * left operator to the key that is used by the MultiMap of anti-hash-join operator
 * @param rightHash the hashing function used to convert the values of the records of the
 * right operator to the key that is used by the MultiMap of anti-hash-join operator
 */
@needs[scala.collection.mutable.MultiMap[Any, Any]]
@deep // class HashJoinAnti[A, B, C](leftParent: Operator[A], rightParent: Operator[B])(joinCond: (A, B) => Boolean)(leftHash: A => C)(rightHash: B => C) extends Operator[A] {
class HashJoinAnti[A: Manifest, B, C](leftParent: Operator[A], rightParent: Operator[B])(joinCond: (A, B) => Boolean)(leftHash: A => C)(rightHash: B => C) extends Operator[A] {
  @inline var mode: scala.Int = 0
  val hm = MultiMap[C, A]
  val expectedSize = leftParent.expectedSize * 100

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
    hm.foreach { pair =>
      val v = pair._2
      v.foreach { e =>
        child.consume(e.asInstanceOf[Record])
      }
    }
  }
  def consume(tuple: Record) {
    // Step 1: Prepare a hash table for the FROM side of the join
    if (mode == 0) {
      val t = tuple.asInstanceOf[A]
      val k = leftHash(t)
      hm.addBinding(k, t)
    } else {
      val t = tuple.asInstanceOf[B]
      val k = rightHash(t)
      hm.get(k).foreach { elems =>
        elems.retain(e => !joinCond(e, t))
      }
    }
  }
}

/**
 * View Operator
 *
 * @param parent the parent operator of this operator
 */
@needs[Array[Any]]
@deep
class ViewOp[A: Manifest](parent: Operator[A]) extends Operator[A] {
  var size = 0
  val table = new Array[A](parent.expectedSize)
  val expectedSize = parent.expectedSize
  @inline var initialized = false

  def open() {
    parent.child = this
    parent.open
  }
  def reset() {}
  def next() {
    if (!initialized) {
      parent.next
      initialized = true
    }
    var idx = 0
    while (!stop && idx < size) {
      val e = table(idx)
      idx += 1
      child.consume(e.asInstanceOf[Record])
    }
  }
  def consume(tuple: Record) {
    table(size) = tuple.asInstanceOf[A]
    size += 1
  }
}

/**
 * Left Outer Join Operator
 *
 * @param leftParent the left parent operator of this operator
 * @param rightParent the right parent operator of this operator
 * @param joinCond the join condition
 * @param leftHash the hashing function used to convert the values of the records of the
 * left operator to the key that is used by the MultiMap of left-outer-join operator
 * @param rightHash the hashing function used to convert the values of the records of the
 * right operator to the key that is used by the MultiMap of left-outer-join operator
 */
@needs[scala.collection.mutable.MultiMap[Any, Any]]
@deep
class LeftOuterJoinOp[A <: Record, B <: Record: Manifest, C](val leftParent: Operator[A], val rightParent: Operator[B])(val joinCond: (A, B) => Boolean)(val leftHash: A => C)(val rightHash: B => C) extends Operator[DynamicCompositeRecord[A, B]] {
  @inline var mode: scala.Int = 0
  val hm = MultiMap[C, B]
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
      hm.addBinding(k, tuple.asInstanceOf[B])
    } else {
      val k = leftHash(tuple.asInstanceOf[A])
      val hmGet = hm.get(k)
      if (hmGet.nonEmpty) {
        val tmpBuffer = hmGet.get
        tmpBuffer foreach { bufElem =>
          val elem = {
            if (joinCond(tuple.asInstanceOf[A], bufElem)) {
              tuple.asInstanceOf[A].concatenateDynamic(bufElem, "", "")
            } else
              tuple.asInstanceOf[A].concatenateDynamic(defaultB, "", "")
          }
          child.consume(elem)
        }
      } else {
        child.consume(tuple.asInstanceOf[A].concatenateDynamic(defaultB, "", ""))
      }
    }
  }
}
