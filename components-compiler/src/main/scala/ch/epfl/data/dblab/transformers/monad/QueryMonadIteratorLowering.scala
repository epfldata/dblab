package ch.epfl.data
package dblab
package transformers
package monad

import schema._
import scala.language.implicitConversions
import sc.pardis.ir._
import deep.dsls.QueryEngineExp
import reflect.runtime.universe.{ TypeTag, Type }
import sc.pardis.optimization._
import deep._
import sc.pardis.types._
import sc.pardis.types.PardisTypeImplicits._
import sc.pardis.shallow.utils.DefaultValue
import scala.collection.mutable
import quasi._

/**
 * Lowers query monad operations using continuation-passing style.
 */
class QueryMonadIteratorLowering(override val schema: Schema, override val IR: QueryEngineExp, val QML: QueryMonadLowering) extends QueryMonadLoweringInterface(schema, IR) {
  import IR._

  val recordUsageAnalysis: RecordUsageAnalysis[QueryEngineExp] = QML.recordUsageAnalysis

  implicit def mayBeType[T: TypeRep]: TypeRep[MayBe[T]] = new RecordType(StructTags.ClassTag[MayBe[T]]("MayBe" + typeRep[T].name), scala.None)

  class MayBe[T]

  def newMayBe[T: TypeRep](element: Rep[T], isEmpty: Rep[Boolean]): Rep[MayBe[T]] = __new[MayBe[T]](("element", false, element),
    ("isEmpty", false, isEmpty))
  def Nothing[T: TypeRep]: Rep[MayBe[T]] = newMayBe(zeroValue[T], unit(true))
  def Just[T: TypeRep](e: Rep[T]): Rep[MayBe[T]] = newMayBe(e, unit(false))

  abstract class MayBeOps[T: TypeRep] {
    def map[S: TypeRep](f: Rep[T => S]): Rep[MayBe[S]] =
      semiFold(() => Nothing[S], x => Just(inlineFunction(f, x)))
    def filter(p: Rep[T => Boolean]): Rep[MayBe[T]] =
      semiFold(() => Nothing[T], x => __ifThenElse(inlineFunction(p, x), Just(x), Nothing[T]))
    def foreach(f: Rep[T] => Rep[Unit]): Rep[Unit] = semiFold[Unit](
      () => unit(),
      f)
    def flatMap[S: TypeRep](f: Rep[T] => Rep[MayBe[S]]): Rep[MayBe[S]] =
      semiFold(() => Nothing[S], x => f(x).semiFold(() => Nothing[S], y => Just(y)))
    def materialize(): Rep[MayBe[T]] = ???
    def semiFold[S: TypeRep](empty: () => Rep[S], f: Rep[T] => Rep[S]): Rep[S]
  }

  implicit class MayBeRep[T: TypeRep](self: Rep[MayBe[T]]) extends MayBeOps[T] {
    def semiFold[S: TypeRep](empty: () => Rep[S], f: Rep[T] => Rep[S]): Rep[S] =
      self match {
        case _ =>
          dsl"""
      if ($isEmpty)
        ${empty()}
      else
        ${f(element)}
    """
      }

    def isEmpty: Rep[Boolean] = field[Boolean](self, "isEmpty")
    def element: Rep[T] = field[T](self, "element")
  }

  abstract class QueryIterator[T: TypeRep] { self =>
    val tp = typeRep[T]

    def next(): Rep[MayBe[T]]

    def findFirst(p: Rep[T => Boolean]): Rep[MayBe[T]] = {
      val elem = __newVarNamed[MayBe[T]](Nothing[T], "result")
      dsl"""
        var found = false
        while (!found && ${
        dsl"$elem = ${next()}"
        !dsl"$elem".isEmpty
      }) {
          if (${inlineFunction(p, dsl"$elem".element)}) {
            found = true
          }
        }
        $elem
      """
    }

    def foreach(f: Rep[T] => Rep[Unit]): Rep[Unit] = {
      val done = __newVarNamed(unit(false), "done")
      dsl"""
        while(!$done) {
          ${next().semiFold(() => dsl"$done = true", f)}
        }
      """
    }
    def map[S: TypeRep](f: Rep[T => S]): QueryIterator[S] = new QueryIterator[S] {
      def next(): Rep[MayBe[S]] = self.next().map(f)
    }
    def take(num: Rep[Int]): QueryIterator[T] = new QueryIterator[T] {
      val counter = __newVarNamed[Int](unit(0), "counter")
      def next(): Rep[MayBe[T]] = dsl"""
        if($counter < $num) {
          $counter = $counter + 1
          ${self.next()}
        } else {
          ${Nothing[T]}
        }
      """
    }
    def filter(p: Rep[T => Boolean]): QueryIterator[T] = new QueryIterator[T] {
      def next(): Rep[MayBe[T]] = {
        self.findFirst(p)
      }
    }

    def count: Rep[Int] = {
      val size = __newVarNamed[Int](unit(0), "size")
      foreach(e => {
        __assign(size, readVar(size) + unit(1))
      })
      readVar(size)
    }

    def avg: Rep[T] = {
      assert(typeRep[T] == DoubleType)
      // it will generate the loops before avg two times
      // (sum.asInstanceOf[Rep[Double]] / count).asInstanceOf[Rep[T]]
      val sumResult = __newVarNamed[Double](unit(0.0), "sumResult")
      val size = __newVarNamed[Int](unit(0), "size")
      foreach(elem => {
        __assign(sumResult, readVar(sumResult) + elem.asInstanceOf[Rep[Double]])
        __assign(size, readVar(size) + unit(1))
      })
      (readVar(sumResult) / readVar(size)).asInstanceOf[Rep[T]]
    }

    def sum: Rep[T] = {
      assert(typeRep[T] == DoubleType)
      val sumResult = __newVarNamed[Double](unit(0.0), "sumResult")
      foreach(elem => {
        __assign(sumResult, readVar(sumResult) + elem.asInstanceOf[Rep[Double]])
      })
      readVar(sumResult).asInstanceOf[Rep[T]]
    }

    def leftHashSemiJoin2[S: TypeRep, R: TypeRep](q2: QueryIterator[S])(
      leftHash: Rep[T] => Rep[R])(
        rightHash: Rep[S] => Rep[R])(
          joinCond: (Rep[T], Rep[S]) => Rep[Boolean]): QueryIterator[T] = new QueryIterator[T] {
      val hm = __newMultiMap[R, S]()
      for (elem <- q2) {
        hm.addBinding(rightHash(elem), elem)
      }
      val leftIterator = self.filter(__lambda {
        t =>
          {
            val k = leftHash(t)
            // TODO add exists to option to make this one nicer
            val result = __newVarNamed(unit(false), "setExists")
            hm.get(k).foreach(__lambda { buf =>
              __assign(result, buf.exists(__lambda { e => joinCond(t, e) }))
            })
            readVar(result)
          }
      })

      def next() = leftIterator.next()
    }

    def mergeJoin2[S: TypeRep, Res: TypeRep](q2: QueryIterator[S])(
      ord: (Rep[T], Rep[S]) => Rep[Int])(joinCond: (Rep[T], Rep[S]) => Rep[Boolean]): QueryIterator[Res] =
      new QueryIterator[Res] {
        val elem1 = __newVarNamed[MayBe[T]](unit(null), "elemLeft")
        val elem2 = __newVarNamed[MayBe[S]](unit(null), "elemRight")
        val result = __newVarNamed[MayBe[Res]](unit(null), "result")
        val atEnd = __newVarNamed(unit(false), "atEnd")
        val found = __newVarNamed(unit(false), "found")
        val init = __newVarNamed(unit(false), "init")
        val leftShouldProceed = __newVar(dsl"false")
        def next(): Rep[MayBe[Res]] = {
          dsl"""
            $found = false
            $result = ${Nothing[Res]}
            while(!$atEnd && !$found) {
              if ($leftShouldProceed || !$init) {
                $elem1 = ${self.next()}
              }
              if (!$leftShouldProceed || !$init) {
                $elem2 = ${q2.next()}
              }
              $init = true
              if ($atEnd) {
                $result = ${Nothing[Res]}
              } else {${
            dsl"$leftShouldProceed = false"
            dsl"$elem1".semiFold(
              () => dsl"{$atEnd = true}",
              ne1 => {
                dsl"$elem2".semiFold(
                  () => dsl"{$atEnd = true}",
                  ne2 => {
                    val cmp = ord(ne1, ne2)
                    dsl"""
                              if ($cmp < 0) {
                                $leftShouldProceed = true
                              } else {
                                if ($cmp == 0) {
                                  $found = true
                                  $result = ${Just(concat_records[T, S, Res](ne1, ne2))}
                                } else {
                                }
                              }
                              """
                  })
              })
          }
              }

            }
            $result
              

      """
        }
      }

    def minBy[S: TypeRep](by: Rep[T => S]): Rep[T] = {
      val minResult = __newVarNamed[T](unit(null).asInstanceOf[Rep[T]], "minResult")
      def compare(x: Rep[T], y: Rep[T]): Rep[Int] =
        QML.ordering_minus(inlineFunction(by, x), inlineFunction(by, y))
      foreach((elem: Rep[T]) => {
        __ifThenElse((readVar(minResult) __== unit(null)) || compare(elem, readVar(minResult)) < unit(0), {
          __assign(minResult, elem)
        }, {
          unit()
        })
      })
      readVar(minResult)
    }

    def sortBy[S: TypeRep](sortFunction: Rep[T => S]): QueryIterator[T] = new QueryIterator[T] {

      val (treeSet, size) = {
        val treeSet = __newTreeSet2(Ordering[T](__lambda { (x, y) =>
          QML.ordering_minus(inlineFunction(sortFunction, x), inlineFunction(sortFunction, y))
        }))
        self.foreach((elem: Rep[T]) => {
          treeSet += elem
          unit()
        })
        (treeSet, treeSet.size)
      }

      val index = __newVarNamed(unit(0), "sortIndex")
      def next(): Rep[MayBe[T]] = {
        def thenPart: Rep[MayBe[T]] = {
          val elem = treeSet.head
          treeSet -= elem
          dsl"$index = $index + 1"
          Just(elem)
        }
        dsl"""
          if($index < $size) {
            $thenPart
          } else {
            ${Nothing[T]}
          }
        """
      }
    }

    def hashJoin2[S: TypeRep, R: TypeRep, Res: TypeRep](q2: QueryIterator[S])(
      leftHash: Rep[T] => Rep[R])(
        rightHash: Rep[S] => Rep[R])(
          joinCond: (Rep[T], Rep[S]) => Rep[Boolean]): QueryIterator[Res] = new QueryIterator[Res] {
      assert(typeRep[Res].isInstanceOf[RecordType[_]])
      val hm = __newMultiMap[R, T]()
      self.foreach((elem: Rep[T]) => {
        hm.addBinding(leftHash(elem), elem)
      })
      def next(): Rep[MayBe[Res]] = {
        val elem1 = __newVarNamed[MayBe[T]](unit(null), "elemLeft")
        val found = __newVarNamed(unit(false), "found")
        val elem2 = q2 findFirst (__lambda { (t: Rep[S]) =>
          dsl"""
          val k = ${rightHash(t)}
          $hm.get(k) foreach { tmpBuffer =>
            val leftElem = tmpBuffer find (bufElem => $joinCond(bufElem, $t))
            leftElem foreach ${
            __lambda { (le: Rep[T]) =>
              dsl"""
              $elem1 = ${Just(le)}
              $found = true
              """
            }
          }
          }
          $found
      """
        })
        dsl"""
        if ($found) {
          ${Just(concat_records[T, S, Res](dsl"$elem1".element, elem2.element))}
        } else {
          ${Nothing[Res]}
        }
        """

      }
    }

    def groupByMapValues[K: TypeRep, S: TypeRep](groupByResult: GroupByResult[K, T])(
      par: Rep[T => K], pred: Option[Rep[T => Boolean]])(
        func: Rep[Array[T] => S])(
          adder: (Rep[_], QueryIterator[T]) => Unit): QueryIterator[(K, S)] = new QueryIterator[(K, S)] {
      type V = T

      val partitions = {

        val GroupByResult(array, keyRevertIndex, eachBucketSize, _, keyIndex) =
          groupByResult

        val lastIndex = __newVarNamed(unit(0), "lastIndex")

        self.foreach((elem: Rep[V]) => {
          val cond = pred.map(p => inlineFunction(p, elem)).getOrElse(unit(true))
          __ifThenElse(cond, {
            val key = inlineFunction(par, elem)
            val bucket = keyIndex.getOrElseUpdate(key, {
              keyRevertIndex(readVar(lastIndex)) = key
              __assign(lastIndex, readVar(lastIndex) + unit(1))
              readVar(lastIndex) - unit(1)
            })
            array(bucket)(eachBucketSize(bucket)) = elem
            eachBucketSize(bucket) += unit(1)
          }, unit())
        })
        readVar(lastIndex)
      }

      val index = __newVarNamed(unit(0), "indexGroupBy")

      def next(): Rep[MayBe[(K, S)]] = {
        val GroupByResult(array, keyRevertIndex, eachBucketSize, _, _) =
          groupByResult
        val i = readVar(index)
        __ifThenElse(i < partitions, {
          val arr = array(i).dropRight(array(i).length - eachBucketSize(i))
          val key = keyRevertIndex(i)
          val Def(Lambda(_, input, _)) = func
          adder(input, QueryIterator(arr))
          val newValue = inlineFunction(func, arr)
          dsl"$index = $index + 1"
          Just(Tuple2(key, newValue))
        }, Nothing[(K, S)])
      }
    }
  }

  object QueryIterator {
    def apply[T: TypeRep](arr: Rep[Array[T]]): QueryIterator[T] = new QueryIterator[T] {
      val index = __newVarNamed[Int](unit(0), "index")
      def next(): Rep[MayBe[T]] = dsl"""
          if ($index >= $arr.length)
            ${Nothing[T]}
          else {
            $index = $index + 1
            ${Just(arr(dsl"$index - 1"))}
          }
        """
    }
  }

  type LoweredQuery[T] = QueryIterator[T]

  def __newLoweredQuery[T: TypeRep](array: Rep[Array[T]]): LoweredQuery[T] = QueryIterator(array)
  def monadFilter[T: TypeRep](query: LoweredQuery[T], p: Rep[T => Boolean]): LoweredQuery[T] = query.filter(p)
  def monadMap[T: TypeRep, S: TypeRep](query: LoweredQuery[T], f: Rep[T => S]): LoweredQuery[S] = query.map(f)
  def monadForeach[T: TypeRep](query: LoweredQuery[T], f: Rep[T => Unit]): Unit = query.foreach(x => inlineFunction(f, x))
  def monadSortBy[T: TypeRep, S: TypeRep](query: LoweredQuery[T], f: Rep[T => S]): LoweredQuery[T] = query.sortBy(f)
  def monadCount[T: TypeRep](query: LoweredQuery[T]): Rep[Int] = query.count
  def monadSum[T: TypeRep](query: LoweredQuery[T]): Rep[T] = query.sum
  def monadAvg[T: TypeRep](query: LoweredQuery[T]): Rep[T] = query.avg
  def monadMinBy[T: TypeRep, S: TypeRep](query: LoweredQuery[T], f: Rep[T => S]): Rep[T] = query.minBy(f)
  def monadTake[T: TypeRep](query: LoweredQuery[T], n: Rep[Int]): LoweredQuery[T] = query.take(n)
  def monadMergeJoin[T: TypeRep, S: TypeRep, Res: TypeRep](q1: LoweredQuery[T], q2: LoweredQuery[S])(
    ord: (Rep[T], Rep[S]) => Rep[Int])(joinCond: (Rep[T], Rep[S]) => Rep[Boolean]): LoweredQuery[Res] = q1.mergeJoin2(q2)(ord)(joinCond)
  def monadLeftHashSemiJoin[T: TypeRep, S: TypeRep, R: TypeRep](q1: LoweredQuery[T], q2: LoweredQuery[S])(
    leftHash: Rep[T] => Rep[R])(rightHash: Rep[S] => Rep[R])(
      joinCond: (Rep[T], Rep[S]) => Rep[Boolean]): LoweredQuery[T] =
    q1.leftHashSemiJoin2(q2)(leftHash)(rightHash)(joinCond)
  def monadHashJoin[T: TypeRep, S: TypeRep, R: TypeRep, Res: TypeRep](q1: LoweredQuery[T], q2: LoweredQuery[S])(
    leftHash: Rep[T] => Rep[R])(rightHash: Rep[S] => Rep[R])(
      joinCond: (Rep[T], Rep[S]) => Rep[Boolean]): LoweredQuery[Res] =
    q1.hashJoin2(q2)(leftHash)(rightHash)(joinCond)
  def monadGroupByMapValues[T: TypeRep, K: TypeRep, S: TypeRep](
    query: LoweredQuery[T], groupByResult: GroupByResult[K, T])(
      par: Rep[T => K], pred: Option[Rep[T => Boolean]])(
        func: Rep[Array[T] => S])(
          adder: (Rep[_], LoweredQuery[T]) => Unit): LoweredQuery[(K, S)] =
    query.groupByMapValues(groupByResult)(par, pred)(func)(adder)
}
