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
import scala.language.existentials

/**
 * Lowers query monad operations using the stream fusion technique.
 */
class QueryMonadStreamLowering(override val schema: Schema, override val IR: QueryEngineExp, val churchEncoding: Boolean, val QML: QueryMonadLowering) extends QueryMonadLoweringInterface(schema, IR) {
  import IR._

  val recordUsageAnalysis: RecordUsageAnalysis[QueryEngineExp] = QML.recordUsageAnalysis

  implicit def streamType[T: TypeRep]: TypeRep[Stream[T]] = new RecordType(StructTags.ClassTag[Stream[T]]("Stream" + typeRep[T].name), scala.None)

  class Stream[T] {
    def map[S](f: T => S): Stream[S] = ???
    def filter(p: T => Boolean): Stream[T] = ???
  }
  def Done[T: TypeRep]: Rep[Stream[T]] = newStream(zeroValue[T], unit(false), unit(true))
  def newStream[T: TypeRep](element: Rep[T], isSkip: Rep[Boolean], isDone: Rep[Boolean]): Rep[Stream[T]] = __new[Stream[T]](("element", false, element),
    ("isSkip", false, isSkip),
    ("isDone", false, isDone))
  def Skip[T: TypeRep]: Rep[Stream[T]] = newStream(zeroValue[T], unit(true), unit(false))
  def Yield[T: TypeRep](e: Rep[T]): Rep[Stream[T]] = newStream(e, unit(false), unit(false))

  abstract class StreamOps[T: TypeRep] {
    def map[S: TypeRep](f: Rep[T => S]): Rep[Stream[S]] =
      buildS { (done, skip, f1) =>
        semiFold(done, skip, x => f1(inlineFunction(f, x)))
      }
    def filter(p: Rep[T => Boolean]): Rep[Stream[T]] =
      buildS { (done, skip, yld) =>
        semiFold(done, skip, x => __ifThenElse(inlineFunction(p, x), yld(x), skip()))
      }
    def foreach(f: Rep[T] => Rep[Unit]): Rep[Unit] = semiFold[Unit](
      () => unit(),
      () => unit(),
      f)
    def flatMap[S: TypeRep](f: Rep[T] => Rep[Stream[S]]): Rep[Stream[S]] =
      buildS { (done, skip, yld) =>
        semiFold(done, skip, x => f(x).semiFold(done, skip, yld))
      }
    def materialize(): Rep[Stream[T]]
    def semiFold[S: TypeRep](done: () => Rep[S], skip: () => Rep[S], f: Rep[T] => Rep[S]): Rep[S]
  }

  case class BuildStream[T](builder: (() => Rep[BUILDRESULT], () => Rep[BUILDRESULT], Rep[T] => Rep[BUILDRESULT]) => Rep[BUILDRESULT]) extends Stream[T]

  class BUILDRESULT

  implicit def buildResultType: TypeRep[BUILDRESULT] = _buildResultType.asInstanceOf[TypeRep[BUILDRESULT]]

  var _buildResultType: TypeRep[_] = typeRep[Unit]

  def buildS[T: TypeRep](builder: (() => Rep[BUILDRESULT], () => Rep[BUILDRESULT], Rep[T] => Rep[BUILDRESULT]) => Rep[BUILDRESULT]): Rep[Stream[T]] = if (churchEncoding) {
    _buildResultType = typeRep[Unit]
    unit(BuildStream(builder))
  } else {
    _buildResultType = typeRep[Stream[T]]
    builder.asInstanceOf[(() => Rep[Stream[T]], () => Rep[Stream[T]], Rep[T] => Rep[Stream[T]]) => Rep[Stream[T]]](() => Done[T], () => Skip[T], e => Yield[T](e))
  }

  implicit class StreamRep[T: TypeRep](self: Rep[Stream[T]]) extends StreamOps[T] {
    def semiFold[S: TypeRep](done: () => Rep[S], skip: () => Rep[S], f: Rep[T] => Rep[S]): Rep[S] =
      self match {
        case Constant(BuildStream(builder)) => builder.asInstanceOf[(() => Rep[S], () => Rep[S], Rep[T] => Rep[S]) => Rep[S]](done, skip, f)
        case _ =>
          dsl"""
      if ($isDone)
        ${done()}
      else if($isSkip)
        ${skip()}
      else
        ${f(element)}
    """
      }
    def materialize(): Rep[Stream[T]] = if (churchEncoding) {
      _buildResultType = typeRep[Stream[T]]
      semiFold(() => Done[T], () => Skip[T], Yield[T])
    } else {
      self
    }

    def isSkip: Rep[Boolean] = field[Boolean](self, "isSkip")
    def isDone: Rep[Boolean] = field[Boolean](self, "isDone")
    def element: Rep[T] = field[T](self, "element")
  }

  abstract class QueryStream[T: TypeRep] { self =>
    val tp = typeRep[T]

    def stream(): Rep[Stream[T]]

    def foreach(f: Rep[T] => Rep[Unit]): Rep[Unit] = {
      val done = __newVarNamed(unit(false), "done")
      dsl"""
        while(!$done) {
          ${stream().semiFold(() => dsl"$done = true", () => dsl"()", f)}
        }
      """
    }

    def next(): Rep[Stream[T]] = {
      val done = __newVarNamed(unit(false), "done")
      val result = __newVarNamed[Stream[T]](Skip[T], "result")
      dsl"""
        while(!$done) {${
        stream().semiFold(() =>
          dsl"""
                $done = true
                $result = ${Done[T]}
          """, () => dsl"()", e => {
          dsl"""
                $done = true
                $result = ${Yield(e)}
              """
        })
      }}
        $result
      """
    }

    def map[S: TypeRep](f: Rep[T => S]): QueryStream[S] = new QueryStream[S] {
      def stream(): Rep[Stream[S]] = dsl"${self.stream()}.map($f)"
    }

    def filter(p: Rep[T => Boolean]): QueryStream[T] = new QueryStream[T] {
      def stream(): Rep[Stream[T]] = {
        val elem = self.stream()
        elem.filter(p)
      }
    }

    def take(num: Rep[Int]): QueryStream[T] = new QueryStream[T] {
      val counter = __newVarNamed[Int](unit(0), "counter")
      def stream(): Rep[Stream[T]] = buildS { (done, skip, yld) =>
        dsl"""
          if($counter < $num) {
            ${self.stream().semiFold(() => done(), () => skip(), x => { dsl"$counter = $counter + 1"; yld(x) })}
          } else {
            ${done()}
          }
        """
      }
    }

    def leftHashSemiJoin2[S: TypeRep, R: TypeRep](q2: QueryStream[S])(
      leftHash: Rep[T] => Rep[R])(
        rightHash: Rep[S] => Rep[R])(
          joinCond: (Rep[T], Rep[S]) => Rep[Boolean]): QueryStream[T] = new QueryStream[T] {
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

      def stream() = leftIterator.stream()
    }

    def mergeJoin2[S: TypeRep, Res: TypeRep](q2: QueryStream[S])(
      ord: (Rep[T], Rep[S]) => Rep[Int])(joinCond: (Rep[T], Rep[S]) => Rep[Boolean]): QueryStream[Res] =
      new QueryStream[Res] {
        val elem1 = __newVarNamed[Stream[T]](unit(null), "elemLeft")
        val elem2 = __newVarNamed[Stream[S]](unit(null), "elemRight")
        val atEnd = __newVarNamed(unit(false), "atEnd")
        val init = __newVarNamed(unit(false), "init")
        val leftShouldProceed = __newVar(dsl"false")
        def stream(): Rep[Stream[Res]] = {
          dsl"""
            if ($leftShouldProceed || !$init) {
              $elem1 = ${
            // self.stream().materialize()
            self.next()
          }
            }
            if (!$leftShouldProceed || !$init) {
              $elem2 = ${
            // q2.stream().materialize()
            q2.next()
          }
            }
            $init = true
            ${
            buildS { (done, skip, yld: Rep[Res] => Rep[BUILDRESULT]) =>
              dsl"""
              if ($atEnd) {
                ${done()}
              } else {${
                dsl"$leftShouldProceed = false"
                dsl"$elem1".semiFold(
                  () => dsl"{$atEnd = true; ${done()}}",
                  () => {
                    dsl"{$leftShouldProceed = true; ${skip()}}"
                  },
                  ne1 => {
                    dsl"$elem2".semiFold(
                      () => dsl"{$atEnd = true; ${done()}}",
                      () => skip(),
                      ne2 => {
                        val cmp = ord(ne1, ne2)
                        dsl"""
                              if ($cmp < 0) {
                                $leftShouldProceed = true
                                ${skip()}
                              } else {
                                if ($cmp == 0) {
                                  ${yld(concat_records[T, S, Res](ne1, ne2))}
                                } else {
                                  ${skip()}
                                }
                              }
                              """
                      })
                  })
              }
              }
              """
            }
          }

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

    def sortBy[S: TypeRep](sortFunction: Rep[T => S]): QueryStream[T] = new QueryStream[T] {

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
      def stream(): Rep[Stream[T]] = {
        def thenPart: Rep[Stream[T]] = {
          val elem = treeSet.head
          treeSet -= elem
          dsl"$index = $index + 1"
          Yield(elem)
        }
        dsl"""
          if($index < $size) {
            $thenPart
          } else {
            ${Done[T]}
          }
        """
      }
    }

    // def hashJoin2[S: TypeRep, R: TypeRep, Res: TypeRep](q2: QueryStream[S])(
    //   leftHash: Rep[T] => Rep[R])(
    //   rightHash: Rep[S] => Rep[R])(
    //   joinCond: (Rep[T], Rep[S]) => Rep[Boolean]): QueryStream[Res] = new QueryStream[Res] {
    //   type Source = (q2.Source, Int)
    //   def sourceType: TypeRep[Source] = Tuple2Type(q2.sourceType, typeRep[Int])

    //   val tmpBuffer = __newVarNamed[ArrayBuffer[T]](infix_asInstanceOf[ArrayBuffer[T]](unit(null)), unit("tmpBuffer"))

    //   val hm = {
    //     val hm = __newMultiMap[R, T]()
    //     self.foreach((elem: Rep[T]) => {
    //       hm.addBinding(leftHash(elem), elem)
    //     })
    //     hm
    //   }

    //   def source: Rep[Source] = Tuple2(q2.source, unit(0))

    //   def atEnd(s: Rep[Source]): Rep[Boolean] = __ifThenElse(s._2 >= tmpBuffer.size, {
    //       __ifThenElse(q2.atEnd,
    //         unit(true), {
    //           val e2 = q2.next(s._1)
    //           val key = rightHash(e2._1)
    //           hm.get(key) foreach {
    //             __lambda { buff =>
    //               __assign(tmpBuffer, buff)
    //             }
    //           }
    //           q2.atEnd  
    //         }
    //       )
    //     }, {
    //       unit(false)
    //     })
    //   def next(s: Rep[Source]): Rep[(T, Source)] = {
    //     val elem = treeSet.head
    //     treeSet -= elem
    //     Tuple2(elem, s + unit(1))
    //   }
    // }

    //   def hashJoin2[S: TypeRep, R: TypeRep, Res: TypeRep](q2: QueryStream[S])(leftHash: Rep[T] => Rep[R])(rightHash: Rep[S] => Rep[R])(joinCond: (Rep[T], Rep[S]) => Rep[Boolean]): QueryStream[Res] = (k: Rep[Res] => Rep[Unit]) => {
    //     assert(typeRep[Res].isInstanceOf[RecordType[_]])
    //     // System.out.println(s"hashJoin called!!!")
    //     val hm = __newMultiMap[R, T]()
    //     foreach((elem: Rep[T]) => {
    //       hm.addBinding(leftHash(elem), elem)
    //     })
    //     q2.foreach((elem: Rep[S]) => {
    //       val key = rightHash(elem)
    //       hm.get(key) foreach {
    //         __lambda { tmpBuffer =>
    //           tmpBuffer foreach {
    //             __lambda { bufElem =>
    //               __ifThenElse(joinCond(bufElem, elem), {
    //                 k(concat_records[T, S, Res](bufElem, elem))
    //               }, unit())
    //             }
    //           }
    //         }
    //       }
    //     })
    //   }

    def groupByMapValues[K: TypeRep, S: TypeRep](groupByResult: GroupByResult[K, T])(
      par: Rep[T => K], pred: Option[Rep[T => Boolean]])(
        func: Rep[Array[T] => S])(
          adder: (Rep[_], QueryStream[T]) => Unit): QueryStream[(K, S)] = new QueryStream[(K, S)] {
      type V = T

      val partitions = {

        val GroupByResult(array, keyRevertIndex, eachBucketSize, _, keyIndex) =
          groupByResult

        val lastIndex = __newVarNamed(unit(0), "lastIndex")

        // printf(unit("start!"))
        self.foreach((elem: Rep[V]) => {
          // val key = par(elem)
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

      def stream(): Rep[Stream[(K, S)]] = {
        val GroupByResult(array, keyRevertIndex, eachBucketSize, _, _) =
          groupByResult
        val i = readVar(index)
        __ifThenElse(i < partitions, {
          val arr = array(i).dropRight(array(i).length - eachBucketSize(i))
          // System.out.println(s"arr size ${arr.size} bucket size ${eachBucketSize(i)}")
          val key = keyRevertIndex(i)
          val Def(Lambda(_, input, _)) = func
          adder(input, QueryStream(arr))
          val newValue = inlineFunction(func, arr)
          dsl"$index = $index + 1"
          Yield(Tuple2(key, newValue))
        }, Done[(K, S)])
      }
    }
  }

  object QueryStream {
    def apply[T: TypeRep](arr: Rep[Array[T]]): QueryStream[T] = new QueryStream[T] {
      val index = __newVarNamed[Int](unit(0), "index")
      def stream(): Rep[Stream[T]] = buildS { (done, skip, yld) =>
        dsl"""
          if ($index >= $arr.length)
            ${done()}
          else {
            $index = $index + 1
            ${yld(arr(dsl"$index - 1"))}
          }
        """
      }

    }
  }

  type LoweredQuery[T] = QueryStream[T]

  def __newLoweredQuery[T: TypeRep](array: Rep[Array[T]]): LoweredQuery[T] = QueryStream(array)
  def monadFilter[T: TypeRep](query: LoweredQuery[T], p: Rep[T => Boolean]): LoweredQuery[T] = query.filter(p)
  def monadMap[T: TypeRep, S: TypeRep](query: LoweredQuery[T], f: Rep[T => S]): LoweredQuery[S] = query.map(f)
  def monadForeach[T: TypeRep](query: LoweredQuery[T], f: Rep[T => Unit]): Unit = query.foreach(x => inlineFunction(f, x))
  def monadSortBy[T: TypeRep, S: TypeRep](query: LoweredQuery[T], f: Rep[T => S]): LoweredQuery[T] = query.sortBy(f)
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
      joinCond: (Rep[T], Rep[S]) => Rep[Boolean]): LoweredQuery[Res] = ???
  def monadGroupByMapValues[T: TypeRep, K: TypeRep, S: TypeRep](
    query: LoweredQuery[T], groupByResult: GroupByResult[K, T])(
      par: Rep[T => K], pred: Option[Rep[T => Boolean]])(
        func: Rep[Array[T] => S])(
          adder: (Rep[_], LoweredQuery[T]) => Unit): LoweredQuery[(K, S)] =
    query.groupByMapValues(groupByResult)(par, pred)(func)(adder)
}
