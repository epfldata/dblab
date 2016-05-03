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
class QueryMonadUnfoldLowering(override val schema: Schema, override val IR: QueryEngineExp, val QML: QueryMonadLowering) extends QueryMonadStreamLoweringInterface(schema, IR) {
  import IR._

  val recordUsageAnalysis: RecordUsageAnalysis[QueryEngineExp] = QML.recordUsageAnalysis

  abstract class QueryUnfold[T: TypeRep] { self =>
    val tp = typeRep[T]
    type Source
    implicit def sourceType: TypeRep[Source]

    def source: Rep[Source]

    def atEnd(s: Rep[Source]): Rep[Boolean]
    def next(s: Rep[Source]): Rep[(T, Source)]
    def foreach(f: Rep[T] => Rep[Unit]): Rep[Unit] = {
      val s = __newVar(source)
      __whileDo(!atEnd(s), {
        val n = next(s)
        f(n._1)
        __assign(s, n._2)
      })
    }
    // def foreach(f: Rep[T] => Rep[Unit]): QueryUnfold[Unit] = new QueryUnfold[Unit] {
    //   type Source = self.Source
    //   implicit def sourceType: TypeRep[Source] = self.sourceType

    //   def source: Rep[Source] = self.source

    //   def atEnd(s: Rep[Source]): Rep[Boolean] = self.atEnd(s)
    //   def next(s: Rep[Source]): Rep[(Unit, Source)] = {
    //     val n = self.next(s)
    //     Tuple2(f(n._1), n._2)
    //   }
    // }
    def map[S: TypeRep](f: Rep[T => S]): QueryUnfold[S] = new QueryUnfold[S] {
      type Source = self.Source
      implicit def sourceType: TypeRep[Source] = self.sourceType

      def source: Rep[Source] = self.source

      def atEnd(s: Rep[Source]): Rep[Boolean] = self.atEnd(s)
      def next(s: Rep[Source]): Rep[(S, Source)] = {
        val n = self.next(s)
        Tuple2(f(n._1), n._2)
      }
    }
    // def take(num: Rep[Int]): QueryUnfold[T] = (k: Rep[T] => Rep[Unit]) => {
    //   val counter = __newVarNamed[Int](unit(0), "counter")
    //   foreach(e => __ifThenElse(readVar(counter) < num,
    //     {
    //       k(e)
    //       __assign(counter, readVar(counter) + unit(1))
    //     },
    //     unit()))
    // }

    // FIXME
    // def filter(p: Rep[T => Boolean]): QueryUnfold[T] = self
    def filter2(p: Rep[T => Boolean]): QueryUnfold[T] = new QueryUnfold[T] {
      type Source = self.Source
      implicit def sourceType: TypeRep[Source] = self.sourceType

      def source: Rep[Source] = self.source

      val hd = __newVar[T](zeroValue[T])
      val curTail = __newVar[Source](zeroValue[Source])

      def atEnd(s: Rep[Source]): Rep[Boolean] = self.atEnd(s) || {
        val tmpAtEnd = __newVar(unit(false))
        val tmpSource = __newVar(s)
        val nextAndRest = self.next(tmpSource)
        val tmpHd = __newVar(nextAndRest._1)
        val tmpRest = __newVar(nextAndRest._2)

        __whileDo(!(readVar(tmpAtEnd) || p(tmpHd)), {
          __assign(tmpSource, tmpRest)
          __ifThenElse(self.atEnd(tmpSource),
            __assign(tmpAtEnd, unit(true)), {
              val nextAndRest2 = self.next(tmpSource)
              __assign(tmpHd, nextAndRest2._1)
              __assign(tmpRest, nextAndRest2._2)
            })
        })
        __assign(hd, tmpHd)
        __assign(curTail, tmpRest)
        readVar(tmpAtEnd)
      }
      def next(s: Rep[Source]): Rep[(T, Source)] = {
        val isAtEnd = atEnd(s)
        val resE = __ifThenElse(isAtEnd,
          zeroValue[T],
          readVar(hd))
        val resS = __ifThenElse(isAtEnd,
          zeroValue[Source],
          readVar(curTail))
        Tuple2(resE, resS)
      }
    }

    def filter(p: Rep[T => Boolean]): QueryUnfold[T] = new QueryUnfold[T] {
      type Source = self.Source
      implicit def sourceType: TypeRep[Source] = self.sourceType

      def source: Rep[Source] = self.source

      val hd = __newVar[T](zeroValue[T])
      val curTail = __newVar[Source](zeroValue[Source])
      val tmpAtEnd = __newVar(unit(false))

      sealed trait LastCalledFunction
      case object NothingYet extends LastCalledFunction
      case object AtEnd extends LastCalledFunction
      case object Next extends LastCalledFunction

      var lastCalledFunction: LastCalledFunction = NothingYet

      def atEnd(s: Rep[Source]): Rep[Boolean] = lastCalledFunction match {
        case NothingYet | AtEnd =>
          lastCalledFunction = AtEnd
          self.atEnd(s) || readVar(tmpAtEnd) || {
            val tmpSource = __newVar(s)
            val nextAndRest = self.next(tmpSource)
            val tmpHd = __newVar(nextAndRest._1)
            val tmpRest = __newVar(nextAndRest._2)

            __whileDo(!(readVar(tmpAtEnd) || p(tmpHd)), {
              __assign(tmpSource, tmpRest)
              __ifThenElse(self.atEnd(tmpSource), {
                __assign(tmpAtEnd, unit(true))
                __assign(tmpHd, zeroValue[T])
                __assign(tmpRest, zeroValue[Source])
              }, {
                val nextAndRest2 = self.next(tmpSource)
                __assign(tmpHd, nextAndRest2._1)
                __assign(tmpRest, nextAndRest2._2)
              })
            })
            __assign(hd, tmpHd)
            __assign(curTail, tmpRest)
            readVar(tmpAtEnd)
          }
        // case AtEnd => //readVar(tmpAtEnd)
        //   throw new Exception("atEnd after atEnd is not considered yet!")
        case Next => throw new Exception("atEnd after next is not considered yet!")
      }

      def next(s: Rep[Source]): Rep[(T, Source)] = lastCalledFunction match {
        case NothingYet => throw new Exception("next before atEnd is not considered yet!")
        case AtEnd =>
          Tuple2(readVar(hd), readVar(curTail))
        case Next => throw new Exception("next after next is not considered yet!")
      }

      override def filter(p2: Rep[T => Boolean]): QueryUnfold[T] = self.filter(__lambda { e =>
        inlineFunction(p, e) && inlineFunction(p2, e)
      })
    }
    def count: Rep[Int] = {
      val size = __newVarNamed[Int](unit(0), "size")
      foreach(e => {
        __assign(size, readVar(size) + unit(1))
      })
      readVar(size)
    }

    // def avg: Rep[T] = {
    //   assert(typeRep[T] == DoubleType)
    //   // it will generate the loops before avg two times
    //   // (sum.asInstanceOf[Rep[Double]] / count).asInstanceOf[Rep[T]]
    //   val sumResult = __newVarNamed[Double](unit(0.0), "sumResult")
    //   val size = __newVarNamed[Int](unit(0), "size")
    //   foreach(elem => {
    //     __assign(sumResult, readVar(sumResult) + elem.asInstanceOf[Rep[Double]])
    //     __assign(size, readVar(size) + unit(1))
    //   })
    //   (readVar(sumResult) / readVar(size)).asInstanceOf[Rep[T]]
    // }

    def sum: Rep[T] = {
      assert(typeRep[T] == DoubleType)
      val sumResult = __newVarNamed[Double](unit(0.0), "sumResult")
      foreach(elem => {
        __assign(sumResult, readVar(sumResult) + elem.asInstanceOf[Rep[Double]])
      })
      readVar(sumResult).asInstanceOf[Rep[T]]
    }

    def leftHashSemiJoin2[S: TypeRep, R: TypeRep](q2: QueryUnfold[S])(
      leftHash: Rep[T] => Rep[R])(
        rightHash: Rep[S] => Rep[R])(
          joinCond: (Rep[T], Rep[S]) => Rep[Boolean]): QueryUnfold[T] = new QueryUnfold[T] {
      val hm = __newMultiMap[R, S]()
      for (elem <- q2) {
        hm.addBinding(rightHash(elem), elem)
      }
      val leftUnfold = self.filter(__lambda {
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
      type Source = leftUnfold.Source
      def sourceType: TypeRep[Source] = leftUnfold.sourceType
      def source = leftUnfold.source
      def atEnd(ts: Rep[Source]): Rep[Boolean] = leftUnfold.atEnd(ts)
      def next(ts: Rep[Source]) = leftUnfold.next(ts)
    }

    def mergeJoin2[S: TypeRep, Res: TypeRep](q2: QueryUnfold[S])(
      ord: (Rep[T], Rep[S]) => Rep[Int])(joinCond: (Rep[T], Rep[S]) => Rep[Boolean]): QueryUnfold[Res] =
      new QueryUnfold[Res] {
        val q1 = self
        type Source1 = q1.Source
        type Source2 = q2.Source
        type Source = (q1.Source, q2.Source)
        import q1.{ sourceType => q1SourceType }
        import q2.{ sourceType => q2SourceType }
        def sourceType: TypeRep[Source] = Tuple2Type(q1.sourceType, q2.sourceType)
        def source = Tuple2(q1.source, q2.source)
        val ts1: Var[Source1] = __newVar[Source1](zeroValue[Source1])
        val ts2: Var[Source2] = __newVar[Source2](zeroValue[Source2])
        val elem1: Var[T] = __newVar[T](zeroValue[T])
        val elem2: Var[S] = __newVar[S](zeroValue[S])
        val leftEnd: Var[Boolean] = __newVar[Boolean](unit(false))
        val rightEnd: Var[Boolean] = __newVar[Boolean](unit(false))
        val nextJoinElem: Var[Res] = __newVar[Res](zeroValue[Res])
        val tmpAtEnd: Var[Boolean] = __newVar[Boolean](unit(false)) // keeps if the two sources are at the end or not
        def proceedLeft(): Rep[Unit] = {
          dsl"""
          if (${q1.atEnd(ts1)}) {
            $leftEnd = true
          } else {
            val q1Next = ${q1.next(ts1)}
            val ne1 = q1Next._1
            val ns1 = q1Next._2
            $elem1 = ne1
            $ts1 = ns1
          }
          """
        }
        def proceedRight(): Rep[Unit] = {
          dsl"""
          if (${q2.atEnd(ts2)}) {
            $rightEnd = true
          } else {
            val q2Next = ${q2.next(ts2)}
            $elem2 = q2Next._1
            $ts2 = q2Next._2
          }
          """
        }
        def atEnd(ts: Rep[(Source1, Source2)]) = {
          dsl"""
          $tmpAtEnd || {
            if ($elem1 == ${unit(null)} && $elem2 == ${unit(null)}) {
              $ts1 = ${q1.source}
              $ts2 = ${q2.source}
              ${proceedLeft()}
              ${proceedRight()}
            }
            var found = false
            while (!$tmpAtEnd && !found) {
              if ($leftEnd || $rightEnd) {
                $tmpAtEnd = true
              } else {
                val cmp = ${ord(elem1, elem2)}
                if (cmp < 0) {
                  ${proceedLeft()}
                } else if (cmp > 0) {
                  ${proceedRight()}
                } else {
                  $nextJoinElem = ${concat_records[T, S, Res](elem1, elem2)}
                  ${proceedRight()}
                  found = true
                }
              }
            }
            !found
          }
          """
        }
        def next(ts: Rep[(Source1, Source2)]) = {
          Tuple2(nextJoinElem, Tuple2(ts1, ts2))
        }
      }

    //   def minBy[S: TypeRep](by: Rep[T => S]): Rep[T] = {
    //     val minResult = __newVarNamed[T](unit(null).asInstanceOf[Rep[T]], "minResult")
    //     def compare(x: Rep[T], y: Rep[T]): Rep[Int] =
    //       QML.ordering_minus(inlineFunction(by, x), inlineFunction(by, y))
    //     foreach((elem: Rep[T]) => {
    //       __ifThenElse((readVar(minResult) __== unit(null)) || compare(elem, readVar(minResult)) < unit(0), {
    //         __assign(minResult, elem)
    //       }, {
    //         unit()
    //       })
    //     })
    //     readVar(minResult)
    //   }

    // def sortBy[S: TypeRep](sortFunction: Rep[T => S]): QueryUnfold[T] = self

    def sortBy[S: TypeRep](sortFunction: Rep[T => S]): QueryUnfold[T] = new QueryUnfold[T] {
      type Source = Int
      def sourceType: TypeRep[Source] = typeRep[Int]

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

      def source: Rep[Source] = unit(0)

      def atEnd(s: Rep[Source]): Rep[Boolean] = s >= size
      def next(s: Rep[Source]): Rep[(T, Source)] = {
        val elem = treeSet.head
        treeSet -= elem
        Tuple2(elem, s + unit(1))
      }
    }

    // def hashJoin2[S: TypeRep, R: TypeRep, Res: TypeRep](q2: QueryUnfold[S])(
    //   leftHash: Rep[T] => Rep[R])(
    //   rightHash: Rep[S] => Rep[R])(
    //   joinCond: (Rep[T], Rep[S]) => Rep[Boolean]): QueryUnfold[Res] = new QueryUnfold[Res] {
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

    //   def hashJoin2[S: TypeRep, R: TypeRep, Res: TypeRep](q2: QueryUnfold[S])(leftHash: Rep[T] => Rep[R])(rightHash: Rep[S] => Rep[R])(joinCond: (Rep[T], Rep[S]) => Rep[Boolean]): QueryUnfold[Res] = (k: Rep[Res] => Rep[Unit]) => {
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
          adder: (Rep[_], QueryUnfold[T]) => Unit): QueryUnfold[(K, S)] = new QueryUnfold[(K, S)] {
      type V = T
      type Source = Int
      def sourceType: TypeRep[Source] = typeRep[Int]

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

      def source: Rep[Source] = unit(0)

      def atEnd(s: Rep[Source]): Rep[Boolean] = s >= partitions
      def next(s: Rep[Source]): Rep[((K, S), Source)] = {
        val GroupByResult(array, keyRevertIndex, eachBucketSize, _, _) =
          groupByResult
        val i = s
        val arr = array(i).dropRight(array(i).length - eachBucketSize(i))
        // System.out.println(s"arr size ${arr.size} bucket size ${eachBucketSize(i)}")
        val key = keyRevertIndex(i)
        val Def(Lambda(_, input, _)) = func
        adder(input, QueryUnfold(arr))
        val newValue = inlineFunction(func, arr)
        Tuple2(Tuple2(key, newValue), s + unit(1))
      }
    }
  }

  object QueryUnfold {
    def apply[T: TypeRep](arr: Rep[Array[T]]): QueryUnfold[T] = new QueryUnfold[T] {
      type Source = Int
      def sourceType: TypeRep[Int] = typeRep[Int]
      def source = unit(0)

      def atEnd(ts: Rep[Int]) = ts >= arr.length
      def next(ts: Rep[Int]) = Tuple2(arr(ts), ts + unit(1))

    }
    // QueryUnfold { (k: Rep[T] => Rep[Unit]) =>
    //   QML.array_foreach_using_while(arr, k)
    // }
    // implicit def apply[T: TypeRep](k: (Rep[T] => Rep[Unit]) => Rep[Unit]): QueryUnfold[T] =
    //   new QueryUnfold[T] {
    //     def foreach(k2: Rep[T] => Rep[Unit]): Rep[Unit] = k(k2)
    //   }
  }

  type LoweredQuery[T] = QueryUnfold[T]

  def __newLoweredQuery[T: TypeRep](array: Rep[Array[T]]): LoweredQuery[T] = QueryUnfold(array)
  def monadFilter[T: TypeRep](query: LoweredQuery[T], p: Rep[T => Boolean]): LoweredQuery[T] = query.filter(p)
  def monadMap[T: TypeRep, S: TypeRep](query: LoweredQuery[T], f: Rep[T => S]): LoweredQuery[S] = query.map(f)
  def monadForeach[T: TypeRep](query: LoweredQuery[T], f: Rep[T => Unit]): Unit = query.foreach(x => inlineFunction(f, x))
  def monadSortBy[T: TypeRep, S: TypeRep](query: LoweredQuery[T], f: Rep[T => S]): LoweredQuery[T] = query.sortBy(f)
  def monadCount[T: TypeRep](query: LoweredQuery[T]): Rep[Int] = query.count
  def monadSum[T: TypeRep](query: LoweredQuery[T]): Rep[T] = query.sum
  def monadAvg[T: TypeRep](query: LoweredQuery[T]): Rep[T] = ??? //query.avg
  def monadMinBy[T: TypeRep, S: TypeRep](query: LoweredQuery[T], f: Rep[T => S]): Rep[T] = ??? //query.minBy(f)
  def monadTake[T: TypeRep](query: LoweredQuery[T], n: Rep[Int]): LoweredQuery[T] = ??? //query.take(n)
  def monadMergeJoin[T: TypeRep, S: TypeRep, Res: TypeRep](q1: LoweredQuery[T], q2: LoweredQuery[S])(
    ord: (Rep[T], Rep[S]) => Rep[Int])(joinCond: (Rep[T], Rep[S]) => Rep[Boolean]): LoweredQuery[Res] = q1.mergeJoin2(q2)(ord)(joinCond)
  def monadLeftHashSemiJoin[T: TypeRep, S: TypeRep, R: TypeRep](q1: LoweredQuery[T], q2: LoweredQuery[S])(
    leftHash: Rep[T] => Rep[R])(rightHash: Rep[S] => Rep[R])(
      joinCond: (Rep[T], Rep[S]) => Rep[Boolean]): LoweredQuery[T] =
    q1.leftHashSemiJoin2(q2)(leftHash)(rightHash)(joinCond)
  def monadHashJoin[T: TypeRep, S: TypeRep, R: TypeRep, Res: TypeRep](q1: LoweredQuery[T], q2: LoweredQuery[S])(
    leftHash: Rep[T] => Rep[R])(rightHash: Rep[S] => Rep[R])(
      joinCond: (Rep[T], Rep[S]) => Rep[Boolean]): LoweredQuery[Res] =
    ???
  //q1.hashJoin2(q2)(leftHash)(rightHash)(joinCond)
  def monadGroupByMapValues[T: TypeRep, K: TypeRep, S: TypeRep](
    query: LoweredQuery[T], groupByResult: GroupByResult[K, T])(
      par: Rep[T => K], pred: Option[Rep[T => Boolean]])(
        func: Rep[Array[T] => S])(
          adder: (Rep[_], LoweredQuery[T]) => Unit): LoweredQuery[(K, S)] =
    query.groupByMapValues(groupByResult)(par, pred)(func)(adder)
}
