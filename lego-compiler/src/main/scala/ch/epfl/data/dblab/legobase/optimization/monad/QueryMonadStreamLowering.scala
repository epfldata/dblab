package ch.epfl.data
package dblab.legobase
package optimization
package monad

import schema._
import scala.language.implicitConversions
import sc.pardis.ir._
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
class QueryMonadStreamLowering(val schema: Schema, override val IR: LegoBaseExp) extends RuleBasedTransformer[LegoBaseExp](IR) with StructProcessing[LegoBaseExp] {
  import IR._

  val QML = new QueryMonadLowering(schema, IR)

  def NULL[S: TypeRep]: Rep[S] = zeroValue[S]
  def None[S: TypeRep]: Rep[Option[S]] = Option(NULL[S])

  def zeroValue[S: TypeRep]: Rep[S] = {
    val tp = typeRep[S]
    val v = sc.pardis.shallow.utils.DefaultValue(tp.name).asInstanceOf[S]
    infix_asInstanceOf(unit(v)(tp))(tp)
  }

  implicit def streamType[T: TypeRep]: TypeRep[Stream[T]] = new RecordType(StructTags.ClassTag[Stream[T]]("Stream" + typeRep[T].name), scala.None)

  class Stream[T] {
    def map[S](f: T => S): Stream[S] = ???
    def filter(p: T => Boolean): Stream[T] = ???
    // def flatMap[S](f: T => Stream[S]) = ???
    // def map2[S](f1: T => S, f2: () => S): Stream[S] = ???
  }
  def Done[T: TypeRep]: Rep[Stream[T]] = NULL[Stream[T]]
  def newStream[T: TypeRep](element: Rep[T], isSkip: Rep[Boolean]): Rep[Stream[T]] = __new[Stream[T]](("element", false, element), ("isSkip", false, isSkip))
  def Skip[T: TypeRep]: Rep[Stream[T]] = newStream(NULL[T], unit(true))
  def Yield[T: TypeRep](e: Rep[T]): Rep[Stream[T]] = newStream(e, unit(false))

  implicit class StreamRep[T: TypeRep](self: Rep[Stream[T]]) {
    def map[S: TypeRep](f: Rep[T => S]): Rep[Stream[S]] = flatMap[S](x => Yield(inlineFunction(f, x)))
    def filter(p: Rep[T => Boolean]): Rep[Stream[T]] = flatMap[T](x => __ifThenElse(inlineFunction(p, x), Yield(x), Skip[T]))
    def flatMap[S: TypeRep](f: Rep[T] => Rep[Stream[S]]): Rep[Stream[S]] = dsl"""
      if ($isDone)
        ${Done[S]}
      else if($isSkip)
        ${Skip[S]}
      else
        ${f(element)}
    """
    def isSkip: Rep[Boolean] = field[Boolean](self, "isSkip")
    def isDone: Rep[Boolean] = dsl"$self == ${Done[T]}"
    def element: Rep[T] = field[T](self, "element")
    // def map2[S: TypeRep](f1: Rep[T => S], f2: Rep[() => S]): Rep[Stream[T]] = ???
  }

  implicit class OptionRep1[T: TypeRep](self: Rep[Option[T]]) {
    def map[S: TypeRep](f: Rep[T => S]): Rep[Option[S]] = dsl"""
      if ($self == ${NULL[Option[S]]})
        ${NULL[Option[S]]}
      else if(${self.nonEmpty})
        ${Option(f(self.get))}
      else
        ${None[S]}
    """
  }

  abstract class QueryStream[T: TypeRep] { self =>
    val tp = typeRep[T]
    // type Source
    // implicit def sourceType: TypeRep[Source]

    // def source: Rep[Source]

    // def atEnd(s: Rep[Source]): Rep[Boolean]
    def stream(): Rep[Option[T]]
    def foreach(f: Rep[T] => Rep[Unit]): Rep[Unit] = {
      // dsl"""
      //   var elem: Option[T] = ${NULL[Option[T]]}
      //   while ({
      //     elem = ${stream()}
      //     elem
      //   } != ${NULL[T]}) {
      //     for (e <- elem)
      //       $f(e)
      //   }
      // """
      val elem = __newVar[Option[T]](unit(null))
      __whileDo({
        dsl"""{
            $elem = ${stream()}; 
            $elem
          } != ${NULL[Option[T]]}"""
      }, {
        readVar(elem).foreach {
          __lambda { e =>
            f(e)
          }
        }
      })
    }

    def map[S: TypeRep](f: Rep[T => S]): QueryStream[S] = new QueryStream[S] {
      def stream(): Rep[Option[S]] = dsl"${self.stream()}.map($f)"
    }
    // def map[S: TypeRep](f: Rep[T => S]): QueryStream[S] = new QueryStream[S] {
    //   type Source = self.Source
    //   implicit def sourceType: TypeRep[Source] = self.sourceType

    //   def source: Rep[Source] = self.source

    //   def atEnd(s: Rep[Source]): Rep[Boolean] = self.atEnd(s)
    //   def next(s: Rep[Source]): Rep[(S, Source)] = {
    //     val n = self.next(s)
    //     Tuple2(f(n._1), n._2)
    //   }
    // }
    // def take(num: Rep[Int]): QueryStream[T] = (k: Rep[T] => Rep[Unit]) => {
    //   val counter = __newVarNamed[Int](unit(0), "counter")
    //   foreach(e => __ifThenElse(readVar(counter) < num,
    //     {
    //       k(e)
    //       __assign(counter, readVar(counter) + unit(1))
    //     },
    //     unit()))
    // }

    def filter(p: Rep[T => Boolean]): QueryStream[T] = new QueryStream[T] {
      def stream(): Rep[Option[T]] = {
        val elem = self.stream()
        dsl"""
          if ($elem == ${NULL[Option[T]]})
            ${NULL[Option[T]]}
          else if(${elem.nonEmpty}) {
            val e = ${elem.get}
            if($p(e)) {
              elem
            } else {
              ${None[T]}
            }
          }
          else
            ${None[T]}
        """
      }
    }

    // FIXME
    // def filter(p: Rep[T => Boolean]): QueryStream[T] = self
    // def filter2(p: Rep[T => Boolean]): QueryStream[T] = new QueryStream[T] {
    //   type Source = self.Source
    //   implicit def sourceType: TypeRep[Source] = self.sourceType

    //   def source: Rep[Source] = self.source

    //   val hd = __newVar[T](zeroValue[T])
    //   val curTail = __newVar[Source](zeroValue[Source])

    //   def atEnd(s: Rep[Source]): Rep[Boolean] = self.atEnd(s) || {
    //     val tmpAtEnd = __newVar(unit(false))
    //     val tmpSource = __newVar(s)
    //     val nextAndRest = self.next(tmpSource)
    //     val tmpHd = __newVar(nextAndRest._1)
    //     val tmpRest = __newVar(nextAndRest._2)

    //     __whileDo(!(readVar(tmpAtEnd) || p(tmpHd)), {
    //       __assign(tmpSource, tmpRest)
    //       __ifThenElse(self.atEnd(tmpSource),
    //         __assign(tmpAtEnd, unit(true)), {
    //           val nextAndRest2 = self.next(tmpSource)
    //           __assign(tmpHd, nextAndRest2._1)
    //           __assign(tmpRest, nextAndRest2._2)
    //         })
    //     })
    //     __assign(hd, tmpHd)
    //     __assign(curTail, tmpRest)
    //     readVar(tmpAtEnd)
    //   }
    //   def next(s: Rep[Source]): Rep[(T, Source)] = {
    //     val isAtEnd = atEnd(s)
    //     val resE = __ifThenElse(isAtEnd,
    //       zeroValue[T],
    //       readVar(hd))
    //     val resS = __ifThenElse(isAtEnd,
    //       zeroValue[Source],
    //       readVar(curTail))
    //     Tuple2(resE, resS)
    //   }
    // }

    // def filter(p: Rep[T => Boolean]): QueryStream[T] = new QueryStream[T] {
    //   type Source = self.Source
    //   implicit def sourceType: TypeRep[Source] = self.sourceType

    //   def source: Rep[Source] = self.source

    //   val hd = __newVar[T](zeroValue[T])
    //   val curTail = __newVar[Source](zeroValue[Source])
    //   val tmpAtEnd = __newVar(unit(false))

    //   sealed trait LastCalledFunction
    //   case object NothingYet extends LastCalledFunction
    //   case object AtEnd extends LastCalledFunction
    //   case object Next extends LastCalledFunction

    //   var lastCalledFunction: LastCalledFunction = NothingYet

    //   def atEnd(s: Rep[Source]): Rep[Boolean] = lastCalledFunction match {
    //     case NothingYet | AtEnd =>
    //       lastCalledFunction = AtEnd
    //       self.atEnd(s) || readVar(tmpAtEnd) || {
    //         val tmpSource = __newVar(s)
    //         val nextAndRest = self.next(tmpSource)
    //         val tmpHd = __newVar(nextAndRest._1)
    //         val tmpRest = __newVar(nextAndRest._2)

    //         __whileDo(!(readVar(tmpAtEnd) || p(tmpHd)), {
    //           __assign(tmpSource, tmpRest)
    //           __ifThenElse(self.atEnd(tmpSource), {
    //             __assign(tmpAtEnd, unit(true))
    //             __assign(tmpHd, zeroValue[T])
    //             __assign(tmpRest, zeroValue[Source])
    //           }, {
    //             val nextAndRest2 = self.next(tmpSource)
    //             __assign(tmpHd, nextAndRest2._1)
    //             __assign(tmpRest, nextAndRest2._2)
    //           })
    //         })
    //         __assign(hd, tmpHd)
    //         __assign(curTail, tmpRest)
    //         readVar(tmpAtEnd)
    //       }
    //     // case AtEnd => //readVar(tmpAtEnd)
    //     //   throw new Exception("atEnd after atEnd is not considered yet!")
    //     case Next => throw new Exception("atEnd after next is not considered yet!")
    //   }

    //   def next(s: Rep[Source]): Rep[(T, Source)] = lastCalledFunction match {
    //     case NothingYet => throw new Exception("next before atEnd is not considered yet!")
    //     case AtEnd =>
    //       Tuple2(readVar(hd), readVar(curTail))
    //     case Next => throw new Exception("next after next is not considered yet!")
    //   }

    //   override def filter(p2: Rep[T => Boolean]): QueryStream[T] = self.filter(__lambda { e =>
    //     inlineFunction(p, e) && inlineFunction(p2, e)
    //   })
    // }
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

    // def leftHashSemiJoin2[S: TypeRep, R: TypeRep](q2: QueryStream[S])(
    //   leftHash: Rep[T] => Rep[R])(
    //     rightHash: Rep[S] => Rep[R])(
    //       joinCond: (Rep[T], Rep[S]) => Rep[Boolean]): QueryStream[T] = new QueryStream[T] {
    //   val hm = __newMultiMap[R, S]()
    //   for (elem <- q2) {
    //     hm.addBinding(rightHash(elem), elem)
    //   }
    //   val leftStream = self.filter(__lambda {
    //     t =>
    //       {
    //         val k = leftHash(t)
    //         // TODO add exists to option to make this one nicer
    //         val result = __newVarNamed(unit(false), "setExists")
    //         hm.get(k).foreach(__lambda { buf =>
    //           __assign(result, buf.exists(__lambda { e => joinCond(t, e) }))
    //         })
    //         readVar(result)
    //       }
    //   })
    //   type Source = leftStream.Source
    //   def sourceType: TypeRep[Source] = leftStream.sourceType
    //   def source = leftStream.source
    //   def atEnd(ts: Rep[Source]): Rep[Boolean] = leftStream.atEnd(ts)
    //   def next(ts: Rep[Source]) = leftStream.next(ts)
    // }

    // def mergeJoin2[S: TypeRep, Res: TypeRep](q2: QueryStream[S])(
    //   ord: (Rep[T], Rep[S]) => Rep[Int])(joinCond: (Rep[T], Rep[S]) => Rep[Boolean]): QueryStream[Res] =
    //   new QueryStream[Res] {
    //     val q1 = self
    //     type Source1 = q1.Source
    //     type Source2 = q2.Source
    //     type Source = (q1.Source, q2.Source)
    //     import q1.{ sourceType => q1SourceType }
    //     import q2.{ sourceType => q2SourceType }
    //     def sourceType: TypeRep[Source] = Tuple2Type(q1.sourceType, q2.sourceType)
    //     def source = Tuple2(q1.source, q2.source)
    //     val ts1: Var[Source1] = __newVar[Source1](zeroValue[Source1])
    //     val ts2: Var[Source2] = __newVar[Source2](zeroValue[Source2])
    //     val elem1: Var[T] = __newVar[T](zeroValue[T])
    //     val elem2: Var[S] = __newVar[S](zeroValue[S])
    //     val leftEnd: Var[Boolean] = __newVar[Boolean](unit(false))
    //     val rightEnd: Var[Boolean] = __newVar[Boolean](unit(false))
    //     val nextJoinElem: Var[Res] = __newVar[Res](zeroValue[Res])
    //     val tmpAtEnd: Var[Boolean] = __newVar[Boolean](unit(false)) // keeps if the two sources are at the end or not
    //     def proceedLeft(): Rep[Unit] = {
    //       dsl"""
    //       if (${q1.atEnd(ts1)}) {
    //         $leftEnd = true
    //       } else {
    //         val q1Next = ${q1.next(ts1)}
    //         val ne1 = q1Next._1
    //         val ns1 = q1Next._2
    //         $elem1 = ne1
    //         $ts1 = ns1
    //       }
    //       """
    //     }
    //     def proceedRight(): Rep[Unit] = {
    //       dsl"""
    //       if (${q2.atEnd(ts2)}) {
    //         $rightEnd = true
    //       } else {
    //         val q2Next = ${q2.next(ts2)}
    //         $elem2 = q2Next._1
    //         $ts2 = q2Next._2
    //       }
    //       """
    //     }
    //     def atEnd(ts: Rep[(Source1, Source2)]) = {
    //       dsl"""
    //       $tmpAtEnd || {
    //         if ($elem1 == ${unit(null)} && $elem2 == ${unit(null)}) {
    //           $ts1 = ${q1.source}
    //           $ts2 = ${q2.source}
    //           ${proceedLeft()}
    //           ${proceedRight()}
    //         }
    //         var found = false
    //         while (!$tmpAtEnd && !found) {
    //           if ($leftEnd || $rightEnd) {
    //             $tmpAtEnd = true
    //           } else {
    //             val cmp = ${ord(elem1, elem2)}
    //             if (cmp < 0) {
    //               ${proceedLeft()}
    //             } else if (cmp > 0) {
    //               ${proceedRight()}
    //             } else {
    //               $nextJoinElem = ${concat_records[T, S, Res](elem1, elem2)}
    //               ${proceedRight()}
    //               found = true
    //             }
    //           }
    //         }
    //         !found
    //       }
    //       """
    //     }
    //     def next(ts: Rep[(Source1, Source2)]) = {
    //       Tuple2(nextJoinElem, Tuple2(ts1, ts2))
    //     }
    //   }

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

    // def sortBy[S: TypeRep](sortFunction: Rep[T => S]): QueryStream[T] = self

    // def sortBy[S: TypeRep](sortFunction: Rep[T => S]): QueryStream[T] = new QueryStream[T] {
    //   type Source = Int
    //   def sourceType: TypeRep[Source] = typeRep[Int]

    //   val (treeSet, size) = {
    //     val treeSet = __newTreeSet2(Ordering[T](__lambda { (x, y) =>
    //       QML.ordering_minus(inlineFunction(sortFunction, x), inlineFunction(sortFunction, y))
    //     }))
    //     self.foreach((elem: Rep[T]) => {
    //       treeSet += elem
    //       unit()
    //     })
    //     (treeSet, treeSet.size)
    //   }

    //   def source: Rep[Source] = unit(0)

    //   def atEnd(s: Rep[Source]): Rep[Boolean] = s >= size
    //   def next(s: Rep[Source]): Rep[(T, Source)] = {
    //     val elem = treeSet.head
    //     treeSet -= elem
    //     Tuple2(elem, s + unit(1))
    //   }
    // }

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

    //   def groupByMapValues[K: TypeRep, S: TypeRep](par: Rep[T => K], pred: Option[Rep[T => Boolean]])(func: Rep[Array[T] => S]): QueryStream[(K, S)] = new QueryStream[(K, S)] {
    //     type V = T
    //     type Source = Int
    //     def sourceType: TypeRep[Source] = typeRep[Int]

    //     val (groupByResult, partitions) = {
    //       val monad = streamMap.find(_._2 == self).get._1

    //       System.out.println(s"HERE!$monad")
    //       val groupByResult = groupByResults(monad).asInstanceOf[GroupByResult[K, V]]

    //       val GroupByResult(array, keyRevertIndex, eachBucketSize, _, keyIndex) =
    //         groupByResult

    //       val lastIndex = __newVarNamed(unit(0), "lastIndex")

    //       // printf(unit("start!"))
    //       self.foreach((elem: Rep[V]) => {
    //         // val key = par(elem)
    //         val cond = pred.map(p => inlineFunction(p, elem)).getOrElse(unit(true))
    //         __ifThenElse(cond, {
    //           val key = inlineFunction(par, elem)
    //           val bucket = keyIndex.getOrElseUpdate(key, {
    //             keyRevertIndex(readVar(lastIndex)) = key
    //             __assign(lastIndex, readVar(lastIndex) + unit(1))
    //             readVar(lastIndex) - unit(1)
    //           })
    //           array(bucket)(eachBucketSize(bucket)) = elem
    //           eachBucketSize(bucket) += unit(1)
    //         }, unit())
    //       })
    //       (groupByResult, readVar(lastIndex))
    //     }

    //     def source: Rep[Source] = unit(0)

    //     def atEnd(s: Rep[Source]): Rep[Boolean] = s >= partitions
    //     def next(s: Rep[Source]): Rep[((K, S), Source)] = {
    //       val GroupByResult(array, keyRevertIndex, eachBucketSize, _, _) =
    //         groupByResult
    //       val i = s
    //       val arr = array(i).dropRight(array(i).length - eachBucketSize(i))
    //       // System.out.println(s"arr size ${arr.size} bucket size ${eachBucketSize(i)}")
    //       val key = keyRevertIndex(i)
    //       val Def(Lambda(_, input, _)) = func
    //       streamMap += input -> QueryStream(arr).asInstanceOf[QueryStream[Any]]
    //       val newValue = inlineFunction(func, arr)
    //       Tuple2(Tuple2(key, newValue), s + unit(1))
    //     }
    //   }
  }

  // case class GroupByResult[K, V](partitionedArray: Rep[Array[Array[V]]], keyRevertIndex: Rep[Array[K]],
  //                                eachBucketSize: Rep[Array[Int]], partitions: Rep[Int], keyIndex: Rep[HashMap[K, Int]])

  // val groupByResults = scala.collection.mutable.Map[Rep[Any], GroupByResult[Any, Any]]()

  // def initGroupByArray[T: TypeRep, K: TypeRep](monad: Rep[Query[T]], par: Rep[T => K]): GroupByResult[K, T] = {
  //   type V = T
  //   def sizeByCardinality: Int = schema.stats.getCardinalityOrElse(typeRep[K].name, 8).toInt
  //   val max_partitions = par match {
  //     case Def(Lambda(_, i, Block(stmts, Def(StructImmutableField(struct, name))))) if i == struct && stmts.size == 1 =>
  //       schema.stats.getDistinctAttrValuesOrElse(name, sizeByCardinality)
  //     case _ =>
  //       sizeByCardinality
  //   }

  //   // System.out.println(typeRep[K] + "-" + max_partitions)
  //   // val MAX_SIZE = unit(4000)
  //   val MAX_SIZE = unit(max_partitions)
  //   val keyIndex = __newHashMap[K, Int]()
  //   val keyRevertIndex = __newArray[K](MAX_SIZE)
  //   val lastIndex = __newVarNamed(unit(0), "lastIndex")
  //   val array = __newArray[Array[V]](MAX_SIZE)
  //   // TODO generalize
  //   schema.stats += "QS_MEM_ARRAY_LINEITEM" -> 4
  //   schema.stats += "QS_MEM_ARRAY_DOUBLE" -> 4
  //   val eachBucketSize = __newArray[Int](MAX_SIZE)
  //   // FIXME if we use .count it will regenerate the same loop until before groupBy
  //   // val arraySize = this.count / MAX_SIZE * unit(4)
  //   val thisSize = unit(schema.stats.getCardinalityOrElse(typeRep[T].name, 1 << 25).toInt)
  //   val arraySize = thisSize / MAX_SIZE * unit(8)
  //   Range(unit(0), MAX_SIZE).foreach {
  //     __lambda { i =>
  //       // val arraySize = originalArray.length
  //       // val arraySize = unit(128)
  //       array(i) = __newArray[V](arraySize)
  //       eachBucketSize(i) = unit(0)
  //     }
  //   }
  //   GroupByResult(array, keyRevertIndex, eachBucketSize, MAX_SIZE, keyIndex)
  // }

  object QueryStream {
    def apply[T: TypeRep](arr: Rep[Array[T]]): QueryStream[T] = new QueryStream[T] {
      val index = __newVarNamed[Int](unit(0), "index")
      def stream(): Rep[Option[T]] =
        dsl"""
          if ($index >= $arr.length)
            ${NULL[Option[T]]}
          else {
            $index = $index + 1
            ${Option(arr(dsl"$index - 1"))}
          }
        """
    }
    // QueryStream { (k: Rep[T] => Rep[Unit]) =>
    //   QML.array_foreach_using_while(arr, k)
    // }
    // implicit def apply[T: TypeRep](k: (Rep[T] => Rep[Unit]) => Rep[Unit]): QueryStream[T] =
    //   new QueryStream[T] {
    //     def foreach(k2: Rep[T] => Rep[Unit]): Rep[Unit] = k(k2)
    //   }
  }

  implicit def queryToStream[T](sym: Rep[Query[T]]): QueryStream[T] = {
    // System.out.println(s"finding stream for $sym")
    val stream = streamMap(sym.asInstanceOf[Rep[Any]]).asInstanceOf[QueryStream[T]]
    // System.out.println(s"tp associated to sym $sym is: ${stream.tp}")
    stream
  }

  val streamMap = mutable.Map[Rep[Any], QueryStream[Any]]()

  rewrite += statement {
    case sym -> QueryNew2(array) =>
      val cps = QueryStream(array)(array.tp.typeArguments(0).asInstanceOf[TypeRep[Any]])
      streamMap += sym -> cps
      sym
  }

  rewrite += statement {
    case sym -> QueryFilter(monad, p) =>
      val cps = queryToStream(monad).filter(p)
      streamMap += sym -> cps
      sym
  }

  rewrite += statement {
    case sym -> QueryMap(monad, f) =>
      val cps = queryToStream(monad).map(f)(f.tp.typeArguments(1).asInstanceOf[TypeRep[Any]])
      streamMap += sym -> cps
      sym
  }

  rewrite += statement {
    case sym -> QueryForeach(monad, f) =>
      // val cps = queryToStream(monad).foreach(i => inlineFunction(f, i)).asInstanceOf[QueryStream[Any]]
      // streamMap += sym -> cps
      // sym
      queryToStream(monad).foreach(i => inlineFunction(f, i))
  }

  // rewrite += statement {
  //   case sym -> QuerySortBy(monad, f) =>
  //     val cps = monad.sortBy(f)(f.tp.typeArguments(1).asInstanceOf[TypeRep[Any]])
  //     streamMap += sym -> cps
  //     sym
  // }

  // rewrite += rule {
  //   case QueryMinBy(monad, by) =>
  //     queryToStream(monad).minBy(by)(by.tp.typeArguments(1).asInstanceOf[TypeRep[Any]])
  // }

  rewrite += rule {
    case QueryCount(monad) =>
      queryToStream(monad).count
  }

  // rewrite += rule {
  //   case QueryAvg(monad) =>
  //     queryToStream(monad).avg
  // }

  rewrite += rule {
    case QuerySum(monad) =>
      queryToStream(monad).sum
  }

  // rewrite += statement {
  //   case sym -> QueryTake(monad, num) =>
  //     val cps = queryToStream(monad).take(num)
  //     streamMap += sym -> cps
  //     sym
  // }

  // rewrite += remove {
  //   case JoinableQueryNew(joinMonad) => ()
  // }

  // rewrite += remove {
  //   case QueryGetList(monad) => ()
  // }

  // rewrite += statement {
  //   case sym -> JoinableQueryLeftHashSemiJoin(monad1, monad2, leftHash, rightHash, joinCond) => {
  //     val Def(JoinableQueryNew(Def(QueryGetList(m1)))) = monad1
  //     val Def(Lambda(lh, _, _)) = leftHash
  //     val Def(Lambda(rh, _, _)) = rightHash
  //     val Def(Lambda2(jc, _, _, _)) = joinCond
  //     val cps = m1.leftHashSemiJoin2(monad2)(lh)(rh)(jc)(monad2.tp.typeArguments(0).asInstanceOf[TypeRep[Record]],
  //       leftHash.tp.typeArguments(1).asInstanceOf[TypeRep[Any]])
  //     streamMap += sym -> cps.asInstanceOf[QueryStream[Any]]
  //     sym
  //   }
  // }

  // rewrite += statement {
  //   case sym -> JoinableQueryHashJoin(monad1, monad2, leftHash, rightHash, joinCond) => {
  //     val Def(JoinableQueryNew(Def(QueryGetList(m1)))) = monad1
  //     val Def(Lambda(lh, _, _)) = leftHash
  //     val Def(Lambda(rh, _, _)) = rightHash
  //     val Def(Lambda2(jc, _, _, _)) = joinCond
  //     val cps = m1.hashJoin2(monad2)(lh)(rh)(jc)(monad2.tp.typeArguments(0).asInstanceOf[TypeRep[Record]],
  //       leftHash.tp.typeArguments(1).asInstanceOf[TypeRep[Any]],
  //       sym.tp.typeArguments(0).asInstanceOf[TypeRep[Any]])
  //     streamMap += sym -> cps.asInstanceOf[QueryStream[Any]]
  //     sym
  //   }
  // }

  // rewrite += statement {
  //   case sym -> JoinableQueryMergeJoin(monad1, monad2, ord, joinCond) => {
  //     val Def(JoinableQueryNew(Def(QueryGetList(m1)))) = monad1
  //     val Def(Lambda2(or, _, _, _)) = ord
  //     val Def(Lambda2(jc, _, _, _)) = joinCond
  //     val cps = m1.mergeJoin2(monad2)(or)(jc)(monad2.tp.typeArguments(0).asInstanceOf[TypeRep[Record]],
  //       sym.tp.typeArguments(0).asInstanceOf[TypeRep[Any]])
  //     streamMap += sym -> cps.asInstanceOf[QueryStream[Any]]
  //     sym
  //   }
  // }

  // val mapValuesFuncs = scala.collection.mutable.ArrayBuffer[Rep[Query[Any] => Any]]()

  // analysis += statement {
  //   case sym -> GroupedQueryMapValues(groupedMonad, func) =>
  //     mapValuesFuncs += func
  //     ()
  // }

  // rewrite += removeStatement {
  //   case (sym -> Lambda(_, _, _)) if mapValuesFuncs.contains(sym) =>
  //     ()
  // }

  // case class GroupByInfo[K, V](monad: Rep[Query[V]], pred: Option[Rep[V => Boolean]], par: Rep[V => K])
  // val groupBysInfo = scala.collection.mutable.Map[Rep[Any], GroupByInfo[Any, Any]]()

  // object QueryGroupByAll {
  //   def unapply(groupedMonad: Def[Any]): Option[GroupByInfo[Any, Any]] = groupedMonad match {
  //     case QueryGroupBy(monad, par)               => Some(GroupByInfo(monad, None, par))
  //     case QueryFilteredGroupBy(monad, pred, par) => Some(GroupByInfo(monad, Some(pred), par))
  //     case _                                      => None
  //   }
  // }

  // analysis += statement {
  //   case sym -> QueryGroupByAll(info) => {
  //     // System.out.println(info)
  //     groupBysInfo += sym -> info
  //     ()
  //   }
  // }

  // rewrite += rule {
  //   case GenericEngineRunQueryObject(b) =>
  //     for ((key, groupByInfo) <- groupBysInfo) {
  //       val groupByResult = initGroupByArray(groupByInfo.monad, groupByInfo.par)(
  //         groupByInfo.par.tp.typeArguments(0).asInstanceOf[TypeRep[Any]], groupByInfo.par.tp.typeArguments(1).asInstanceOf[TypeRep[Any]])
  //       groupByResults(groupByInfo.monad) = groupByResult
  //       System.out.println(s"ADDED ${groupByInfo.monad}!")
  //     }
  //     val newBlock = transformBlock(b)(b.tp)
  //     GenericEngineRunQueryObject(newBlock)(newBlock.tp)
  // }

  // // TODO: Separate groupBy and mapValues
  // rewrite += statement {
  //   case sym -> GroupedQueryMapValues(groupedMonad, func) =>
  //     val (monad, pred, par) = groupedMonad match {
  //       case Def(QueryFilteredGroupBy(monad, pred, par)) => (monad, Some(pred), par)
  //       case Def(QueryGroupBy(monad, par))               => (monad, None, par)
  //     }
  //     implicit val typeK = groupedMonad.tp.typeArguments(0).asInstanceOf[TypeRep[Any]]
  //     implicit val typeV = groupedMonad.tp.typeArguments(1).asInstanceOf[TypeRep[Any]]
  //     implicit val typeS = func.tp.typeArguments(1).asInstanceOf[TypeRep[Any]]
  //     val cps = monad.groupByMapValues(par, pred)(func.asInstanceOf[Rep[Array[Any] => Any]])(typeK, typeS)
  //     streamMap += sym -> cps.asInstanceOf[QueryStream[Any]]
  //     sym
  // }
}
