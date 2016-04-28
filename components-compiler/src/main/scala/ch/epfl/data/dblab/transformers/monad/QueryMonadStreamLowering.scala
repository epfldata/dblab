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
class QueryMonadStreamLowering(val schema: Schema, override val IR: QueryEngineExp, val churchEncoding: Boolean, val QML: QueryMonadLowering) extends RuleBasedTransformer[QueryEngineExp](IR) with StructProcessing[QueryEngineExp] {
  import IR._

  val recordUsageAnalysis: RecordUsageAnalysis[QueryEngineExp] = QML.recordUsageAnalysis

  def NULL[S: TypeRep]: Rep[S] = zeroValue[S]

  def zeroValue[S: TypeRep]: Rep[S] = {
    val tp = typeRep[S]
    val v = sc.pardis.shallow.utils.DefaultValue(tp.name).asInstanceOf[S]
    infix_asInstanceOf(unit(v)(tp))(tp)
  }

  implicit def streamType[T: TypeRep]: TypeRep[Stream[T]] = new RecordType(StructTags.ClassTag[Stream[T]]("Stream" + typeRep[T].name), scala.None)

  class Stream[T] {
    def map[S](f: T => S): Stream[S] = ???
    def filter(p: T => Boolean): Stream[T] = ???
  }
  def Done[T: TypeRep]: Rep[Stream[T]] = newStream(NULL[T], unit(false), unit(true))
  def newStream[T: TypeRep](element: Rep[T], isSkip: Rep[Boolean], isDone: Rep[Boolean]): Rep[Stream[T]] = __new[Stream[T]](("element", false, element),
    ("isSkip", false, isSkip),
    ("isDone", false, isDone))
  def Skip[T: TypeRep]: Rep[Stream[T]] = newStream(NULL[T], unit(true), unit(false))
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

    def groupByMapValues[K: TypeRep, S: TypeRep](par: Rep[T => K], pred: Option[Rep[T => Boolean]])(func: Rep[Array[T] => S]): QueryStream[(K, S)] = new QueryStream[(K, S)] {
      type V = T

      val (groupByResult, partitions) = {
        val monad = streamMap.find(_._2 == self).get._1

        System.out.println(s"HERE!$monad")
        val groupByResult = groupByResults(monad).asInstanceOf[GroupByResult[K, V]]

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
        (groupByResult, readVar(lastIndex))
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
          streamMap += input -> QueryStream(arr).asInstanceOf[QueryStream[Any]]
          val newValue = inlineFunction(func, arr)
          dsl"$index = $index + 1"
          Yield(Tuple2(key, newValue))
        }, Done[(K, S)])
      }
    }
  }

  case class GroupByResult[K, V](partitionedArray: Rep[Array[Array[V]]], keyRevertIndex: Rep[Array[K]],
                                 eachBucketSize: Rep[Array[Int]], partitions: Rep[Int], keyIndex: Rep[HashMap[K, Int]])

  val groupByResults = scala.collection.mutable.Map[Rep[Any], GroupByResult[Any, Any]]()

  def initGroupByArray[T: TypeRep, K: TypeRep](monad: Rep[Query[T]], par: Rep[T => K]): GroupByResult[K, T] = {
    type V = T
    def sizeByCardinality: Int = schema.stats.getCardinalityOrElse(typeRep[K].name, 8).toInt
    val max_partitions = par match {
      case Def(Lambda(_, i, Block(stmts, Def(StructImmutableField(struct, name))))) if i == struct && stmts.size == 1 =>
        schema.stats.getDistinctAttrValuesOrElse(name, sizeByCardinality)
      case _ =>
        sizeByCardinality
    }

    // System.out.println(typeRep[K] + "-" + max_partitions)
    // val MAX_SIZE = unit(4000)
    val MAX_SIZE = unit(max_partitions)
    val keyIndex = __newHashMap[K, Int]()
    val keyRevertIndex = __newArray[K](MAX_SIZE)
    val lastIndex = __newVarNamed(unit(0), "lastIndex")
    val array = __newArray[Array[V]](MAX_SIZE)
    // TODO generalize
    schema.stats += "QS_MEM_ARRAY_LINEITEM" -> 4
    schema.stats += "QS_MEM_ARRAY_DOUBLE" -> 4
    val eachBucketSize = __newArray[Int](MAX_SIZE)
    // FIXME if we use .count it will regenerate the same loop until before groupBy
    // val arraySize = this.count / MAX_SIZE * unit(4)
    // val thisSize = unit(schema.stats.getCardinalityOrElse(typeRep[T].name, 1 << 25).toInt)
    val thisSize = unit(schema.stats.getCardinality(typeRep[T].name).toInt)
    val arraySize = thisSize / MAX_SIZE * unit(8)
    Range(unit(0), MAX_SIZE).foreach {
      __lambda { i =>
        // val arraySize = originalArray.length
        // val arraySize = unit(128)
        array(i) = __newArray[V](arraySize)
        eachBucketSize(i) = unit(0)
      }
    }
    GroupByResult(array, keyRevertIndex, eachBucketSize, MAX_SIZE, keyIndex)
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

  rewrite += statement {
    case sym -> QuerySortBy(monad, f) =>
      val cps = monad.sortBy(f)(f.tp.typeArguments(1).asInstanceOf[TypeRep[Any]])
      streamMap += sym -> cps
      sym
  }

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

  rewrite += remove {
    case JoinableQueryNew(joinMonad) => ()
  }

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

  rewrite += statement {
    case sym -> JoinableQueryMergeJoin(monad1, monad2, ord, joinCond) => {
      val Def(JoinableQueryNew(Def(QueryGetList(m1)))) = monad1
      val Def(Lambda2(or, _, _, _)) = ord
      val Def(Lambda2(jc, _, _, _)) = joinCond
      val cps = m1.mergeJoin2(monad2)(or)(jc)(monad2.tp.typeArguments(0).asInstanceOf[TypeRep[Record]],
        sym.tp.typeArguments(0).asInstanceOf[TypeRep[Any]])
      streamMap += sym -> cps.asInstanceOf[QueryStream[Any]]
      sym
    }
  }

  val mapValuesFuncs = scala.collection.mutable.ArrayBuffer[Rep[Query[Any] => Any]]()

  analysis += statement {
    case sym -> GroupedQueryMapValues(groupedMonad, func) =>
      mapValuesFuncs += func
      ()
  }

  rewrite += removeStatement {
    case (sym -> Lambda(_, _, _)) if mapValuesFuncs.contains(sym) =>
      ()
  }

  case class GroupByInfo[K, V](monad: Rep[Query[V]], pred: Option[Rep[V => Boolean]], par: Rep[V => K])
  val groupBysInfo = scala.collection.mutable.Map[Rep[Any], GroupByInfo[Any, Any]]()

  object QueryGroupByAll {
    def unapply(groupedMonad: Def[Any]): Option[GroupByInfo[Any, Any]] = groupedMonad match {
      case QueryGroupBy(monad, par)               => Some(GroupByInfo(monad, None, par))
      case QueryFilteredGroupBy(monad, pred, par) => Some(GroupByInfo(monad, Some(pred), par))
      case _                                      => None
    }
  }

  analysis += statement {
    case sym -> QueryGroupByAll(info) => {
      // System.out.println(info)
      groupBysInfo += sym -> info
      ()
    }
  }

  rewrite += rule {
    case GenericEngineRunQueryObject(b) =>
      for ((key, groupByInfo) <- groupBysInfo) {
        val groupByResult = initGroupByArray(groupByInfo.monad, groupByInfo.par)(
          groupByInfo.par.tp.typeArguments(0).asInstanceOf[TypeRep[Any]], groupByInfo.par.tp.typeArguments(1).asInstanceOf[TypeRep[Any]])
        groupByResults(groupByInfo.monad) = groupByResult
        System.out.println(s"ADDED ${groupByInfo.monad}!")
      }
      val newBlock = transformBlock(b)(b.tp)
      GenericEngineRunQueryObject(newBlock)(newBlock.tp)
  }

  // TODO: Separate groupBy and mapValues
  rewrite += statement {
    case sym -> GroupedQueryMapValues(groupedMonad, func) =>
      val (monad, pred, par) = groupedMonad match {
        case Def(QueryFilteredGroupBy(monad, pred, par)) => (monad, Some(pred), par)
        case Def(QueryGroupBy(monad, par))               => (monad, None, par)
      }
      implicit val typeK = groupedMonad.tp.typeArguments(0).asInstanceOf[TypeRep[Any]]
      implicit val typeV = groupedMonad.tp.typeArguments(1).asInstanceOf[TypeRep[Any]]
      implicit val typeS = func.tp.typeArguments(1).asInstanceOf[TypeRep[Any]]
      val cps = monad.groupByMapValues(par, pred)(func.asInstanceOf[Rep[Array[Any] => Any]])(typeK, typeS)
      streamMap += sym -> cps.asInstanceOf[QueryStream[Any]]
      sym
  }
}
