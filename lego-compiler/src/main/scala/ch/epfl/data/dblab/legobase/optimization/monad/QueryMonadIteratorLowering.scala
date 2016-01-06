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

/**
 * Lowers query monad operations using continuation-passing style.
 */
class QueryMonadIteratorLowering(val schema: Schema, override val IR: LegoBaseExp) extends RuleBasedTransformer[LegoBaseExp](IR) with StructProcessing[LegoBaseExp] {
  import IR._

  val QML = new QueryMonadLowering(schema, IR)

  abstract class QueryIterator[T: TypeRep] { self =>
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
    // def foreach(f: Rep[T] => Rep[Unit]): QueryIterator[Unit] = new QueryIterator[Unit] {
    //   type Source = self.Source
    //   implicit def sourceType: TypeRep[Source] = self.sourceType

    //   def source: Rep[Source] = self.source

    //   def atEnd(s: Rep[Source]): Rep[Boolean] = self.atEnd(s)
    //   def next(s: Rep[Source]): Rep[(Unit, Source)] = {
    //     val n = self.next(s)
    //     Tuple2(f(n._1), n._2)
    //   }
    // }
    def map[S: TypeRep](f: Rep[T => S]): QueryIterator[S] = new QueryIterator[S] {
      type Source = self.Source
      implicit def sourceType: TypeRep[Source] = self.sourceType

      def source: Rep[Source] = self.source

      def atEnd(s: Rep[Source]): Rep[Boolean] = self.atEnd(s)
      def next(s: Rep[Source]): Rep[(S, Source)] = {
        val n = self.next(s)
        Tuple2(f(n._1), n._2)
      }
    }
    // def take(num: Rep[Int]): QueryIterator[T] = (k: Rep[T] => Rep[Unit]) => {
    //   val counter = __newVarNamed[Int](unit(0), "counter")
    //   foreach(e => __ifThenElse(readVar(counter) < num,
    //     {
    //       k(e)
    //       __assign(counter, readVar(counter) + unit(1))
    //     },
    //     unit()))
    // }

    def zeroValue[S: TypeRep]: Rep[S] = {
      val tp = typeRep[S]
      val v = sc.pardis.shallow.utils.DefaultValue(tp.name).asInstanceOf[S]
      infix_asInstanceOf(unit(v)(tp))(tp)
    }

    // FIXME
    // def filter(p: Rep[T => Boolean]): QueryIterator[T] = self
    // def filter(p: Rep[T => Boolean]): QueryIterator[T] = new QueryIterator[T] {
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

    def filter(p: Rep[T => Boolean]): QueryIterator[T] = new QueryIterator[T] {
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
        case NothingYet =>
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
        case AtEnd => readVar(tmpAtEnd)
        case Next  => throw new Exception("atEnd after next is not considered yet!")
      }

      def next(s: Rep[Source]): Rep[(T, Source)] = lastCalledFunction match {
        case NothingYet => throw new Exception("next before atEnd is not considered yet!")
        case AtEnd =>
          Tuple2(readVar(hd), readVar(curTail))
        case Next => throw new Exception("next after next is not considered yet!")
      }
    }
    // def count: Rep[Int] = {
    //   val size = __newVarNamed[Int](unit(0), "size")
    //   foreach(e => {
    //     __assign(size, readVar(size) + unit(1))
    //   })
    //   readVar(size)
    // }

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
    //   def leftHashSemiJoin2[S: TypeRep, R: TypeRep](q2: QueryIterator[S])(leftHash: Rep[T] => Rep[R])(rightHash: Rep[S] => Rep[R])(joinCond: (Rep[T], Rep[S]) => Rep[Boolean]): QueryIterator[T] = (k: Rep[T] => Rep[Unit]) => {
    //     val hm = __newMultiMap[R, S]()
    //     for (elem <- q2) {
    //       hm.addBinding(rightHash(elem), elem)
    //     }
    //     for (elem <- this) {
    //       val key = leftHash(elem)
    //       hm.get(key) foreach {
    //         __lambda { tmpBuffer =>
    //           __ifThenElse(tmpBuffer.exists(
    //             __lambda { bufElem =>
    //               joinCond(elem, bufElem)
    //             }), {
    //             k(elem)
    //           }, unit())
    //         }
    //       }
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

    // def sortBy[S: TypeRep](sortFunction: Rep[T => S]): QueryIterator[T] = self

    def sortBy[S: TypeRep](sortFunction: Rep[T => S]): QueryIterator[T] = new QueryIterator[T] {
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

    //   def hashJoin2[S: TypeRep, R: TypeRep, Res: TypeRep](q2: QueryIterator[S])(leftHash: Rep[T] => Rep[R])(rightHash: Rep[S] => Rep[R])(joinCond: (Rep[T], Rep[S]) => Rep[Boolean]): QueryIterator[Res] = (k: Rep[Res] => Rep[Unit]) => {
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

    def groupByMapValues[K: TypeRep, S: TypeRep](par: Rep[T => K], pred: Option[Rep[T => Boolean]])(func: Rep[Array[T] => S]): QueryIterator[(K, S)] = new QueryIterator[(K, S)] {
      type V = T
      type Source = Int
      def sourceType: TypeRep[Source] = typeRep[Int]

      val (groupByResult, partitions) = {
        val monad = iteratorMap.find(_._2 == self).get._1

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
        iteratorMap += input -> QueryIterator(arr).asInstanceOf[QueryIterator[Any]]
        val newValue = inlineFunction(func, arr)
        Tuple2(Tuple2(key, newValue), s + unit(1))
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
    val thisSize = unit(schema.stats.getCardinalityOrElse(typeRep[T].name, 1 << 25).toInt)
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

  object QueryIterator {
    def apply[T: TypeRep](arr: Rep[Array[T]]): QueryIterator[T] = new QueryIterator[T] {
      type Source = Int
      def sourceType: TypeRep[Int] = typeRep[Int]
      def source = unit(0)

      def atEnd(ts: Rep[Int]) = ts >= arr.length
      def next(ts: Rep[Int]) = Tuple2(arr(ts), ts + unit(1))

    }
    // QueryIterator { (k: Rep[T] => Rep[Unit]) =>
    //   QML.array_foreach_using_while(arr, k)
    // }
    // implicit def apply[T: TypeRep](k: (Rep[T] => Rep[Unit]) => Rep[Unit]): QueryIterator[T] =
    //   new QueryIterator[T] {
    //     def foreach(k2: Rep[T] => Rep[Unit]): Rep[Unit] = k(k2)
    //   }
  }

  implicit def queryToIterator[T](sym: Rep[Query[T]]): QueryIterator[T] = {
    // System.out.println(s"finding cps for $sym")
    val cps = iteratorMap(sym.asInstanceOf[Rep[Any]]).asInstanceOf[QueryIterator[T]]
    // System.out.println(s"tp associated to sym $sym is: ${cps.tp}")
    cps
  }

  val iteratorMap = mutable.Map[Rep[Any], QueryIterator[Any]]()

  rewrite += statement {
    case sym -> QueryNew2(array) =>
      val cps = QueryIterator(array)(array.tp.typeArguments(0).asInstanceOf[TypeRep[Any]])
      iteratorMap += sym -> cps
      sym
  }

  rewrite += statement {
    case sym -> QueryFilter(monad, p) =>
      val cps = queryToIterator(monad).filter(p)
      iteratorMap += sym -> cps
      sym
  }

  rewrite += statement {
    case sym -> QueryMap(monad, f) =>
      val cps = queryToIterator(monad).map(f)(f.tp.typeArguments(1).asInstanceOf[TypeRep[Any]])
      iteratorMap += sym -> cps
      sym
  }

  rewrite += statement {
    case sym -> QueryForeach(monad, f) =>
      // val cps = queryToIterator(monad).foreach(i => inlineFunction(f, i)).asInstanceOf[QueryIterator[Any]]
      // iteratorMap += sym -> cps
      // sym
      queryToIterator(monad).foreach(i => inlineFunction(f, i))
  }

  rewrite += statement {
    case sym -> QuerySortBy(monad, f) =>
      val cps = monad.sortBy(f)(f.tp.typeArguments(1).asInstanceOf[TypeRep[Any]])
      iteratorMap += sym -> cps
      sym
  }

  // rewrite += rule {
  //   case QueryMinBy(monad, by) =>
  //     queryToIterator(monad).minBy(by)(by.tp.typeArguments(1).asInstanceOf[TypeRep[Any]])
  // }

  // rewrite += rule {
  //   case QueryCount(monad) =>
  //     queryToIterator(monad).count
  // }

  // rewrite += rule {
  //   case QueryAvg(monad) =>
  //     queryToIterator(monad).avg
  // }

  rewrite += rule {
    case QuerySum(monad) =>
      queryToIterator(monad).sum
  }

  // rewrite += statement {
  //   case sym -> QueryTake(monad, num) =>
  //     val cps = queryToIterator(monad).take(num)
  //     iteratorMap += sym -> cps
  //     sym
  // }

  // rewrite += remove {
  //   case JoinableQueryNew(joinMonad) => ()
  // }

  rewrite += remove {
    case QueryGetList(monad) => ()
  }

  // rewrite += statement {
  //   case sym -> JoinableQueryLeftHashSemiJoin(monad1, monad2, leftHash, rightHash, joinCond) => {
  //     val Def(JoinableQueryNew(Def(QueryGetList(m1)))) = monad1
  //     val Def(Lambda(lh, _, _)) = leftHash
  //     val Def(Lambda(rh, _, _)) = rightHash
  //     val Def(Lambda2(jc, _, _, _)) = joinCond
  //     val cps = m1.leftHashSemiJoin2(monad2)(lh)(rh)(jc)(monad2.tp.typeArguments(0).asInstanceOf[TypeRep[Record]],
  //       leftHash.tp.typeArguments(1).asInstanceOf[TypeRep[Any]])
  //     iteratorMap += sym -> cps.asInstanceOf[QueryIterator[Any]]
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
  //     iteratorMap += sym -> cps.asInstanceOf[QueryIterator[Any]]
  //     sym
  //   }
  // }

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
      iteratorMap += sym -> cps.asInstanceOf[QueryIterator[Any]]
      sym
  }
}
