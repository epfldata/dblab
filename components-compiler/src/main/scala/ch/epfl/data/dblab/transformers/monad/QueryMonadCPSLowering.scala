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
 * Lowers query monad operations using church encoding of lists.
 */
class QueryMonadCPSLowering(override val schema: Schema, override val IR: QueryEngineExp, val QML: QueryMonadLowering) extends QueryMonadLoweringInterface(schema, IR) {
  import IR._
  val recordUsageAnalysis: RecordUsageAnalysis[QueryEngineExp] = QML.recordUsageAnalysis

  val SUPPORT_ONLY_1_TO_N = true

  abstract class QueryCPS[T: TypeRep] {
    val tp = typeRep[T]
    def foreach(k: Rep[T] => Rep[Unit]): Rep[Unit]
    def map[S: TypeRep](f: Rep[T => S]): QueryCPS[S] = (k: Rep[S] => Rep[Unit]) => {
      foreach(e => k(inlineFunction(f, e)))
    }
    def take(num: Rep[Int]): QueryCPS[T] = (k: Rep[T] => Rep[Unit]) => {
      val counter = __newVarNamed[Int](unit(0), "counter")
      foreach(e => __ifThenElse(readVar(counter) < num,
        {
          k(e)
          __assign(counter, readVar(counter) + unit(1))
        },
        unit()))
    }
    def filter(p: Rep[T => Boolean]): QueryCPS[T] = (k: Rep[T] => Rep[Unit]) => {
      foreach(e => __ifThenElse(inlineFunction(p, e), k(e), unit()))
    }

    def leftHashSemiJoin2[S: TypeRep, R: TypeRep](q2: QueryCPS[S])(leftHash: Rep[T] => Rep[R])(rightHash: Rep[S] => Rep[R])(joinCond: (Rep[T], Rep[S]) => Rep[Boolean]): QueryCPS[T] = (k: Rep[T] => Rep[Unit]) => {
      val hm = __newMultiMap[R, S]()
      for (elem <- q2) {
        hm.addBinding(rightHash(elem), elem)
      }
      for (elem <- this) {
        val key = leftHash(elem)
        hm.get(key) foreach {
          __lambda { tmpBuffer =>
            // __ifThenElse(tmpBuffer.exists(
            //   __lambda { bufElem =>
            //     joinCond(elem, bufElem)
            //   }), {
            //   k(elem)
            // }, unit())
            dsl"""val leftElem = $tmpBuffer find (bufElem => $joinCond($elem, bufElem))
              leftElem foreach (le => $k($elem))"""
          }
        }
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

    def mergeJoin2[S: TypeRep, Res: TypeRep](q2: QueryCPS[S])(
      ord: (Rep[T], Rep[S]) => Rep[Int])(joinCond: (Rep[T], Rep[S]) => Rep[Boolean]): QueryCPS[Res] = (k: Rep[Res] => Rep[Unit]) => {
      val sizeByCardinality: Int = schema.stats.getCardinalityOrElse(typeRep[T].name, 1 << 24).toInt
      val arr = __newArray[T](unit(sizeByCardinality))
      val size = __newVarNamed[Int](unit(0), "arraySize")
      foreach((elem: Rep[T]) => {
        dsl"""
           $arr($size) = $elem
           $size = $size + 1
        """
      })
      val leftSize = (size: Rep[Int])
      val leftIndex = __newVarNamed(unit(0), "leftIndex")
      q2.foreach((elem: Rep[S]) => {
        dsl"""
          while($leftIndex < $leftSize && ${ord(arr(leftIndex), elem)} < 0) {
            $leftIndex = $leftIndex + 1
          }
          if($leftIndex < $leftSize && ${ord(arr(leftIndex), elem)} == 0) {
            ${k(concat_records[T, S, Res](arr(leftIndex), elem))}
          }
        """
      })
    }

    def sortBy[S: TypeRep](sortFunction: Rep[T => S]): QueryCPS[T] = (k: Rep[T] => Rep[Unit]) => {
      // TODO generalize
      val treeSet = __newTreeSet2(Ordering[T](__lambda { (x, y) =>
        QML.ordering_minus(inlineFunction(sortFunction, x), inlineFunction(sortFunction, y))
      }))
      foreach((elem: Rep[T]) => {
        treeSet += elem
        unit()
      })
      Range(unit(0), treeSet.size).foreach(__lambda { i =>
        val elem = treeSet.head
        treeSet -= elem
        k(elem)
      })
    }

    def hashJoin2[S: TypeRep, R: TypeRep, Res: TypeRep](q2: QueryCPS[S])(leftHash: Rep[T] => Rep[R])(rightHash: Rep[S] => Rep[R])(joinCond: (Rep[T], Rep[S]) => Rep[Boolean]): QueryCPS[Res] = (k: Rep[Res] => Rep[Unit]) => {
      assert(typeRep[Res].isInstanceOf[RecordType[_]])
      // System.out.println(s"hashJoin called!!!")
      val hm = __newMultiMap[R, T]()
      foreach((elem: Rep[T]) => {
        hm.addBinding(leftHash(elem), elem)
      })
      q2.foreach((elem: Rep[S]) => {
        val key = rightHash(elem)
        hm.get(key) foreach {
          __lambda { tmpBuffer =>
            if (SUPPORT_ONLY_1_TO_N) {
              dsl"""val leftElem = $tmpBuffer find (bufElem => $joinCond(bufElem, $elem))
              leftElem foreach ${
                __lambda { (le: Rep[T]) =>
                  k(concat_records[T, S, Res](le, elem))
                }
              }"""
            } else {
              tmpBuffer foreach {
                __lambda { bufElem =>
                  __ifThenElse(joinCond(bufElem, elem), {
                    k(concat_records[T, S, Res](bufElem, elem))
                  }, unit())
                }
              }
            }
          }
        }
      })
    }

    def groupByMapValues[K: TypeRep, S: TypeRep](groupByResult: GroupByResult[K, T])(
      par: Rep[T => K], pred: Option[Rep[T => Boolean]])(
        func: Rep[Array[T] => S])(
          adder: (Rep[_], QueryCPS[T]) => Unit): QueryCPS[(K, S)] = (k: (Rep[(K, S)]) => Rep[Unit]) => {
      type V = T

      // System.out.println(s"HERE!$monad")

      val GroupByResult(array, keyRevertIndex, eachBucketSize, _, keyIndex) =
        groupByResult

      val lastIndex = __newVarNamed(unit(0), "lastIndex")

      // printf(unit("start!"))
      foreach((elem: Rep[V]) => {
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

      val partitions = lastIndex

      Range(unit(0), partitions).foreach {
        __lambda { i =>
          // val arr = array_dropRight(array(i), eachBucketSize(i))
          val arr = array(i).dropRight(array(i).length - eachBucketSize(i))
          // System.out.println(s"arr size ${arr.size} bucket size ${eachBucketSize(i)}")
          val key = keyRevertIndex(i)
          val Def(Lambda(_, input, _)) = func
          adder(input, QueryCPS(arr))
          val newValue = inlineFunction(func, arr)
          k(Tuple2(key, newValue))
        }
      }
    }
  }

  object QueryCPS {
    def apply[T: TypeRep](arr: Rep[Array[T]]): QueryCPS[T] =
      QueryCPS { (k: Rep[T] => Rep[Unit]) =>
        QML.array_foreach_using_while(arr, k)
      }
    implicit def apply[T: TypeRep](k: (Rep[T] => Rep[Unit]) => Rep[Unit]): QueryCPS[T] =
      new QueryCPS[T] {
        def foreach(k2: Rep[T] => Rep[Unit]): Rep[Unit] = k(k2)
      }
  }

  type LoweredQuery[T] = QueryCPS[T]

  def __newLoweredQuery[T: TypeRep](array: Rep[Array[T]]): LoweredQuery[T] = QueryCPS(array)
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
      joinCond: (Rep[T], Rep[S]) => Rep[Boolean]): LoweredQuery[Res] =
    q1.hashJoin2(q2)(leftHash)(rightHash)(joinCond)
  def monadGroupByMapValues[T: TypeRep, K: TypeRep, S: TypeRep](
    query: LoweredQuery[T], groupByResult: GroupByResult[K, T])(
      par: Rep[T => K], pred: Option[Rep[T => Boolean]])(
        func: Rep[Array[T] => S])(
          adder: (Rep[_], LoweredQuery[T]) => Unit): LoweredQuery[(K, S)] =
    query.groupByMapValues(groupByResult)(par, pred)(func)(adder)
}
