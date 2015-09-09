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
class QueryMonadCPSLowering(val schema: Schema, override val IR: LegoBaseExp) extends RuleBasedTransformer[LegoBaseExp](IR) with StructCollector[LegoBaseExp] {
  import IR._

  abstract class QueryCPS[T: TypeRep] {
    val tp = typeRep[T]
    def foreach(k: Rep[T] => Rep[Unit]): Rep[Unit]
    def map[S: TypeRep](f: Rep[T] => Rep[S]): QueryCPS[S] = (k: Rep[S] => Rep[Unit]) => {
      foreach(e => k(f(e)))
    }
    def filter(p: Rep[T] => Rep[Boolean]): QueryCPS[T] = (k: Rep[T] => Rep[Unit]) => {
      foreach(e => __ifThenElse(p(e), k(e), unit()))
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
      (sum.asInstanceOf[Rep[Double]] / count).asInstanceOf[Rep[T]]
    }

    def sum: Rep[T] = {
      assert(typeRep[T] == DoubleType)
      val sumResult = __newVarNamed[Double](unit(0.0), "sumResult")
      foreach(elem => {
        __assign(sumResult, readVar(sumResult) + elem.asInstanceOf[Rep[Double]])
      })
      readVar(sumResult).asInstanceOf[Rep[T]]
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
            __ifThenElse(tmpBuffer.exists(
              __lambda { bufElem =>
                joinCond(elem, bufElem)
              }), {
              k(elem)
            }, unit())
          }
        }
      }
    }

    def groupByMapValues[K: TypeRep, S: TypeRep](par: Rep[T => K], pred: Option[Rep[T => Boolean]])(func: Rep[Array[T] => S]): QueryCPS[(K, S)] = (k: (Rep[(K, S)]) => Rep[Unit]) => {
      type V = T
      def sizeByCardinality: Int = schema.stats.getCardinalityOrElse(typeRep[K].name, 8).toInt
      val max_partitions = par match {
        case Def(Lambda(_, i, Block(stmts, Def(StructImmutableField(struct, name))))) if i == struct && stmts.size == 1 =>
          schema.stats.getDistinctAttrValuesOrElse(name, sizeByCardinality)
        case _ =>
          sizeByCardinality
      }

      System.out.println(typeRep[K] + "-" + max_partitions)
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
      val arraySize = this.count / MAX_SIZE * unit(4)
      Range(unit(0), MAX_SIZE).foreach {
        __lambda { i =>
          // val arraySize = originalArray.length
          // val arraySize = unit(128)
          array(i) = __newArray[V](arraySize)
          eachBucketSize(i) = unit(0)
        }
      }

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
          cpsMap += input -> QueryCPS(arr).asInstanceOf[QueryCPS[Any]]
          val newValue = inlineFunction(func, arr)
          k(Tuple2(key, newValue))
        }
      }
    }
  }

  object QueryCPS {
    def apply[T: TypeRep](arr: Rep[Array[T]]): QueryCPS[T] =
      QueryCPS { (k: Rep[T] => Rep[Unit]) =>
        array_foreach(arr, k)
      }
    implicit def apply[T: TypeRep](k: (Rep[T] => Rep[Unit]) => Rep[Unit]): QueryCPS[T] =
      new QueryCPS[T] {
        def foreach(k2: Rep[T] => Rep[Unit]): Rep[Unit] = k(k2)
      }
  }

  implicit def queryToCps[T](sym: Rep[Query[T]]): QueryCPS[T] = {
    System.out.println(s"finding cps for $sym")
    val cps = cpsMap(sym.asInstanceOf[Rep[Any]]).asInstanceOf[QueryCPS[T]]
    System.out.println(s"tp associated to sym $sym is: ${cps.tp}")
    cps
  }

  val cpsMap = mutable.Map[Rep[Any], QueryCPS[Any]]()

  rewrite += statement {
    case sym -> QueryNew2(array) =>
      val cps = QueryCPS(array)(array.tp.typeArguments(0).asInstanceOf[TypeRep[Any]])
      cpsMap += sym -> cps
      sym
  }

  rewrite += statement {
    case sym -> QueryFilter(monad, p) =>
      val Def(Lambda(pred, _, _)) = p
      val cps = monad.filter(pred)
      cpsMap += sym -> cps
      // System.out.println(s"$sym -> $cps added to map")
      sym
  }

  rewrite += statement {
    case sym -> QueryMap(monad, f) =>
      val Def(Lambda(func, _, _)) = f
      val cps = monad.map(func)(f.tp.typeArguments(1).asInstanceOf[TypeRep[Any]])
      cpsMap += sym -> cps
      sym
  }

  rewrite += statement {
    case sym -> QueryForeach(monad, f) =>
      val Def(Lambda(func, _, _)) = f
      val cps = monad.foreach(func)
      cps
  }

  rewrite += rule {
    case QueryCount(monad) =>
      queryToCps(monad).count
  }

  rewrite += rule {
    case QueryAvg(monad) =>
      queryToCps(monad).avg
  }

  rewrite += rule {
    case QuerySum(monad) =>
      queryToCps(monad).sum
  }

  rewrite += remove {
    case JoinableQueryNew(joinMonad) => apply(joinMonad)
  }

  rewrite += remove {
    case QueryGetList(monad) => apply(monad)
  }

  rewrite += statement {
    case sym -> JoinableQueryLeftHashSemiJoin(monad1, monad2, leftHash, rightHash, joinCond) => {
      val Def(JoinableQueryNew(Def(QueryGetList(m1)))) = monad1
      val Def(Lambda(lh, _, _)) = leftHash
      val Def(Lambda(rh, _, _)) = rightHash
      val Def(Lambda2(jc, _, _, _)) = joinCond
      val cps = m1.leftHashSemiJoin2(monad2)(lh)(rh)(jc)(monad2.tp.typeArguments(0).asInstanceOf[TypeRep[Record]],
        leftHash.tp.typeArguments(1).asInstanceOf[TypeRep[Any]])
      cpsMap += sym -> cps.asInstanceOf[QueryCPS[Any]]
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
    case (sym -> _) if mapValuesFuncs.contains(sym) =>
      ()
  }

  rewrite += statement {
    case sym -> GroupedQueryMapValues(groupedMonad, func) =>
      val Def(QueryGroupBy(monad, par)) = groupedMonad
      implicit val typeK = groupedMonad.tp.typeArguments(0).asInstanceOf[TypeRep[Any]]
      implicit val typeV = groupedMonad.tp.typeArguments(1).asInstanceOf[TypeRep[Any]]
      implicit val typeS = func.tp.typeArguments(1).asInstanceOf[TypeRep[Any]]
      // queryGroupByMapValues(monad, Some(pred), par, func.asInstanceOf[Rep[Array[Any] => Any]])(typeK, typeV, typeS)
      // groupedQueryMapValues(groupedMonad, func.asInstanceOf[Rep[Array[Any] => Any]])(typeK, typeV, typeS)
      val cps = monad.groupByMapValues(par, None)(func.asInstanceOf[Rep[Array[Any] => Any]])(typeK, typeS)
      cpsMap += sym -> cps.asInstanceOf[QueryCPS[Any]]
      sym
  }
}