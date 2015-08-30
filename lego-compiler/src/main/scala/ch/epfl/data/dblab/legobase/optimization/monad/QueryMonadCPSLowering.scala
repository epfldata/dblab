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

    // def groupByMapValues[K: TypeRep, S: TypeRep](par: Rep[T] => Rep[K])(f: QueryCPS[V] => Rep[S]): QueryCPS[(K, S)] = (k: (Rep[(K, S)]) => Rep[Unit]) => {
    // //    val hm = __newMultiMap[K, T]()
    // // for (elem <- this) {
    // //   hm.addBinding(par(elem), elem)
    // // }
    // // hm.foreach {
    // //   case (key, set) =>
    // //     val value = f(QueryCPS(set.asInstanceOf[scala.collection.mutable.HashSet[Any]].toArray.asInstanceOf[Array[V]]))
    // //     k((key, value))
    // // }
    // ???
    // }
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

  rewrite += rule {
    case QueryCount(monad) =>
      queryToCps(monad).count
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

  // rewrite += statement {
  //   case sym -> GroupedQueryMapValues(groupedMonad, func) =>
  //     val Def(QueryGroupBy(monad, par)) = groupedMonad
  //     implicit val typeK = groupedMonad.tp.typeArguments(0).asInstanceOf[TypeRep[Any]]
  //     implicit val typeV = groupedMonad.tp.typeArguments(1).asInstanceOf[TypeRep[Any]]
  //     implicit val typeS = func.tp.typeArguments(1).asInstanceOf[TypeRep[Any]]
  //     // queryGroupByMapValues(monad, Some(pred), par, func.asInstanceOf[Rep[Array[Any] => Any]])(typeK, typeV, typeS)
  //     groupedQueryMapValues(groupedMonad, func.asInstanceOf[Rep[Array[Any] => Any]])(typeK, typeV, typeS)

  // }
}
