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
 * Interface for lowers query monad operations.
 */
abstract class QueryMonadStreamLoweringInterface(val schema: Schema, override val IR: QueryEngineExp) extends RuleBasedTransformer[QueryEngineExp](IR) with StructProcessing[QueryEngineExp] {
  import IR._

  type LoweredQuery[T]

  def getLoweredQuery[T](sym: Rep[Query[T]]): LoweredQuery[T] = loweredMap(sym).asInstanceOf[LoweredQuery[T]]

  val loweredMap = mutable.Map[Rep[Any], LoweredQuery[Any]]()

  def constructLoweredQuery[T: TypeRep](array: Rep[Array[T]]): LoweredQuery[T]
  def monadFilter[T: TypeRep](query: LoweredQuery[T], p: Rep[T => Boolean]): LoweredQuery[T]
  def monadMap[T: TypeRep, S: TypeRep](query: LoweredQuery[T], f: Rep[T => S]): LoweredQuery[S]
  def monadForeach[T: TypeRep](query: LoweredQuery[T], f: Rep[T => Unit]): Unit
  def monadSortBy[T: TypeRep, S: TypeRep](query: LoweredQuery[T], f: Rep[T => S]): LoweredQuery[T]
  def monadCount[T: TypeRep](query: LoweredQuery[T]): Rep[Int]
  def monadSum[T: TypeRep](query: LoweredQuery[T]): Rep[T]

  rewrite += statement {
    case sym -> QueryNew2(array) =>
      val low = constructLoweredQuery(array)(array.tp.typeArguments(0).asInstanceOf[TypeRep[Any]])
      loweredMap += sym -> low
      sym
  }

  rewrite += statement {
    case sym -> QueryFilter(monad, p) =>
      val low = monadFilter(getLoweredQuery(monad), p)(p.tp.typeArguments(0).asInstanceOf[TypeRep[Any]])
      loweredMap += sym -> low
      sym
  }

  rewrite += statement {
    case sym -> QueryMap(monad, f) =>
      val low = monadMap(getLoweredQuery(monad), f)(f.tp.typeArguments(0).asInstanceOf[TypeRep[Any]], f.tp.typeArguments(1).asInstanceOf[TypeRep[Any]])
      loweredMap += sym -> low
      sym
  }

  rewrite += statement {
    case sym -> QueryForeach(monad, f) =>
      monadForeach(getLoweredQuery(monad), f)(f.tp.typeArguments(0).asInstanceOf[TypeRep[Any]])
      unit()
  }

  rewrite += statement {
    case sym -> QuerySortBy(monad, f) =>
      val low = monadMap(getLoweredQuery(monad), f)(f.tp.typeArguments(0).asInstanceOf[TypeRep[Any]], f.tp.typeArguments(1).asInstanceOf[TypeRep[Any]])
      loweredMap += sym -> low
      sym
  }

  // rewrite += rule {
  //   case QueryMinBy(monad, by) =>
  //     queryToStream(monad).minBy(by)(by.tp.typeArguments(1).asInstanceOf[TypeRep[Any]])
  // }

  rewrite += rule {
    case QueryCount(monad) =>
      monadCount(getLoweredQuery(monad))
  }

  // rewrite += rule {
  //   case QueryAvg(monad) =>
  //     queryToStream(monad).avg
  // }

  rewrite += rule {
    case QuerySum(monad) =>
      monadSum(getLoweredQuery(monad))
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
