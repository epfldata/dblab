package ch.epfl.data
package dblab
package transformers
package monad

import schema._
import utils.Logger
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
import scala.language.higherKinds

/**
 * Interface for lowering query monad operations.
 */
abstract class QueryMonadLoweringInterface(val schema: Schema, override val IR: QueryEngineExp) extends RuleBasedTransformer[QueryEngineExp](IR) with StructProcessing[QueryEngineExp] {
  import IR._

  val logger = Logger(getClass)

  def zeroValue[S: TypeRep]: Rep[S] = {
    val tp = typeRep[S]
    val v = sc.pardis.shallow.utils.DefaultValue(tp.name).asInstanceOf[S]
    infix_asInstanceOf(unit(v)(tp))(tp)
  }

  type LoweredQuery[T]

  def getLoweredQuery[T](sym: Rep[Query[T]]): LoweredQuery[T] = loweredMap(sym).asInstanceOf[LoweredQuery[T]]

  val loweredMap = mutable.Map[Rep[_], LoweredQuery[_]]()

  def __newLoweredQuery[T: TypeRep](array: Rep[Array[T]]): LoweredQuery[T]
  def monadFilter[T: TypeRep](query: LoweredQuery[T], p: Rep[T => Boolean]): LoweredQuery[T]
  def monadMap[T: TypeRep, S: TypeRep](query: LoweredQuery[T], f: Rep[T => S]): LoweredQuery[S]
  def monadForeach[T: TypeRep](query: LoweredQuery[T], f: Rep[T => Unit]): Unit
  def monadFoldLeft[T: TypeRep, S: TypeRep](query: LoweredQuery[T], z: Rep[S], f: Rep[(S, T) => S]): Rep[S] = {
    val foldResult = __newVarNamed[S](z, "foldResult")
    monadForeach(query, (elem: Rep[T]) => {
      __assign(foldResult, inlineFunction(f, readVar(foldResult), elem))
    })
    readVar(foldResult)
  }
  def monadSortBy[T: TypeRep, S: TypeRep](query: LoweredQuery[T], f: Rep[T => S]): LoweredQuery[T]
  def monadCount[T: TypeRep](query: LoweredQuery[T]): Rep[Int]
  def monadSum[T: TypeRep](query: LoweredQuery[T]): Rep[T]
  def monadAvg[T: TypeRep](query: LoweredQuery[T]): Rep[T]
  def monadMinBy[T: TypeRep, S: TypeRep](query: LoweredQuery[T], f: Rep[T => S]): Rep[T]
  def monadTake[T: TypeRep](query: LoweredQuery[T], n: Rep[Int]): LoweredQuery[T]
  def monadMergeJoin[T: TypeRep, S: TypeRep, Res: TypeRep](q1: LoweredQuery[T], q2: LoweredQuery[S])(
    ord: (Rep[T], Rep[S]) => Rep[Int])(joinCond: (Rep[T], Rep[S]) => Rep[Boolean]): LoweredQuery[Res]
  def monadLeftHashSemiJoin[T: TypeRep, S: TypeRep, R: TypeRep](q1: LoweredQuery[T], q2: LoweredQuery[S])(
    leftHash: Rep[T] => Rep[R])(rightHash: Rep[S] => Rep[R])(
      joinCond: (Rep[T], Rep[S]) => Rep[Boolean]): LoweredQuery[T]
  def monadHashJoin[T: TypeRep, S: TypeRep, R: TypeRep, Res: TypeRep](q1: LoweredQuery[T], q2: LoweredQuery[S])(
    leftHash: Rep[T] => Rep[R])(rightHash: Rep[S] => Rep[R])(
      joinCond: (Rep[T], Rep[S]) => Rep[Boolean]): LoweredQuery[Res]
  def monadGroupByMapValues[T: TypeRep, K: TypeRep, S: TypeRep](
    query: LoweredQuery[T], groupByResult: GroupByResult[K, T])(
      par: Rep[T => K], pred: Option[Rep[T => Boolean]])(
        func: Rep[Array[T] => S])(
          adder: (Rep[_], LoweredQuery[T]) => Unit): LoweredQuery[(K, S)]

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
  rewrite += statement {
    case sym -> QueryNew2(array) =>
      val low = __newLoweredQuery(array)(array.tp.typeArguments(0).asInstanceOf[TypeRep[Any]])
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

  rewrite += rule {
    case QueryFoldLeft(monad, z, f) =>
      monadFoldLeft(getLoweredQuery(monad), z, f)(f.tp.typeArguments(0).asInstanceOf[TypeRep[Any]], z.tp.asInstanceOf[TypeRep[Any]])
  }

  rewrite += statement {
    case sym -> QuerySortBy(monad, f) =>
      val low = monadSortBy(getLoweredQuery(monad), f)(f.tp.typeArguments(0).asInstanceOf[TypeRep[Any]], f.tp.typeArguments(1).asInstanceOf[TypeRep[Any]])
      loweredMap += sym -> low
      sym
  }

  rewrite += rule {
    case QueryMinBy(monad, f) =>
      monadMinBy(getLoweredQuery(monad), f)(f.tp.typeArguments(0).asInstanceOf[TypeRep[Any]], f.tp.typeArguments(1).asInstanceOf[TypeRep[Any]])
  }

  rewrite += rule {
    case QueryCount(monad) =>
      monadCount(getLoweredQuery(monad))
  }

  rewrite += rule {
    case QueryAvg(monad) =>
      monadAvg(getLoweredQuery(monad))
  }

  rewrite += rule {
    case QuerySum(monad) =>
      monadSum(getLoweredQuery(monad))
  }

  rewrite += statement {
    case sym -> QueryTake(monad, num) =>
      val low = monadTake(getLoweredQuery(monad), num)
      loweredMap += sym -> low
      sym
  }

  rewrite += remove {
    case JoinableQueryNew(joinMonad) => ()
  }

  rewrite += remove {
    case QueryGetList(monad) => ()
  }

  rewrite += statement {
    case sym -> JoinableQueryLeftHashSemiJoin(monad1, monad2, leftHash, rightHash, joinCond) => {
      val Def(JoinableQueryNew(Def(QueryGetList(m1)))) = monad1
      val Def(Lambda(lh, _, _)) = leftHash
      val Def(Lambda(rh, _, _)) = rightHash
      val Def(Lambda2(jc, _, _, _)) = joinCond
      val low = monadLeftHashSemiJoin(getLoweredQuery(m1),
        getLoweredQuery(monad2))(lh)(rh)(jc)(monad1.tp.typeArguments(0).asInstanceOf[TypeRep[Record]],
          monad2.tp.typeArguments(0).asInstanceOf[TypeRep[Record]],
          leftHash.tp.typeArguments(1).asInstanceOf[TypeRep[Any]])
      loweredMap += sym -> low.asInstanceOf[LoweredQuery[Any]]
      sym
    }
  }

  rewrite += statement {
    case sym -> JoinableQueryHashJoin(monad1, monad2, leftHash, rightHash, joinCond) => {
      val Def(JoinableQueryNew(Def(QueryGetList(m1)))) = monad1
      val Def(Lambda(lh, _, _)) = leftHash
      val Def(Lambda(rh, _, _)) = rightHash
      val Def(Lambda2(jc, _, _, _)) = joinCond
      val low = monadHashJoin(getLoweredQuery(m1), getLoweredQuery(monad2))(lh)(rh)(jc)(
        monad1.tp.typeArguments(0).asInstanceOf[TypeRep[Record]],
        monad2.tp.typeArguments(0).asInstanceOf[TypeRep[Record]],
        leftHash.tp.typeArguments(1).asInstanceOf[TypeRep[Any]],
        sym.tp.typeArguments(0).asInstanceOf[TypeRep[Any]])
      loweredMap += sym -> low.asInstanceOf[LoweredQuery[Any]]
      sym
    }
  }

  rewrite += statement {
    case sym -> JoinableQueryMergeJoin(monad1, monad2, ord, joinCond) => {
      val Def(JoinableQueryNew(Def(QueryGetList(m1)))) = monad1
      val Def(Lambda2(or, _, _, _)) = ord
      val Def(Lambda2(jc, _, _, _)) = joinCond
      val low = monadMergeJoin(getLoweredQuery(m1), getLoweredQuery(monad2))(or)(jc)(monad1.tp.typeArguments(0).asInstanceOf[TypeRep[Record]],
        monad2.tp.typeArguments(0).asInstanceOf[TypeRep[Record]],
        sym.tp.typeArguments(0).asInstanceOf[TypeRep[Any]])
      loweredMap += sym -> low.asInstanceOf[LoweredQuery[Any]]
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

  case class GroupByResult[K, V](partitionedArray: Rep[Array[Array[V]]], keyRevertIndex: Rep[Array[K]],
                                 eachBucketSize: Rep[Array[Int]], partitions: Rep[Int], keyIndex: Rep[HashMap[K, Int]])

  val groupByResults = scala.collection.mutable.Map[Rep[Any], GroupByResult[Any, Any]]()

  rewrite += rule {
    case GenericEngineRunQueryObject(b) =>
      for ((key, groupByInfo) <- groupBysInfo) {
        val groupByResult = initGroupByArray(groupByInfo.monad, groupByInfo.par)(
          groupByInfo.par.tp.typeArguments(0).asInstanceOf[TypeRep[Any]], groupByInfo.par.tp.typeArguments(1).asInstanceOf[TypeRep[Any]])
        groupByResults(groupByInfo.monad) = groupByResult
        logger.debug(s"ADDED ${groupByInfo.monad}!")
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
      val groupByResult = groupByResults(monad)
      val low = monadGroupByMapValues(getLoweredQuery(monad), groupByResult)(par, pred)(
        func.asInstanceOf[Rep[Array[Any] => Any]])((input, lq) => loweredMap += input -> lq)(typeV, typeK, typeS)
      loweredMap += sym -> low
      sym
  }
}
