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

/**
 * Lowers query monad operations.
 */
class QueryMonadLowering(val schema: Schema, override val IR: LegoBaseExp) extends RuleBasedTransformer[LegoBaseExp](IR) with StructProcessing[LegoBaseExp] {
  import IR._

  def array_filter[T: TypeRep](array: Rep[Array[T]], p: Rep[T => Boolean]): Rep[Array[T]] = {
    val Def(IR.Lambda(pred, _, _)) = p
    val size = __newVarNamed[Int](unit(0), "arraySize")
    array_foreach(array, (elem: Rep[T]) => __ifThenElse(pred(elem),
      __assign(size, readVar(size) + unit(1)), unit()))
    val resultSize = readVar(size)
    val resultArray = __newArray[T](resultSize)
    val counter = __newVarNamed[Int](unit(0), "arrayCounter")
    array_foreach(array, (elem: Rep[T]) => {
      __ifThenElse(pred(elem), {
        resultArray(readVar(counter)) = elem
        __assign(counter, readVar(counter) + unit(1))
      },
        unit())

    })
    resultArray
  }

  // def array_dropRight[T: TypeRep](array: Rep[Array[T]], keepNum: Rep[Int]): Rep[Array[T]] = {
  //   val resultSize = keepNum
  //   val resultArray = __newArray[T](resultSize)
  //   Range(unit(0), resultSize).foreach {
  //     __lambda { i =>
  //       resultArray(i) = array(i)
  //     }
  //   }
  //   resultArray
  // }

  def array_map[T: TypeRep, S: TypeRep](array: Rep[Array[T]], f: Rep[T => S]): Rep[Array[S]] = {
    val Def(IR.Lambda(func, _, _)) = f
    val size = __newVarNamed[Int](unit(0), "arraySize")
    val resultArray = __newArray[S](array.length)
    val counter = __newVarNamed[Int](unit(0), "arrayCounter")
    array_foreach(array, (elem: Rep[T]) => {
      resultArray(readVar(counter)) = func(elem)
      __assign(counter, readVar(counter) + unit(1))
    })
    resultArray
  }

  def array_sum[T: TypeRep](array: Rep[Array[T]]): Rep[T] = {
    // TODO handle the generic case using numeric
    assert(typeRep[T] == DoubleType)
    val sumResult = __newVarNamed[Double](unit(0.0), "sumResult")
    array_foreach(array, (elem: Rep[T]) => {
      __assign(sumResult, readVar(sumResult) + elem.asInstanceOf[Rep[Double]])
    })
    readVar(sumResult).asInstanceOf[Rep[T]]
  }

  def array_foldLeft[T: TypeRep, S: TypeRep](array: Rep[Array[T]], z: Rep[S], f: Rep[(S, T) => S]): Rep[S] = {
    val foldResult = __newVarNamed[S](z, "foldResult")
    array_foreach(array, (elem: Rep[T]) => {
      __assign(foldResult, inlineFunction(f, readVar(foldResult), elem))
    })
    readVar(foldResult)
  }

  def array_avg[T: TypeRep](array: Rep[Array[T]]): Rep[T] = {
    // TODO handle the generic case using numeric
    assert(typeRep[T] == DoubleType)
    (array_sum(array).asInstanceOf[Rep[Double]] / array.length).asInstanceOf[Rep[T]]
  }

  rewrite += rule {
    case QueryNew2(array) =>
      array
  }

  rewrite += rule {
    case QueryFilter(monad, p) =>
      val array = apply(monad).asInstanceOf[Rep[Array[Any]]]
      array_filter(array, p)(array.tp.typeArguments(0).asInstanceOf[TypeRep[Any]])
  }

  rewrite += rule {
    case QueryMap(monad, f) =>
      val array = apply(monad).asInstanceOf[Rep[Array[Any]]]
      array_map(array, f)(array.tp.typeArguments(0).asInstanceOf[TypeRep[Any]], f.tp.typeArguments(1).asInstanceOf[TypeRep[Any]])
  }

  rewrite += rule {
    case QueryForeach(monad, f) =>
      val array = apply(monad).asInstanceOf[Rep[Array[Any]]]
      val Def(IR.Lambda(func, _, _)) = f
      array_foreach(array, func)(array.tp.typeArguments(0).asInstanceOf[TypeRep[Any]])
  }

  rewrite += rule {
    case QuerySum(monad) =>
      val array = apply(monad).asInstanceOf[Rep[Array[Any]]]
      array_sum(array)(array.tp.typeArguments(0).asInstanceOf[TypeRep[Any]])
  }

  rewrite += rule {
    case QueryCount(monad) =>
      val array = apply(monad).asInstanceOf[Rep[Array[Any]]]
      array.length
  }

  rewrite += rule {
    case QueryAvg(monad) =>
      val array = apply(monad).asInstanceOf[Rep[Array[Any]]]
      array_avg(array)(array.tp.typeArguments(0).asInstanceOf[TypeRep[Any]])
  }

  rewrite += rule {
    case QueryFoldLeft(monad, z, f) =>
      val array = apply(monad).asInstanceOf[Rep[Array[Any]]]
      array_foldLeft(array, z, f)(array.tp.typeArguments(0).asInstanceOf[TypeRep[Any]], z.tp.asInstanceOf[TypeRep[Any]])
  }

  rewrite += rule {
    case QueryTake(monad, num) =>
      val array = apply(monad).asInstanceOf[Rep[Array[Any]]]
      arrayDropRight(array, array.length - num)(array.tp.typeArguments(0).asInstanceOf[TypeRep[Any]])
  }

  rewrite += statement {
    case sym -> QueryGroupBy(monad, par) => {
      implicit val typeK = par.tp.typeArguments(1).asInstanceOf[TypeRep[Any]]
      implicit val typeV = par.tp.typeArguments(0).asInstanceOf[TypeRep[Any]]
      val result = queryGroupBy(monad, None, par)(typeK, typeV)
      groupByResults(sym) = result
      result.partitionedArray
    }
  }

  rewrite += statement {
    case sym -> QueryFilteredGroupBy(monad, pred, par) => {
      implicit val typeK = par.tp.typeArguments(1).asInstanceOf[TypeRep[Any]]
      implicit val typeV = par.tp.typeArguments(0).asInstanceOf[TypeRep[Any]]
      val result = queryGroupBy(monad, Some(pred), par)(typeK, typeV)
      groupByResults(sym) = result
      result.partitionedArray
    }
  }

  case class GroupByResult[K, V](partitionedArray: Rep[Array[Array[V]]], keyRevertIndex: Rep[Array[K]], eachBucketSize: Rep[Array[Int]], partitions: Rep[Int])
  val groupByResults = scala.collection.mutable.Map[Rep[Any], GroupByResult[Any, Any]]()

  def queryGroupBy[K: TypeRep, V: TypeRep](monad: Rep[Query[V]], pred: Option[Rep[V => Boolean]], par: Rep[V => K]): GroupByResult[K, V] = {
    val originalArray = apply(monad).asInstanceOf[Rep[Array[V]]]
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
    Range(unit(0), MAX_SIZE).foreach {
      __lambda { i =>
        val arraySize = originalArray.length / MAX_SIZE * unit(8)
        // val arraySize = originalArray.length
        // val arraySize = unit(128)
        array(i) = __newArray[V](arraySize)
        eachBucketSize(i) = unit(0)
      }
    }

    // printf(unit("start!"))
    array_foreach(originalArray, (elem: Rep[V]) => {
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
    GroupByResult(array, keyRevertIndex, eachBucketSize, lastIndex)
  }

  // def queryGroupByMapValues[K: TypeRep, V: TypeRep, S: TypeRep](monad: Rep[Query[V]], pred: Option[Rep[V => Boolean]], par: Rep[V => K], func: Rep[Array[V] => S]): Rep[Any] = {
  def groupedQueryMapValues[K: TypeRep, V: TypeRep, S: TypeRep](groupedMonad: Rep[GroupedQuery[K, V]], func: Rep[Array[V] => S]): Rep[Any] = {
    val GroupByResult(array, keyRevertIndex, eachBucketSize, partitions) =
      // queryGroupBy(monad, pred, par)
      groupByResults(groupedMonad.asInstanceOf[Rep[Any]]).asInstanceOf[GroupByResult[K, V]]
    val resultArray = __newArray[(K, S)](partitions)
    Range(unit(0), partitions).foreach {
      __lambda { i =>
        // val arr = array_dropRight(array(i), eachBucketSize(i))
        val arr = array(i).dropRight(array(i).length - eachBucketSize(i))
        // System.out.println(s"arr size ${arr.size} bucket size ${eachBucketSize(i)}")
        val key = keyRevertIndex(i)
        val newValue = inlineFunction(func, arr)
        resultArray(i) = Tuple2(key, newValue)
      }
    }
    resultArray.asInstanceOf[Rep[Any]]
  }

  rewrite += rule {
    case GroupedQueryMapValues(groupedMonad, func) =>
      implicit val typeK = groupedMonad.tp.typeArguments(0).asInstanceOf[TypeRep[Any]]
      implicit val typeV = groupedMonad.tp.typeArguments(1).asInstanceOf[TypeRep[Any]]
      implicit val typeS = func.tp.typeArguments(1).asInstanceOf[TypeRep[Any]]
      // queryGroupByMapValues(monad, Some(pred), par, func.asInstanceOf[Rep[Array[Any] => Any]])(typeK, typeV, typeS)
      groupedQueryMapValues(groupedMonad, func.asInstanceOf[Rep[Array[Any] => Any]])(typeK, typeV, typeS)

  }

  object TupleNType {
    def unapply[T](tp: TypeRep[T]): Option[scala.Seq[TypeRep[Any]]] = tp match {
      case Tuple2Type(tp1, tp2)           => Some(scala.Seq(tp1, tp2))
      case Tuple3Type(tp1, tp2, tp3)      => Some(scala.Seq(tp1, tp2, tp3))
      case Tuple4Type(tp1, tp2, tp3, tp4) => Some(scala.Seq(tp1, tp2, tp3, tp4))
      case _                              => None
    }
  }

  object TupleNCreate {
    def unapply[T](exp: Rep[T]): Option[scala.Seq[Rep[Any]]] = exp match {
      case Def(Tuple2ApplyObject(x, y))       => Some(scala.Seq(x, y))
      case Def(Tuple3ApplyObject(x, y, z))    => Some(scala.Seq(x, y, z))
      case Def(Tuple4ApplyObject(x, y, z, t)) => Some(scala.Seq(x, y, z, t))
      case _                                  => None
    }
  }

  // TODO should be moved to SC?
  def ordering_minus[T: TypeRep](num1: Rep[T], num2: Rep[T]): Rep[Int] = typeRep[T] match {
    case IntType    => num1.asInstanceOf[Rep[Int]] - num2.asInstanceOf[Rep[Int]]
    case DoubleType => (num1.asInstanceOf[Rep[Double]] - num2.asInstanceOf[Rep[Double]]).toInt
    case StringType => (num1, num2) match {
      case (Def(OptimalStringString(str1)), Def(OptimalStringString(str2))) => str1 diff str2
      case (str1: Rep[String], str2: Rep[String])                           => str1 diff str2
    }
    case TupleNType(tps) => {
      val TupleNCreate(elems1) = num1
      val TupleNCreate(elems2) = num2
      elems1.zip(elems2).zip(tps).foldLeft(unit(0))((acc, cur) => {
        __ifThenElse(acc __== unit(0), {
          val e1 = cur._1._1
          val e2 = cur._1._2
          val tp = cur._2
          ordering_minus(e1, e2)(tp)
        }, acc)
      })
    }
    // case Tuple2Type(tp1, tp2) => {
    //   class Tp1
    //   class Tp2
    //   implicit val ttp1 = tp1.asInstanceOf[TypeRep[Tp1]]
    //   implicit val ttp2 = tp2.asInstanceOf[TypeRep[Tp2]]
    //   val tup1 = num1.asInstanceOf[Rep[(Tp1, Tp2)]]
    //   val tup2 = num2.asInstanceOf[Rep[(Tp1, Tp2)]]
    //   val e11 = tup1._1
    //   val e21 = tup2._1
    //   val c1 = ordering_minus(e11, e21)
    //   __ifThenElse(c1 __== unit(0), {
    //     val e12 = tup1._2
    //     val e22 = tup2._2
    //     ordering_minus(e12, e22)
    //   }, {
    //     c1
    //   })
    // }
    // case Tuple3Type(tp1, tp2, tp3) => {
    //   class Tp1
    //   class Tp2
    //   class Tp3
    //   implicit val ttp1 = tp1.asInstanceOf[TypeRep[Tp1]]
    //   implicit val ttp2 = tp2.asInstanceOf[TypeRep[Tp2]]
    //   implicit val ttp3 = tp3.asInstanceOf[TypeRep[Tp3]]
    //   implicit class Tuple3NOps(self: Rep[(Tp1, Tp2, Tp3)]) {
    //     def _1: Rep[Tp1] = self match {
    //       case Def(Tuple3ApplyObject(x, y, z)) => x
    //     }
    //     def _2: Rep[Tp2] = self match {
    //       case Def(Tuple3ApplyObject(x, y, z)) => y
    //     }
    //     def _3: Rep[Tp3] = self match {
    //       case Def(Tuple3ApplyObject(x, y, z)) => z
    //     }
    //   }
    //   val tup1 = num1.asInstanceOf[Rep[(Tp1, Tp2, Tp3)]]
    //   val tup2 = num2.asInstanceOf[Rep[(Tp1, Tp2, Tp3)]]
    //   val e11 = tup1._1
    //   val e21 = tup2._1
    //   val c1 = ordering_minus(e11, e21)
    //   __ifThenElse(c1 __== unit(0), {
    //     val e12 = tup1._2
    //     val e22 = tup2._2
    //     val c2 = ordering_minus(e12, e22)
    //     __ifThenElse(c2 __== unit(0), {
    //       val e13 = tup1._3
    //       val e23 = tup2._3
    //       ordering_minus(e13, e23)
    //     }, {
    //       c2
    //     })
    //   }, {
    //     c1
    //   })
    // }
  }

  def array_sortBy[T: TypeRep, S: TypeRep](array: Rep[Array[T]], sortFunction: Rep[T => S]): Rep[Array[T]] = {
    val resultArray = __newArray[T](array.length)
    val counter = __newVarNamed[Int](unit(0), "arrayCounter")
    val treeSet = __newTreeSet2(Ordering[T](__lambda { (x, y) =>
      ordering_minus(inlineFunction(sortFunction, x), inlineFunction(sortFunction, y))
    }))
    array_foreach(array, (elem: Rep[T]) => {
      treeSet += elem
      __assign(counter, readVar(counter) + unit(1))
    })
    Range(unit(0), treeSet.size).foreach(__lambda { i =>
      val elem = treeSet.head
      treeSet -= elem
      resultArray(i) = elem
    })
    resultArray
  }

  def array_minBy[T: TypeRep, S: TypeRep](array: Rep[Array[T]], by: Rep[T => S]): Rep[T] = {
    val minResult = __newVarNamed[T](unit(null).asInstanceOf[Rep[T]], "minResult")
    def compare(x: Rep[T], y: Rep[T]): Rep[Int] =
      ordering_minus(inlineFunction(by, x), inlineFunction(by, y))
    array_foreach(array, (elem: Rep[T]) => {
      __ifThenElse((readVar(minResult) __== unit(null)) || compare(elem, readVar(minResult)) < unit(0), {
        __assign(minResult, elem)
      }, {
        unit()
      })
    })
    readVar(minResult)
  }

  rewrite += statement {
    case sym -> QuerySortBy(monad, sortFunction) => {
      val array = apply(monad).asInstanceOf[Rep[Array[Any]]]
      array_sortBy(array, sortFunction)(array.tp.typeArguments(0).asInstanceOf[TypeRep[Any]], sortFunction.tp.typeArguments(1).asInstanceOf[TypeRep[Any]])
    }
  }

  rewrite += rule {
    case QueryMinBy(monad, by) =>
      val array = apply(monad).asInstanceOf[Rep[Array[Any]]]
      array_minBy(array, by)(array.tp.typeArguments(0).asInstanceOf[TypeRep[Any]], by.tp.typeArguments(1).asInstanceOf[TypeRep[Any]])
  }

  rewrite += rule {
    case JoinableQueryNew(joinMonad) => apply(joinMonad)
  }

  rewrite += rule {
    case QueryGetList(monad) => apply(monad)
  }

  def array_foreach_using_while[T: TypeRep](arr: Rep[Array[T]], f: Rep[T] => Rep[Unit]): Rep[Unit] = {
    val counter = __newVar[Int](unit(0))
    __whileDo(readVar(counter) < arr.length, {
      val e = arr(readVar(counter))
      f(e)
      __assign(counter, readVar(counter) + unit(1))
    })
  }

  def hashJoin[T: TypeRep, S: TypeRep, R: TypeRep, Res: TypeRep](array1: Rep[Array[T]], array2: Rep[Array[S]], leftHash: Rep[T => R], rightHash: Rep[S => R], joinCond: Rep[(T, S) => Boolean]): Rep[Array[Res]] = {
    assert(typeRep[Res].isInstanceOf[RecordType[_]])
    // TODO generalize to the cases other than primary key - foreign key relations
    // val maxSize = __ifThenElse(array1.length > array2.length, array1.length, array2.length)
    val maxSize = unit(100000000)
    val res = __newArray[Res](maxSize)(concat_types[T, S, Res])
    val counter = __newVar[Int](unit(0))
    val hm = __newMultiMap[R, T]()
    // System.out.println(concat_types[T, S, Res])
    array_foreach_using_while(array1, (elem: Rep[T]) => {
      hm.addBinding(leftHash(elem), elem)
    })
    array_foreach_using_while(array2, (elem: Rep[S]) => {
      val k = rightHash(elem)
      hm.get(k) foreach {
        __lambda { tmpBuffer =>
          tmpBuffer foreach {
            __lambda { bufElem =>
              __ifThenElse(joinCond(bufElem, elem), {
                res(readVar(counter)) =
                  concat_records[T, S, Res](bufElem, elem)
                __assign(counter, readVar(counter) + unit(1))
              }, unit())
            }
          }
        }
      }
    })
    res.dropRight(maxSize - readVar(counter))
  }

  def leftSemiHashJoin[T: TypeRep, S: TypeRep, R: TypeRep](array1: Rep[Array[T]], array2: Rep[Array[S]], leftHash: Rep[T => R], rightHash: Rep[S => R], joinCond: Rep[(T, S) => Boolean]): Rep[Array[T]] = {
    // TODO generalize to the cases other than primary key - foreign key relations
    // val maxSize = __ifThenElse(array1.length > array2.length, array1.length, array2.length)
    val maxSize = unit(100000000)
    val res = __newArray[T](maxSize)
    val counter = __newVar[Int](unit(0))
    val hm = __newMultiMap[R, S]()
    // System.out.println(concat_types[T, S, Res])
    array_foreach_using_while(array2, (elem: Rep[S]) => {
      hm.addBinding(rightHash(elem), elem)
    })
    array_foreach_using_while(array1, (elem: Rep[T]) => {
      val k = leftHash(elem)
      hm.get(k) foreach {
        __lambda { tmpBuffer =>
          __ifThenElse(tmpBuffer.exists(
            __lambda { bufElem =>
              joinCond(elem, bufElem)
            }), {
            res(readVar(counter)) =
              elem
            __assign(counter, readVar(counter) + unit(1))
          }, unit())
        }
      }
    })
    res.dropRight(maxSize - readVar(counter))
  }

  rewrite += statement {
    case sym -> JoinableQueryHashJoin(monad1, monad2, leftHash, rightHash, joinCond) => {
      val arr1 = apply(monad1).asInstanceOf[Rep[Array[Any]]]
      val arr2 = apply(monad2).asInstanceOf[Rep[Array[Any]]]
      hashJoin(arr1,
        arr2,
        leftHash.asInstanceOf[Rep[Any => Any]],
        rightHash.asInstanceOf[Rep[Any => Any]],
        joinCond.asInstanceOf[Rep[(Any, Any) => Boolean]])(arr1.tp.typeArguments(0).asInstanceOf[TypeRep[Any]],
          arr2.tp.typeArguments(0).asInstanceOf[TypeRep[Any]],
          leftHash.tp.typeArguments(1).asInstanceOf[TypeRep[Any]],
          sym.tp.typeArguments(0).asInstanceOf[TypeRep[Any]])
    }
  }

  rewrite += statement {
    case sym -> JoinableQueryLeftHashSemiJoin(monad1, monad2, leftHash, rightHash, joinCond) => {
      val arr1 = apply(monad1).asInstanceOf[Rep[Array[Any]]]
      val arr2 = apply(monad2).asInstanceOf[Rep[Array[Any]]]
      leftSemiHashJoin(arr1,
        arr2,
        leftHash.asInstanceOf[Rep[Any => Any]],
        rightHash.asInstanceOf[Rep[Any => Any]],
        joinCond.asInstanceOf[Rep[(Any, Any) => Boolean]])(arr1.tp.typeArguments(0).asInstanceOf[TypeRep[Any]],
          arr2.tp.typeArguments(0).asInstanceOf[TypeRep[Any]],
          leftHash.tp.typeArguments(1).asInstanceOf[TypeRep[Any]])
    }
  }
}
