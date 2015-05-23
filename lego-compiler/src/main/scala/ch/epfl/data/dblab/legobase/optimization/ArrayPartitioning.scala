package ch.epfl.data
package dblab.legobase
package optimization

import scala.language.implicitConversions
import sc.pardis.ir._
import reflect.runtime.universe.{ TypeTag, Type }
import sc.pardis.optimization._
import deep._
import sc.pardis.types._
import sc.pardis.types.PardisTypeImplicits._
import sc.pardis.shallow.utils.DefaultValue

// TODO there should be no need for queryNumber thanks to Schema information

/**
 * A transformer for partitioning and indexing the arrays whenever possible.
 *
 * Given an input program first it identify all arrays and the iterations over those arrays.
 * Then it looks for the filters over the elements of all those arrays. If it identifies
 * that there is filter based on a range of values, then it partitions the array into different
 * chuncks in the loading time. Then, it iterates only over the relavant partitions of the arrays
 * in the query processing time.
 *
 * TODO maybe add an example
 *
 * @param IR the polymorphic embedding trait which contains the reified program.
 * @param queryNumber specifies the TPCH query number (TODO should be removed)
 */
class ArrayPartitioning(override val IR: LoweringLegoBase, queryNumber: Int) extends RuleBasedTransformer[LoweringLegoBase](IR) {
  import IR._

  import scala.collection.mutable

  val possibleRangeFors = mutable.Set[Rep[Unit]]()
  val rangeForIndex = mutable.Map[Rep[Unit], Rep[Int]]()
  val rangeArray = mutable.Map[Rep[Unit], Rep[Array[Any]]]()
  val rangeArrayApply = mutable.Map[Rep[Unit], Rep[Any]]()
  val rangeElemField = mutable.Map[Rep[Unit], Rep[Any]]()
  val rangeElemFieldConstraints = mutable.Map[Rep[Unit], mutable.ArrayBuffer[Constraint]]()

  val arraysInfo = mutable.Set[ArrayInfo[Any]]()
  val arraysInfoConstraints = mutable.Map[ArrayInfo[Any], List[Constraint]]()
  val arraysInfoLowerBound = mutable.Map[ArrayInfo[Any], Int]()
  val arraysInfoUpperBound = mutable.Map[ArrayInfo[Any], Int]()
  val arraysInfoBuckets = mutable.Map[ArrayInfo[Any], Int]()
  val arraysInfoArray = mutable.Map[ArrayInfo[Any], Rep[Array[Any]]]()
  val arraysInfoCount = mutable.Map[ArrayInfo[Any], Rep[Array[Int]]]()
  val arraysInfoElem = mutable.Map[ArrayInfo[Any], Rep[Any]]()

  implicit def arrayInfoToArrayInfoAny[T](arrayInfo: ArrayInfo[T]): ArrayInfo[Any] = arrayInfo.asInstanceOf[ArrayInfo[Any]]

  sealed trait Constraint {
    val elemField: Rep[Any]
    def elemTpe: TypeRep[Any] = elemField match {
      case Def(StructImmutableField(elem, _)) => elem.tp
      case _                                  => ???
    }
    def bound: Rep[Any]
    def field: Option[String] = elemField match {
      case Def(StructImmutableField(_, f)) => Some(f)
      case _                               => None
    }
    def simplify: Constraint
    def isForDate: Boolean = field match {
      case Some(f) => f.endsWith("DATE")
      case None    => false
    }
  }

  def simplifyExpInt(exp: Rep[Int]): Rep[Int] = exp match {
    case Def(GenericEngineParseDateObject(Constant(d))) =>
      val data = d.split("-").map(x => x.toInt)
      unit((data(0) * 10000) + (data(1) * 100) + data(2))
    case _ => exp
  }

  case class LessThan(elemField: Rep[Any], upperBound: Rep[Int]) extends Constraint {
    def bound = upperBound.asInstanceOf[Rep[Any]]
    def simplify = copy(upperBound = simplifyExpInt(this.upperBound))
  }
  case class GreaterThan(elemField: Rep[Any], lowerBound: Rep[Int]) extends Constraint {
    def bound = lowerBound.asInstanceOf[Rep[Any]]
    def simplify = copy(lowerBound = simplifyExpInt(this.lowerBound))
  }
  case class GreaterThanOffset(elemField: Rep[Any], lowerBound: Rep[Int], offset: Rep[Int]) extends Constraint {
    def bound = lowerBound.asInstanceOf[Rep[Any]]
    def simplify = {
      val newConst = copy(lowerBound = simplifyExpInt(this.lowerBound))
      (newConst.lowerBound, newConst.offset) match {
        // handles only the case for date
        case (Constant(a1), Constant(a2)) if isForDate => {
          val simpleDateFormatter = new java.text.SimpleDateFormat("yyyyMMdd")
          val date1 = simpleDateFormatter.parse(a1.toString).getTime
          val date2 = new java.util.Date(date1 + a2 * 1000L * 60 * 60 * 24)
          import java.util.Calendar
          val calendar = Calendar.getInstance()
          calendar.setTime(date2)
          val y2 = calendar.get(Calendar.YEAR)
          val m2 = calendar.get(Calendar.MONTH)
          val d2 = calendar.get(Calendar.DAY_OF_MONTH)
          val newNumber = y2 * 10000 + m2 * 100 + d2
          GreaterThan(newConst.elemField, Constant(newNumber))
        }
        case _ => newConst
      }
    }
  }

  object ConstraintExtract {
    def unapply[T](node: Def[T]): Option[(Rep[Unit], Constraint)] = node match {
      case Int$less1(elemField, upperBound) if rangeElemField.exists(_._2 == elemField) =>
        // System.out.println(s"< $upperBound")
        val rangeForeach = rangeElemField.find(_._2 == elemField).get._1
        Some((rangeForeach, LessThan(elemField, upperBound)))
      case Int$greater$eq1(elemField, upperBound) if rangeElemField.exists(_._2 == elemField) =>
        // System.out.println(s"< $upperBound")
        val rangeForeach = rangeElemField.find(_._2 == elemField).get._1
        Some((rangeForeach, GreaterThan(elemField, upperBound)))
      case Int$greater1(elemField, lowerBound) if rangeElemFieldConstraints.exists(_._2.exists(c => c.bound == lowerBound)) =>
        // System.out.println(s"> $lowerBound")
        val rangeForeach = rangeElemFieldConstraints.find(_._2.exists(c => c.bound == lowerBound)).get._1
        Some((rangeForeach, GreaterThan(elemField, lowerBound)))
      case _ =>
        None

    }
  }

  case class ArrayInfo[T](rangeForeachSymbol: Rep[Unit], arrayApplyIndex: Rep[Int], array: Rep[Array[T]]) {
    def tpe: TypeRep[T] = array.tp.typeArguments(0).asInstanceOf[TypeRep[T]]
    def constraints: List[Constraint] = arraysInfoConstraints(this)
    def field: String = partitioningField(tpe).get
    def lowerBound: Option[Int] = arraysInfoLowerBound.get(this)
    def upperBound: Option[Int] = arraysInfoUpperBound.get(this)
    def buckets: Rep[Int] = unit(arraysInfoBuckets(this))
    def arraySize: Rep[Int] = array match {
      case Def(ArrayNew(s)) => s
    }
    def count: Rep[Array[Int]] = arraysInfoCount(this)
    def partitionedArray: Rep[Array[Array[T]]] = arraysInfoArray(this).asInstanceOf[Rep[Array[Array[T]]]]
  }

  def getArrayInfo(rangeForeachSymbol: Rep[Unit]): ArrayInfo[Any] = {
    arraysInfo.find(_.rangeForeachSymbol == rangeForeachSymbol).get
  }

  def shouldBePartitioned[T](arrayInfo: ArrayInfo[T]): Boolean = arrayInfo.tpe.name match {
    case "ORDERSRecord" if queryNumber == 3 || queryNumber == 10 => true
    case "LINEITEMRecord" if queryNumber == 6 || queryNumber == 14 => true
    case _ => false
  }

  def partitioningField[T](tpe: TypeRep[T]): Option[String] = tpe.name match {
    case "ORDERSRecord" if queryNumber == 3 || queryNumber == 10 => Some("O_ORDERDATE")
    case "LINEITEMRecord" if queryNumber == 6 || queryNumber == 14 => Some("L_SHIPDATE")
    case _ => None
  }

  def bucketSize[T](arrayInfo: ArrayInfo[T]): Rep[Int] = arrayInfo.tpe.name match {
    case "ORDERSRecord" if queryNumber == 3 || queryNumber == 10 => (arrayInfo.arraySize / arrayInfo.buckets) * unit(4)
    case "LINEITEMRecord" if queryNumber == 6 || queryNumber == 14 => (arrayInfo.arraySize / arrayInfo.buckets) * unit(4)
    case _ => unit(1 << 10)
  }

  case class MyDate(year: Int, month: Int, day: Int) {
    def toInt: Int = year * 10000 + month * 100 + day
  }

  val MAX_DATE = MyDate(1999, 12, 30)
  val MIN_DATE = MyDate(1992, 1, 1)

  def convertDateToIndex(date: Rep[Int]): Rep[Int] = {
    val ym = date / unit(100)
    val month = ym % unit(100)
    val year = ym / unit(100)
    (year - unit(MIN_DATE.year)) * unit(12) + (month - unit(1))
  }

  def convertDateToIndex(date: Int): Int = {
    val ym = date / 100
    val month = ym % 100
    val year = ym / 100
    (year - MIN_DATE.year) * 12 + (month - 1)
  }

  def partitioningFunction[T](arrayInfo: ArrayInfo[T]): (Rep[Int] => Rep[Int]) = arrayInfo.tpe.name match {
    case "ORDERSRecord" if queryNumber == 3 || queryNumber == 10 => (x: Rep[Int]) => {
      convertDateToIndex(x)
    }
    case "LINEITEMRecord" if queryNumber == 6 || queryNumber == 14 => (x: Rep[Int]) => {
      convertDateToIndex(x)
    }
    case _ => ???
  }

  case class PredefinedConstraint(field1: String, field2: String, offset: Rep[Int])
  val predefinedConstraints = List(
    PredefinedConstraint("O_ORDERDATE", "L_SHIPDATE", unit(-122)))

  def computeConstraints(): Unit = {
    def applies1(const: Constraint): Option[PredefinedConstraint] = predefinedConstraints.find(_.field1 == const.field.get)
    def applies2(pred: PredefinedConstraint, const: Constraint): Boolean = pred.field2 == const.field.get
    val newConstraints = rangeElemFieldConstraints.map({
      case (key, set) => {
        val arrayInfo = getArrayInfo(key)
        arrayInfo -> set.distinct.foldLeft(List[Constraint]())((acc, curr) => {
          def convertConstraintCondition(c: Constraint): Boolean =
            applies1(c).exists(pc => applies2(pc, curr))
          if (curr.elemTpe == arrayInfo.tpe)
            acc :+ curr
          else if (acc.exists(convertConstraintCondition)) {
            val const = acc.find(convertConstraintCondition).get

            val newConst = (const, curr) match {
              case (LessThan(e1, b1), GreaterThan(e2, b2)) if b1 == b2 => Some(GreaterThanOffset(e1, b1, applies1(const).get.offset))
              case _ => None
            }
            // System.out.println(s"1: ${curr.elemTpe} 2: ${getArrayInfo(key).tpe} -> $newConst")
            acc ++ newConst
          } else {
            // System.out.println(s"XXX 1: ${curr.elemTpe} 2: ${getArrayInfo(key).tpe} -> const")
            acc
          }
        }).map(_.simplify)
      }
    })
    arraysInfoConstraints ++= newConstraints
    for (arrayInfo <- arraysInfo) {
      if (arrayInfo.constraints.forall(c => c.isForDate)) {
        for (constraint <- arrayInfo.constraints) {
          constraint match {
            case LessThan(_, Constant(upperBound))    => arraysInfoUpperBound += arrayInfo -> upperBound
            case GreaterThan(_, Constant(lowerBound)) => arraysInfoLowerBound += arrayInfo -> lowerBound
            case _                                    =>
          }
        }
        val buckets = convertDateToIndex(MAX_DATE.toInt) - convertDateToIndex(MIN_DATE.toInt) + 1
        arraysInfoBuckets += arrayInfo -> buckets
      }
    }
    // System.out.println(s"old: $rangeElemFieldConstraints")
    System.out.println(s"new: $arraysInfoConstraints, $arraysInfoBuckets")
  }

  override def optimize[T: TypeRep](node: Block[T]): Block[T] = {
    traverseBlock(node)
    arraysInfo ++= possibleRangeFors.filter(rf => rangeForIndex.contains(rf) && rangeArray.contains(rf)).map(rf =>
      ArrayInfo(rf, rangeForIndex(rf), rangeArray(rf))).filter(shouldBePartitioned)
    System.out.println(s"arraysInfo: ${arraysInfo.map(x => x.toString -> x.tpe)}")
    computeConstraints()
    transformProgram(node)
  }

  analysis += statement {
    case sym -> RangeForeach(Def(RangeApplyObject(start, end)), Def(Lambda(_, i, body))) => {
      val unitSym = sym.asInstanceOf[Rep[Unit]]
      possibleRangeFors += unitSym
      rangeForIndex += unitSym -> i.asInstanceOf[Rep[Int]]
      // System.out.println(s"range foreach: $rangeForIndex")
      traverseBlock(body)
      ()
    }
  }

  analysis += statement {
    case sym -> ArrayApply(arr, index) if rangeForIndex.exists(_._2 == index) => {
      val rangeForeach = rangeForIndex.find(_._2 == index).get._1
      rangeArray += rangeForeach -> arr
      rangeArrayApply += rangeForeach -> sym
      // System.out.println(s"arr app: $sym $index $rangeForeach")
      ()
    }
  }

  analysis += statement {
    case sym -> StructImmutableField(elem, field) =>
      if (rangeArrayApply.exists(_._2 == elem)) {
        partitioningField(elem.tp) match {
          case Some(field2) if field == field2 =>
            val rangeForeach = rangeArrayApply.find(_._2 == elem).get._1
            rangeElemField += rangeForeach -> sym
            System.out.println(s"sym field $field")
          case _ => ()
        }
      }
      ()
  }

  analysis += statement {
    case sym -> ConstraintExtract(rangeForeach, constraint) =>
      rangeElemFieldConstraints.getOrElseUpdate(rangeForeach, mutable.ArrayBuffer()) += constraint
      System.out.println(s"$rangeForeach -> $constraint")
  }

  def array_foreach[T: TypeRep](arr: Rep[Array[T]], f: Rep[T] => Rep[Unit]): Rep[Unit] = {
    Range(unit(0), arr.length).foreach {
      __lambda { i =>
        val e = arr(i)
        f(e)
      }
    }
  }

  def createPartitionArray[InnerType](arrayInfo: ArrayInfo[InnerType]): Unit = {
    System.out.println(scala.Console.RED + arrayInfo.tpe + " Partitioned on field " + arrayInfo.field + " with constraints: " + arrayInfo.constraints + scala.Console.RESET)

    implicit val typeInner = arrayInfo.tpe.asInstanceOf[TypeRep[InnerType]]
    val buckets = arrayInfo.buckets
    val partitionedArray = __newArray[Array[InnerType]](buckets)
    val partitionedCount = __newArray[Int](buckets)
    val originalArray = arrayInfo.array
    arraysInfoArray(arrayInfo) = partitionedArray.asInstanceOf[Rep[Array[Any]]]
    arraysInfoCount(arrayInfo) = partitionedCount
    Range(unit(0), buckets).foreach {
      __lambda { i =>
        partitionedArray(i) = __newArray[InnerType](bucketSize(arrayInfo))
      }
    }
    val index = __newVarNamed[Int](unit(0), "partIndex")
    array_foreach(originalArray, {
      (e: Rep[InnerType]) =>
        val pkey = partitioningFunction(arrayInfo)(field[Int](e, arrayInfo.field))
        val currIndex = partitionedCount(pkey)
        val partitionedArrayBucket = partitionedArray(pkey)
        partitionedArrayBucket(currIndex) = e
        partitionedCount(pkey) = currIndex + unit(1)
        __assign(index, readVar(index) + unit(1))
    })
  }

  rewrite += rule {
    case GenericEngineRunQueryObject(b) =>
      for (arrayInfo <- arraysInfo) {
        // printf(unit(s"arrInfo $arrayInfo"))
        createPartitionArray(arrayInfo)
      }
      val newBlock = transformBlock(b)(b.tp)
      GenericEngineRunQueryObject(newBlock)(newBlock.tp)
  }

  var filling = false

  rewrite += statement {
    case sym -> RangeForeach(range, func) if arraysInfo.exists(_.rangeForeachSymbol == sym) => {
      class ElemType
      val arrayInfo = arraysInfo.find(_.rangeForeachSymbol == sym).get.asInstanceOf[ArrayInfo[ElemType]]

      implicit val elemType = arrayInfo.tpe.asInstanceOf[TypeRep[ElemType]]
      System.out.println(s"range foreach rewriting: $range -> $arrayInfo")
      Range(unit(convertDateToIndex(arrayInfo.lowerBound.get)), unit(convertDateToIndex(arrayInfo.upperBound.get) + 1)).foreach {
        __lambda { bucketIndex =>
          val size = arrayInfo.count(bucketIndex)
          val bucketArray = arrayInfo.partitionedArray(bucketIndex)
          Range(unit(0), size).foreach {
            __lambda { index =>
              val elem = bucketArray(index)
              arraysInfoElem(arrayInfo) = elem
              filling = true
              inlineFunction(func, arrayInfo.arrayApplyIndex)
              filling = false
              unit(())
            }
          }
        }

      }
    }
  }

  rewrite += statement {
    case sym -> ArrayApply(arr, index) if filling && arraysInfo.exists(ai => ai.array == arr && ai.arrayApplyIndex == index) => {
      val arrayInfo = arraysInfo.find(ai => ai.array == arr && ai.arrayApplyIndex == index).get
      arraysInfoElem(arrayInfo)
    }
  }
}