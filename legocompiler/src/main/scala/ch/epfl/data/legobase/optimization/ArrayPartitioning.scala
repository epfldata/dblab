package ch.epfl.data
package legobase
package optimization

import scala.language.implicitConversions
import pardis.ir._
import reflect.runtime.universe.{ TypeTag, Type }
import pardis.optimization._
import deep._
import pardis.types._
import pardis.types.PardisTypeImplicits._
import pardis.shallow.utils.DefaultValue

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
  }
  case class LessThan(elemField: Rep[Any], upperBound: Rep[Int]) extends Constraint {
    def bound = upperBound.asInstanceOf[Rep[Any]]
  }
  case class GreaterThan(elemField: Rep[Any], lowerBound: Rep[Int]) extends Constraint {
    def bound = lowerBound.asInstanceOf[Rep[Any]]
  }
  case class GreaterThanOffset(elemField: Rep[Any], lowerBound: Rep[Int], offset: Rep[Int]) extends Constraint {
    def bound = lowerBound.asInstanceOf[Rep[Any]]
  }

  object ConstraintExtract {
    def unapply[T](node: Def[T]): Option[(Rep[Unit], Constraint)] = node match {
      case Int$less3(elemField, upperBound) if rangeElemField.exists(_._2 == elemField) =>
        // System.out.println(s"< $upperBound")
        val rangeForeach = rangeElemField.find(_._2 == elemField).get._1
        Some((rangeForeach, LessThan(elemField, upperBound)))
      case Int$greater3(elemField, lowerBound) if rangeElemFieldConstraints.exists(_._2.exists(c => c.bound == lowerBound)) =>
        // System.out.println(s"> $lowerBound")
        val rangeForeach = rangeElemFieldConstraints.find(_._2.exists(c => c.bound == lowerBound)).get._1
        Some((rangeForeach, GreaterThan(elemField, lowerBound)))
      case _ =>
        None

    }
  }

  case class ArrayInfo[T](rangeForeachSymbol: Rep[Unit], arrayApplyIndex: Rep[Int], array: Rep[Array[T]]) {
    def tpe: TypeRep[T] = array.tp.typeArguments(0).asInstanceOf[TypeRep[T]]
    def constraints: List[Constraint] = arraysInfoConstraints(this.asInstanceOf[ArrayInfo[Any]])
  }

  def getArrayInfo(rangeForeachSymbol: Rep[Unit]): ArrayInfo[Any] = {
    arraysInfo.find(_.rangeForeachSymbol == rangeForeachSymbol).get
  }

  def shouldBePartitioned[T](arrayInfo: ArrayInfo[T]): Boolean = arrayInfo.tpe.name match {
    case "ORDERSRecord" if queryNumber == 3 => true
    case _                                  => false
  }

  def partitioningField[T](tpe: TypeRep[T]): Option[String] = tpe.name match {
    case "ORDERSRecord" if queryNumber == 3 => Some("O_ORDERDATE")
    case _                                  => None
  }

  def convertDateToIndex(date: Rep[Int]): Rep[Int] = {
    val ym = date / unit(100)
    val month = ym % unit(100)
    val year = ym / unit(100)
    (year - unit(1992)) * unit(12) + (month - unit(1))
  }

  def partitioningFunction[T](arrayInfo: ArrayInfo[T]): (Rep[Int] => Rep[Int]) = arrayInfo.tpe.name match {
    case "ORDERSRecord" if queryNumber == 3 => (x: Rep[Int]) => {
      convertDateToIndex(x)
    }
    case _ => ???
  }

  case class PredefinedConstraint(field1: String, field2: String, offset: Rep[Int])

  def computeConstraints(): Unit = {
    val predefinedConstraints = List(
      PredefinedConstraint("O_ORDERDATE", "L_SHIPDATE", unit(-122)))
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
        })
      }
    })
    arraysInfoConstraints ++= newConstraints
    // System.out.println(s"old: $rangeElemFieldConstraints")
    // System.out.println(s"new: $newConstraints")
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

  // rewrite += statement {
  //   case sym -> RangeForeach(range, func) if arraysInfo.exists(_.rangeForeachSymbol == sym) => {
  //     System.out.println(s"range foreach rewriting: $range")
  //     sym
  //   }
  // }
}