package ch.epfl.data
package dblab
package queryengine

import schema._
import legobase.deep.LegoBaseQueryEngineExp
import frontend.parser.OperatorAST._
import frontend.parser._
import ch.epfl.data.sc.pardis.types.{PardisType, RecordType}
import ch.epfl.data.sc.pardis.shallow.Record
import scala.reflect.runtime.universe._
import ch.epfl.data.sc.pardis.types.PardisTypeImplicits._

object ExpressionCompiler {
  implicit class ExpressionOps(e: Expression) {
    def pardisType: PardisType[_] = {
      val IntType = typeTag[scala.Int]
      val FloatType = typeTag[scala.Float]
      val DoubleType = typeTag[scala.Double]
      val CharType = typeTag[scala.Char]
      val VarCharType = typeTag[VarCharType]
      val OptimalString = typeTag[sc.pardis.shallow.OptimalString]

      e.tp match {
        // TODO: is there a better way?
        case IntType       => implicitly[PardisType[Int]]
        case FloatType     => implicitly[PardisType[Float]]
        case DoubleType    => implicitly[PardisType[Double]]
        case CharType      => implicitly[PardisType[Char]]
        case VarCharType   => sc.pardis.deep.OptimalStringIRs.OptimalStringType
        case OptimalString => sc.pardis.deep.OptimalStringIRs.OptimalStringType
        case _ =>
          throw new Exception(s"Type ${e.tp} of expression $e can't be transformed to a PardisType")
      }
    }
  }
}

trait ExpressionCompiler {
  val context: LegoBaseQueryEngineExp
  type Rep[T] = context.Rep[T]
  import context._

  def getField[T: PardisType](qualifier: Option[String], fieldName: String): Option[Rep[T]]

  implicit class ExtraExpressionOps(e: Expression) extends ExpressionCompiler.ExpressionOps(e) {
    def asRep[T: PardisType]: Rep[T] = parseExpression[T](e)
    def toRep: Rep[_] = parseExpression(e)(e.pardisType)
  }

  /**
   * This method receives two numbers and makes sure that both number have the same type.
   * If their type is different, it will upcast the number with lower type to the type
   * of the other number.
   */
  def promoteNumbers(e1: Expression, e2: Expression): (Rep[Any], Rep[Any]) = {
    val n1 = e1.toRep
    val n2 = e2.toRep
    ((e1.pardisType, e2.pardisType) match {
      case (x, y) if x == y      => n1 -> n2
      case (IntType, DoubleType) => infix_asInstanceOf[Int](n1).toDouble -> n2
      //case (IntType, FloatType)    => n1.asInstanceOf[Int].toFloat -> n2
      //case (FloatType, DoubleType) => n1.asInstanceOf[Float].toDouble -> n2
      //case (DoubleType, FloatType) => n1 -> n2.asInstanceOf[Float].toDouble
      case (DoubleType, IntType) => n1 -> infix_asInstanceOf[Int](n2).toDouble
      case (_, null)             => n1 -> n2
      case (null, _)             => n1 -> n2
      //case _ =>
      //n1.asInstanceOf[Double].toDouble -> n2.asInstanceOf[Double].toDouble // FIXME FIXME FIXME THIS SHOULD BE HAPPENING, TYPE INFERENCE BUG
      case (x, y)                => throw new Exception(s"Does not know how to find the common type for $x and $y")
    }).asInstanceOf[(Rep[Any], Rep[Any])]
  }

  def addition[A: TypeRep](x: Rep[_], y: Rep[_]): Rep[A] = (implicitly[TypeRep[A]] match {
    case IntType    => x.asInstanceOf[Rep[Int]] + y.asInstanceOf[Rep[Int]]
    case DoubleType => x.asInstanceOf[Rep[Double]] + y.asInstanceOf[Rep[Double]]
  }).asInstanceOf[Rep[A]]

  def product[A: TypeRep](x: Rep[_], y: Rep[_]): Rep[A] = (implicitly[TypeRep[A]] match {
    case IntType    => x.asInstanceOf[Rep[Int]] * y.asInstanceOf[Rep[Int]]
    case DoubleType => x.asInstanceOf[Rep[Double]] * y.asInstanceOf[Rep[Double]]
  }).asInstanceOf[Rep[A]]

  def subtraction[A: TypeRep](x: Rep[_], y: Rep[_]): Rep[A] = (implicitly[TypeRep[A]] match {
    case IntType    => x.asInstanceOf[Rep[Int]] - y.asInstanceOf[Rep[Int]]
    case DoubleType => x.asInstanceOf[Rep[Double]] - y.asInstanceOf[Rep[Double]]
    case CharType   => x.asInstanceOf[Rep[Char]] - y.asInstanceOf[Rep[Char]]
  }).asInstanceOf[Rep[A]]

  def lessThan[A: TypeRep](x: Rep[_], y: Rep[_]): Rep[Boolean] = implicitly[TypeRep[A]] match {
    case IntType    => x.asInstanceOf[Rep[Int]] < y.asInstanceOf[Rep[Int]]
    case DoubleType => x.asInstanceOf[Rep[Double]] < y.asInstanceOf[Rep[Double]]
  }

  def lessOrEqual[A: TypeRep](x: Rep[_], y: Rep[_]): Rep[Boolean] = implicitly[TypeRep[A]] match {
    case IntType    => x.asInstanceOf[Rep[Int]] <= y.asInstanceOf[Rep[Int]]
    case DoubleType => x.asInstanceOf[Rep[Double]] <= y.asInstanceOf[Rep[Double]]
  }

  def greaterThan[A: TypeRep](x: Rep[_], y: Rep[_]): Rep[Boolean] = implicitly[TypeRep[A]] match {
    case IntType    => x.asInstanceOf[Rep[Int]] > y.asInstanceOf[Rep[Int]]
    case DoubleType => x.asInstanceOf[Rep[Double]] > y.asInstanceOf[Rep[Double]]
    case DateType   => x.asInstanceOf[Rep[Int]] > y.asInstanceOf[Rep[Int]]
  }

  def greaterOrEqual[A: TypeRep](x: Rep[_], y: Rep[_]): Rep[Boolean] = implicitly[TypeRep[A]] match {
    case IntType    => x.asInstanceOf[Rep[Int]] >= y.asInstanceOf[Rep[Int]]
    case DoubleType => x.asInstanceOf[Rep[Double]] >= y.asInstanceOf[Rep[Double]]
  }

  def equals[A: TypeRep](x: Rep[A], y: Rep[A]): Rep[Boolean] = implicitly[TypeRep[A]] match {
    // case IntType =>
    //   // TODO: this shouldn't be hardcoded, maybe it can even be inferred from the schema
    //   x.asInstanceOf[Rep[Int]] - y.asInstanceOf[Rep[Int]] <= unit(1e-6) &&
    //     x.asInstanceOf[Rep[Int]] - y.asInstanceOf[Rep[Int]] >= unit(-1e-6)
    // case DoubleType =>
    //   x.asInstanceOf[Rep[Double]] - y.asInstanceOf[Rep[Double]] <= unit(1e-6) &&
    //     x.asInstanceOf[Rep[Double]] - y.asInstanceOf[Rep[Double]] >= unit(-1e-6)
    case other =>
      AllRepOps(x)(other).__==(y)
  }

  // // TODO: All the expression manipulation stuff could go into another class
  val subqueryInitializedMap = new scala.collection.mutable.HashMap[OperatorNode, Rep[SubquerySingleResult[Record]]]()
  def parseExpression[A: TypeRep](e: Expression): Rep[A] = {
    val result: Rep[_] = e match {
      // Literals
      case FieldIdent(qualifier, name, _) =>
        getField[A](qualifier, name).getOrElse {
          throw new Exception(s"Couldn't find field ${qualifier.getOrElse("") + name}")
        }
      case DateLiteral(v)   => GenericEngine.parseDate(unit(v))
      case FloatLiteral(v)  => unit(v)
      case DoubleLiteral(v) => unit(v)
      case IntLiteral(v)    => unit(v)
      case StringLiteral(v) => GenericEngine.parseString(unit(v))
      case NullLiteral      => unit(null)
      case CharLiteral(v)   => unit(v)
      // Arithmetic Operators
      case Add(left, right) =>
        val (e1, e2) = promoteNumbers(left, right)
        addition(e1, e2)(e1.tp)
      case Subtract(left, right) =>
        val (e1, e2) = promoteNumbers(left, right)
        subtraction(e1, e2)(e1.tp)
      case Multiply(left, right) =>
        val (e1, e2) = promoteNumbers(left, right)
        product(e1, e2)(e1.tp)
      // case Divide(left, right) =>
      //   // TODO: Check if this gives the correct result -- also fix asInstanceOf
      //   computeNumericExpression(left, right, { (x, y) =>
      //     if (y.isInstanceOf[Double])
      //       x.toInt / y.asInstanceOf[Double]
      //     else
      //       x.toInt / y.asInstanceOf[Int]
      //   }, t, t2)(left.tp, right.tp)
      // case UnaryMinus(expr) => computeNumericExpression(IntLiteral(-1), expr, (x, y) => x * y, t, t2)(expr.tp, expr.tp)
      // Logical Operators
      case Equals(left, right) =>
        val lhs = left.toRep
        val rhs = right.toRep
        equals(lhs, rhs)(lhs.tp)
      case NotEquals(left, right) =>
        val lhs = left.toRep
        val rhs = right.toRep
        !equals(lhs, rhs)(lhs.tp)
      case And(left, right) =>
        left.asRep[Boolean] && right.asRep[Boolean]
      case Or(left, right) =>
        left.asRep[Boolean] || right.asRep[Boolean]
      case GreaterOrEqual(left, right) =>
        val (e1, e2) = promoteNumbers(left, right)
        greaterOrEqual(e1, e2)(e1.tp)
      case GreaterThan(left, right) =>
        val (e1, e2) = promoteNumbers(left, right)
        greaterThan(e1, e2)(e1.tp)
      case LessOrEqual(left, right) =>
        val (e1, e2) = promoteNumbers(left, right)
        lessOrEqual(e1, e2)(e1.tp)
      case LessThan(left, right) =>
        val (e1, e2) = promoteNumbers(left, right)
        lessThan(e1, e2)(e1.tp)
      case Not(expr) =>
        !expr.asRep[Boolean]
      // // SQL statements
      case Year(date) =>
        val d = date.toRep
        infix_asInstanceOf[Int](d) / unit(10000);
      case Substring(field, idx1, idx2) =>
        field.asRep[OptimalString].slice(idx1.asRep[Int], idx2.asRep[Int])
      case Like(field, value) =>
        val f = field.asRep[OptimalString]
        //val s = value.asRep[OptimalString]
        val str = value match {
          case StringLiteral(s: String) =>
            s
          case _ =>
            throw new Exception(s"Couldn't intrepret LIKE pattern: %str")
        }
        val delim = "%%"
        val v = GenericEngine.parseString(unit(str.replaceAll("%", "")))
        if (str.startsWith(delim) && str.endsWith(delim)) {
          f.containsSlice(v)
        } else if (str.endsWith(delim)) {
          f.startsWith(v)
        } else if (str.startsWith(delim)) {
          f.endsWith(v)
        } else {
          throw new Exception(s"Couldn't intrepret LIKE pattern: %str")
        }
      // case In(expr, values) => {
      //   val c = parseExpression(expr, t, t2)
      //   val v = values.map(parseExpression(_, t, t2))
      //   if (v.contains(c)) true
      //   else false
      // }
      // case StringConcat(str1, str2) =>
      //   // Todo move this to SC
      //   val left = parseExpression(str1, t, t2)
      //   val right = parseExpression[OptimalString](str2, t, t2)
      //   left + new String((right.data: scala.collection.mutable.ArrayOps[Byte]).map(_.toChar))
      case Case(cond, thenp, elsep) =>
        __ifThenElse(cond.asRep[Boolean], thenp.toRep, elsep.toRep)
      // TODO -- Not good. Must be generalized. The extraction of the single field should go earlier in the pipeline.
      case GetSingleResult(parent: SubquerySingleResultNode) =>
        val p = subqueryInitializedMap.getOrElseUpdate(parent, PlanCompiler.convertOperator(parent).asInstanceOf[Rep[SubquerySingleResult[Record]]])
        val rec = p.getResult
        p.reset

        // TODO: hack
        import PlanCompiler.OperatorNodeOps
        if (parent.resultType.fieldNames.last == "__TOTAL_COUNT")
          parent.resultType.getField(rec, None, parent.resultType.fieldNames.head)(parent.resultType.fieldTypes.head.tp).get
        else if (parent.resultType.fieldNames.length > 1)
          throw new Exception(s"LegoBase BUG: Do not know how to extract single value from tuple with > 1 fields (${parent.resultType.fieldNames}).")
        // TODO: Generalize, this is a hack
        else
          parent.resultType.getField(rec, None, parent.resultType.fieldNames.last)(parent.resultType.fieldTypes.last.tp).get
    }
    result.asInstanceOf[Rep[A]]
  }

}
