package ch.epfl.data
package dblab
package transformers
package c

import deep._
import sc.pardis.optimization._
import sc.pardis.ir._
import deep.dsls.QueryEngineExp
import sc.pardis.types.PardisTypeImplicits._
import sc.pardis.types._
import scala.language.existentials

/**
 * Transforms Scala Arrays into C structs with a pointer of pointer as its field.
 *
 * More specifically:
 * {{{
 *     val x: Array[Foo]
 * }}}
 * is converted into:
 * {{{
 *     struct ArrayFoo {
 *       Foo** array;
 *       int length;
 *     };
 *     ArrayFoo* x;
 * }}}
 *
 * This one has performance overhead in comparison with the one which was using a
 * pointer as its field.
 *
 * This one is Pointer-Store representation.
 *
 * @param IR the polymorphic embedding trait which contains the reified program.
 */
class ScalaArrayToPointerBadRecordTransformer(override val IR: QueryEngineExp) extends RuleBasedTransformer[QueryEngineExp](IR) with CTransformer {
  import IR._
  import CNodes._
  import CTypes._

  // TODO needs to be rewritten

  analysis += statement {
    case sym -> ArrayLength(arr) =>
      System.out.println(s"val $sym = $arr.length")
  }

  override def transformType[T: PardisType]: PardisType[Any] = ({
    val tp = typeRep[T]
    tp match {
      case ArrayType(args) => typePointer(transformType(args))
      case c if c.isRecord => tp.typeArguments match {
        case Nil     => typePointer(tp)
        case List(t) => typePointer(transformType(t))
      }
      case SetType(args) => typePointer(typeLPointer(typeGList))
      case _             => super.transformType[T]
    }
  }).asInstanceOf[PardisType[Any]]

  rewrite += rule {
    case pc @ PardisCast(x) => PardisCast(apply(x))(apply(pc.castFrom), apply(pc.castTp))
  }

  rewrite += rule {
    case a @ ArrayNew(x) =>
      if (a.tp.typeArguments(0).isArray) {
        // Get type of elements stored in array
        val elemType = typePointer(a.tp.typeArguments(0).typeArguments(0))
        // Allocate original array
        val array = malloc(x)(elemType)
        array.asInstanceOf[Expression[Any]]
      } else {
        // Get type of elements stored in array
        val elemType = transformType(a.tp.typeArguments(0))
        // Allocate original array
        val array = malloc(x)(elemType)
        array.asInstanceOf[Expression[Any]]
      }
  }
  rewrite += rule {
    case au @ ArrayUpdate(a, i, v) =>
      val s = apply(a)
      // Get type of elements stored in array
      val elemType = a.tp.typeArguments(0)
      // Get type of internal array
      val newTp = ({
        if (elemType.isArray) typeArray(typePointer(elemType.typeArguments(0)))
        else typeArray(elemType)
      }).asInstanceOf[PardisType[Any]]
      val innerElemType = (if (elemType.isArray) elemType.typeArguments(0) else if (elemType.isRecord) typePointer(elemType) else elemType).asInstanceOf[TypeRep[Any]]
      val arr = s
      if (elemType.isPrimitive || elemType == OptimalStringType)
        pointer_assign_content(arr.asInstanceOf[Expression[Pointer[Any]]], i, apply(v))
      else if (v match {
        case Def(Cast(Constant(null))) => true
        case Constant(null)            => true
        case _                         => false
      }) {
        class T
        implicit val typeT = innerElemType.asInstanceOf[TypeRep[T]]
        val tArr = arr.asInstanceOf[Expression[Pointer[T]]]
        pointer_assign_content(tArr, i, unit(null))
      } else if (elemType.isInstanceOf[SetType[_]] || elemType.isArray || elemType.isRecord) {
        pointer_assign_content(arr.asInstanceOf[Expression[Pointer[Any]]], i, apply(v).asInstanceOf[Expression[Pointer[Any]]])
      } else {
        pointer_assign_content(arr.asInstanceOf[Expression[Pointer[Any]]], i, *(apply(v).asInstanceOf[Expression[Pointer[Any]]])(v.tp.name match {
          case x if v.tp.isArray            => transformType(v.tp).typeArguments(0).asInstanceOf[PardisType[Any]]
          case x if x.startsWith("Pointer") => v.tp.typeArguments(0).asInstanceOf[PardisType[Any]]
          case _                            => v.tp
        }))
      }
  }
  rewrite += rule {
    case ArrayFilter(a, op) =>
      apply(a)
  }
  rewrite += rule {
    case ArrayApply(a, i) =>
      val s = apply(a)
      // Get type of elements stored in array
      val elemType = a.tp.typeArguments(0)
      // Get type of internal array
      val newTp = ({
        if (elemType.isArray) typeArray(typePointer(elemType.typeArguments(0)))
        else typeArray(elemType)
      }).asInstanceOf[PardisType[Any]]
      val arr = s
      if (elemType.isPrimitive || elemType == OptimalStringType || elemType.isInstanceOf[SetType[_]] || elemType.isArray || elemType.isRecord) ArrayApply(arr.asInstanceOf[Expression[Array[Any]]], apply(i))(newTp)
      else {
        i match {
          case Constant(0) => Cast(arr)(arr.tp, typePointer(newTp))
          case _           => PTRADDRESS(arr.asInstanceOf[Expression[Pointer[Any]]], apply(i))(typePointer(newTp).asInstanceOf[PardisType[Pointer[Any]]])
        }
      }
  }

  def __arrayLength[T: TypeRep](arr: Rep[Array[T]]): Rep[Int] = {
    throw new Exception(s"${scala.Console.RED}array length is not supported for $arr!${scala.Console.RESET}")
  }

  rewrite += rule {
    case ArrayLength(a) =>
      __arrayLength(a)
  }
  rewrite += rule {
    case ArrayForeach(a, f) =>
      val length = __arrayLength(a)
      // TODO if we use recursive rule based, the next line will be cleaner
      Range(unit(0), length).foreach {
        __lambda { index =>
          System.out.println(s"index: $index, f: ${f.correspondingNode}")
          val elemNode = apply(ArrayApply(a, index))
          val elem = toAtom(elemNode)(elemNode.tp.typeArguments(0).typeArguments(0).asInstanceOf[TypeRep[Any]])
          inlineFunction(f, elem)
        }
      }
  }

}
