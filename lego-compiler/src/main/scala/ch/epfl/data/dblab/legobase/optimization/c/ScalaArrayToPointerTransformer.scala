package ch.epfl.data
package dblab.legobase
package optimization
package c

import deep._
import sc.pardis.optimization._
import sc.pardis.ir._
import sc.pardis.types.PardisTypeImplicits._
import sc.pardis.types._
import scala.language.existentials

// TODO check how ScalaArrayToPointer optimizations can be unified
/**
 * Transforms Scala Arrays into C pointers.
 *
 * More specifically:
 * {{{
 *     val x: Array[Foo]
 * }}}
 * is converted into:
 * {{{
 *     Foo* x;
 * }}}
 *
 * We assume that the length field of an array is partially evaluated before.
 *
 * This one is Row-Store representation.
 *
 * @param IR the polymorphic embedding trait which contains the reified program.
 * @param settings the compiler settings provided as command line arguments (TODO should be removed)
 */
class ScalaArrayToPointerTransformer(override val IR: LoweringLegoBase, val settings: compiler.Settings) extends RuleBasedTransformer[LoweringLegoBase](IR) with CTransformer {
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
      case ArrayType(args) => typePointer(args)
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
        val elemType = if (a.tp.typeArguments(0).isRecord) a.tp.typeArguments(0) else transformType(a.tp.typeArguments(0))
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
      val innerElemType = (if (elemType.isArray) elemType.typeArguments(0) else elemType).asInstanceOf[TypeRep[Any]]
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
      } else if (elemType.isRecord) {
        class T
        implicit val typeT = apply(v).tp.typeArguments(0).asInstanceOf[TypeRep[T]]
        val newV = apply(v).asInstanceOf[Expression[Pointer[T]]]
        def updateRecord() = {
          val vContent = *(newV)
          val tArr = arr.asInstanceOf[Expression[Pointer[T]]]
          pointer_assign_content(tArr, i, vContent)
          val pointerVar = newV match {
            case Def(ReadVar(v)) => v.asInstanceOf[Var[Pointer[T]]]
            case x               => Var(x.asInstanceOf[Rep[Var[Pointer[T]]]])
          }
          __assign(pointerVar, (&(tArr.asInstanceOf[Rep[T]], i)))(PointerType(typeT))
        }
        if (settings.containerFlattenning) {
          __ifThenElse(newV __== unit(null), {
            class T1
            implicit val typeT1 = innerElemType.asInstanceOf[TypeRep[T1]]
            val tArr = arr.asInstanceOf[Expression[Pointer[T1]]]
            pointer_assign_content(tArr, i, unit(null))
          }, {
            updateRecord()
          })
        } else {
          updateRecord()
        }
      } else if (elemType.isInstanceOf[SetType[_]] || elemType.isArray) {
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
      val elemType = try {
        a.tp.typeArguments(0)
      } catch {
        case ex => throw new Exception(s"A problem with array $a:${a.tp}", ex)
      }
      // Get type of internal array
      val newTp = ({
        if (elemType.isArray) typeArray(typePointer(elemType.typeArguments(0)))
        else typeArray(elemType)
      }).asInstanceOf[PardisType[Any]]
      val arr = s
      if (elemType.isPrimitive || elemType == OptimalStringType || elemType.isInstanceOf[SetType[_]] || elemType.isArray) ArrayApply(arr.asInstanceOf[Expression[Array[Any]]], apply(i))(newTp)
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
