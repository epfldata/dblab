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

/**
 * Transforms Scala Arrays into C structs.
 *
 * More specifically:
 * {{{
 *     val x: Array[Foo]
 * }}}
 * is converted into:
 * {{{
 *     struct ArrayFoo {
 *       Foo* array;
 *       int length;
 *     };
 *     ArrayFoo* x;
 * }}}
 * This one is Row-Store representation.
 *
 *
 * @param IR the polymorphic embedding trait which contains the reified program.
 */
class ScalaArrayToCStructTransformer(override val IR: LegoBaseExp) extends RuleBasedTransformer[LegoBaseExp](IR) with CTransformer {
  import IR._
  import CNodes._
  import CTypes._

  // TODO needs to be rewritten

  override def transformType[T: PardisType]: PardisType[Any] = ({
    val tp = typeRep[T]
    tp match {
      case c if c.isPrimitive    => super.transformType[T]
      case ArrayBufferType(args) => typePointer(typeGArray)
      // case ArrayType(x) if x == ByteType => typePointer(ByteType)
      case ArrayType(args) => typePointer(typeCArray({
        if (args.isArray) typeCArray(args)
        else args
      }))
      case c if c.isRecord => tp.typeArguments match {
        case Nil     => typePointer(tp)
        case List(t) => typePointer(transformType(t))
      }
      case TreeSetType(args) => typePointer(typeGTree)
      case SetType(args)     => typePointer(typeLPointer(typeGList))
      case OptionType(args)  => typePointer(transformType(args))
      case _                 => super.transformType[T]
    }
  }).asInstanceOf[PardisType[Any]]

  rewrite += rule {
    case pc @ PardisCast(x) => PardisCast(apply(x))(apply(pc.castFrom), apply(pc.castTp))
  }

  rewrite += rule {
    case a @ ArrayNew(x) =>
      val size = apply(x)
      if (a.tp.typeArguments(0).isArray) {
        // Get type of elements stored in array
        val elemType = typeCArray(a.tp.typeArguments(0).typeArguments(0))
        // Allocate original array
        val array = malloc(size)(elemType)
        // Create wrapper with length
        val am = typeCArray(typeCArray(a.tp.typeArguments(0).typeArguments(0))).asInstanceOf[PardisType[CArray[CArray[Any]]]] //transformType(a.tp)
        val tagName = structName(am)
        val s = toAtom(
          PardisStruct(StructTags.ClassTag(tagName),
            List(PardisStructArg("array", false, array), PardisStructArg("length", false, x)),
            List())(am))(am)
        val m = malloc(unit(1))(am)
        structCopy(m.asInstanceOf[Expression[Pointer[Any]]], s)
        m.asInstanceOf[Expression[Any]]
      } else {
        // Get type of elements stored in array
        val elemType = if (a.tp.typeArguments(0).isRecord) a.tp.typeArguments(0) else transformType(a.tp.typeArguments(0))
        // Allocate original array
        val array = malloc(size)(elemType)
        // Create wrapper with length
        val am = transformType(a.tp)
        val s = toAtom(
          PardisStruct(StructTags.ClassTag(structName(am)),
            List(PardisStructArg("array", false, array), PardisStructArg("length", false, x)),
            List())(am))(am)
        val m = malloc(unit(1))(am.typeArguments(0))
        structCopy(m.asInstanceOf[Expression[Pointer[Any]]], s)
        m.asInstanceOf[Expression[Any]]
      }
  }
  rewrite += rule {
    case au @ ArrayUpdate(a, i, v) =>
      val s = apply(a)
      // Get type of elements stored in array
      val elemType = a.tp.typeArguments(0)
      // Get type of internal array
      val newTp = ({
        if (elemType.isArray) typePointer(typeCArray(elemType.typeArguments(0)))
        else typeArray(typePointer(elemType))
      }).asInstanceOf[PardisType[Any]]
      // Read array and perform update
      val arr = field(s, "array")(newTp)
      if (elemType.isPrimitive) arrayUpdate(arr.asInstanceOf[Expression[Array[Any]]], i, apply(v))
      else if (elemType.name == "OptimalString")
        pointer_assign_content(arr.asInstanceOf[Expression[Pointer[Any]]], i, apply(v))
      else if (v match {
        case Def(Cast(Constant(null))) => true
        case Constant(null)            => true
        case _                         => false
      }) {
        class T
        implicit val typeT = newTp.typeArguments(0).typeArguments(0).asInstanceOf[TypeRep[T]]
        val tArr = arr.asInstanceOf[Expression[Pointer[T]]]
        pointer_assign_content(tArr, i, unit(null))
      } else if (elemType.isRecord) {
        class T
        implicit val typeT = apply(v).tp.typeArguments(0).asInstanceOf[TypeRep[T]]
        val newV = apply(v).asInstanceOf[Expression[Pointer[T]]]
        __ifThenElse(apply(v) __== unit(null), {
          class T1
          implicit val typeT1 = newTp.typeArguments(0).typeArguments(0).asInstanceOf[TypeRep[T1]]
          val tArr = arr.asInstanceOf[Expression[Pointer[T1]]]
          pointer_assign_content(tArr, i, unit(null))
        }, {
          val vContent = *(newV)
          val tArr = arr.asInstanceOf[Expression[Pointer[T]]]
          pointer_assign_content(tArr, i, vContent)
          val pointerVar = newV match {
            case Def(ReadVar(v)) => v.asInstanceOf[Var[Pointer[T]]]
            case x               => Var(x.asInstanceOf[Rep[Var[Pointer[T]]]])
          }
          __assign(pointerVar, (&(tArr, i)).asInstanceOf[Rep[Pointer[T]]])(PointerType(typeT))
        })
      } else if (elemType.isInstanceOf[SetType[_]]) {
        pointer_assign_content(arr.asInstanceOf[Expression[Pointer[Any]]], i, apply(v).asInstanceOf[Expression[Pointer[Any]]])
      } else
        pointer_assign_content(arr.asInstanceOf[Expression[Pointer[Any]]], i, *(apply(v).asInstanceOf[Expression[Pointer[Any]]])(v.tp.name match {
          case x if v.tp.isArray            => transformType(v.tp).typeArguments(0).asInstanceOf[PardisType[Any]]
          case x if x.startsWith("Pointer") => v.tp.typeArguments(0).asInstanceOf[PardisType[Any]]
          case _                            => v.tp
        }))
  }
  rewrite += rule {
    case ArrayFilter(a, op) => field(apply(a), "array")(transformType(a.tp))
  }
  rewrite += rule {
    case ArrayApply(a, i) =>
      val s = apply(a)
      // Get type of elements stored in array
      val elemType = a.tp.typeArguments(0)
      // Get type of internal array
      val newTp = ({
        if (elemType.isArray) typePointer(typeCArray(elemType.typeArguments(0)))
        else typeArray(typePointer(elemType))
      }).asInstanceOf[PardisType[Any]]
      val arr = field(s, "array")(newTp)
      if (elemType.isPrimitive || elemType == OptimalStringType || elemType.isInstanceOf[SetType[_]]) ArrayApply(arr.asInstanceOf[Expression[Array[Any]]], apply(i))(newTp.asInstanceOf[PardisType[Any]])
      else {
        i match {
          case Constant(0) => Cast(arr)(arr.tp, typePointer(newTp))
          case _           => PTRADDRESS(arr.asInstanceOf[Expression[Pointer[Any]]], apply(i))(typePointer(newTp).asInstanceOf[PardisType[Pointer[Any]]])
        }
      }
  }

  def __arrayLength[T: TypeRep](arr: Rep[Array[T]]): Rep[Int] = {
    val s = apply(arr)
    field[Int](s, "length")
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
  rewrite += rule {
    case s @ PardisStruct(tag, elems, methods) =>
      // TODO if needed method generation should be added
      val x = toAtom(Malloc(unit(1))(s.tp))(typePointer(s.tp))
      val newElems = elems.map(el => PardisStructArg(el.name, el.mutable, transformExp(el.init)(el.init.tp, apply(el.init.tp))))
      structCopy(x, PardisStruct(tag, newElems, methods.map(m => m.copy(body =
        transformDef(m.body.asInstanceOf[Def[Any]]).asInstanceOf[PardisLambdaDef])))(s.tp))
      x
  }
}
