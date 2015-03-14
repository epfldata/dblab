package ch.epfl.data
package legobase
package optimization
package c

import deep._
import pardis.optimization._
import pardis.ir._
import pardis.types.PardisTypeImplicits._
import pardis.types._
import scala.language.existentials

class ScalaArrayToPointerTransformer(override val IR: LoweringLegoBase, val settings: compiler.Settings) extends RuleBasedTransformer[LoweringLegoBase](IR) with CTransformer {
  import IR._
  import CNodes._
  import CTypes._

  analysis += statement {
    case sym -> ArrayLength(arr) =>
      System.out.println(s"val $sym = $arr.length")
  }

  override def transformType[T: PardisType]: PardisType[Any] = ({
    val tp = typeRep[T]
    tp match {
      // case c if c.isPrimitive    => super.transformType[T]
      // case ArrayBufferType(args) => typePointer(typeGArray)
      // case ArrayType(x) if x == ByteType => typePointer(ByteType)
      // case ArrayType(args) => typePointer(typeCArray({
      //   if (args.isArray) typeCArray(args)
      //   else args
      // }))
      case ArrayType(args) => typePointer(args)
      case c if c.isRecord => tp.typeArguments match {
        case Nil     => typePointer(tp)
        case List(t) => typePointer(transformType(t))
      }
      // case TreeSetType(args) => typePointer(typeGTree)
      case SetType(args) => typePointer(typeLPointer(typeGList))
      // case OptionType(args)  => typePointer(transformType(args))
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
        // val elemType = typeCArray(a.tp.typeArguments(0).typeArguments(0))
        // // Allocate original array
        // val array = malloc(x)(elemType)
        // Create wrapper with length
        // val am = typeCArray(typeCArray(a.tp.typeArguments(0).typeArguments(0))).asInstanceOf[PardisType[CArray[CArray[Any]]]] //transformType(a.tp)
        // val tagName = structName(am)
        // val s = toAtom(
        //   PardisStruct(StructTags.ClassTag(tagName),
        //     List(PardisStructArg("array", false, array), PardisStructArg("length", false, x)),
        //     List())(am))(am)
        // val m = malloc(unit(1))(am)
        // structCopy(m.asInstanceOf[Expression[Pointer[Any]]], s)
        // m.asInstanceOf[Expression[Any]]
        val elemType = typePointer(a.tp.typeArguments(0).typeArguments(0))
        // Allocate original array
        val array = malloc(x)(elemType)
        array.asInstanceOf[Expression[Any]]
      } else {
        // Get type of elements stored in array
        // val elemType = if (a.tp.typeArguments(0).isRecord) a.tp.typeArguments(0) else transformType(a.tp.typeArguments(0))
        // // Allocate original array
        // val array = malloc(x)(elemType)
        // // Create wrapper with length
        // val am = transformType(a.tp)
        // val s = toAtom(
        //   PardisStruct(StructTags.ClassTag(structName(am)),
        //     List(PardisStructArg("array", false, array), PardisStructArg("length", false, x)),
        //     List())(am))(am)
        // val m = malloc(unit(1))(am.typeArguments(0))
        // structCopy(m.asInstanceOf[Expression[Pointer[Any]]], s)
        // m.asInstanceOf[Expression[Any]]
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
      // val newTp = ({
      //   if (elemType.isArray) typePointer(typeCArray(elemType.typeArguments(0)))
      //   else typeArray(typePointer(elemType))
      // }).asInstanceOf[PardisType[Any]] //if (elemType.isPrimitive) elemType else typePointer(elemType)
      // // Read array and perform update
      // val arr = field(s, "array")(newTp)
      val newTp = ({
        if (elemType.isArray) typeArray(typePointer(elemType.typeArguments(0)))
        else typeArray(elemType)
      }).asInstanceOf[PardisType[Any]]
      val innerElemType = (if (elemType.isArray) elemType.typeArguments(0) else elemType).asInstanceOf[TypeRep[Any]]
      val arr = s
      // if (elemType.isPrimitive) arrayUpdate(arr.asInstanceOf[Expression[Array[Any]]], i, apply(v))(newTp)
      // else if (elemType.name == "OptimalString")
      if (elemType.isPrimitive || elemType == OptimalStringType)
        pointer_assign_content(arr.asInstanceOf[Expression[Pointer[Any]]], i, apply(v))
      else if (v match {
        case Def(Cast(Constant(null))) => true
        case Constant(null)            => true
        case _                         => false
      }) {
        class T
        // implicit val typeT = apply(v).tp.typeArguments(0).asInstanceOf[TypeRep[T]]
        // implicit val typeT = newTp.typeArguments(0).typeArguments(0).asInstanceOf[TypeRep[T]]
        implicit val typeT = innerElemType.asInstanceOf[TypeRep[T]]
        val tArr = arr.asInstanceOf[Expression[Pointer[T]]]
        pointer_assign_content(tArr, i, unit(null))
      } // else if (elemType.isRecord) {
      //   class T
      //   implicit val typeT = apply(v).tp.typeArguments(0).asInstanceOf[TypeRep[T]]
      //   val newV = apply(v).asInstanceOf[Expression[Pointer[T]]]
      //   val vContent = *(newV)
      //   val tArr = arr.asInstanceOf[Expression[Pointer[T]]]
      //   pointer_assign(tArr, i, vContent)
      //   val pointerVar = newV match {
      //     case Def(ReadVar(v)) => v.asInstanceOf[Var[Pointer[T]]]
      //     case x               => Var(x.asInstanceOf[Rep[Var[Pointer[T]]]])
      //   }
      //   __assign(pointerVar, (&(tArr, i)).asInstanceOf[Rep[Pointer[T]]])(PointerType(typeT))
      else if (elemType.isRecord) {
        class T
        implicit val typeT = apply(v).tp.typeArguments(0).asInstanceOf[TypeRep[T]]
        // implicit val typeT = apply(v).tp.asInstanceOf[TypeRep[T]]
        // System.out.println(s"typeT: $typeT")
        val newV = apply(v).asInstanceOf[Expression[Pointer[T]]]
        def updateRecord() = {
          val vContent = *(newV)
          // System.out.println(s"vContent: ${vContent}:${vContent.tp}")
          val tArr = arr.asInstanceOf[Expression[Pointer[T]]]
          pointer_assign_content(tArr, i, vContent)
          val pointerVar = newV match {
            case Def(ReadVar(v)) => v.asInstanceOf[Var[Pointer[T]]]
            case x               => Var(x.asInstanceOf[Rep[Var[Pointer[T]]]])
          }
          __assign(pointerVar, (&(tArr.asInstanceOf[Rep[T]], i)) /*.asInstanceOf[Rep[Pointer[T]]]*/ )(PointerType(typeT))
        }
        if (settings.containerFlattenning) {
          __ifThenElse(newV __== unit(null), {
            class T1
            // implicit val typeT1 = newTp.typeArguments(0).typeArguments(0).asInstanceOf[TypeRep[T1]]
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
        System.out.println(s"v: ${apply(v)}: ${apply(v).tp} origTp: ${v.tp} elemTp: $elemType")
        pointer_assign_content(arr.asInstanceOf[Expression[Pointer[Any]]], i, *(apply(v).asInstanceOf[Expression[Pointer[Any]]])(v.tp.name match {
          case x if v.tp.isArray            => transformType(v.tp).typeArguments(0).asInstanceOf[PardisType[Any]]
          case x if x.startsWith("Pointer") => v.tp.typeArguments(0).asInstanceOf[PardisType[Any]]
          case _                            => v.tp
        }))
      }
  }
  rewrite += rule {
    case ArrayFilter(a, op) => //field(apply(a), "array")(transformType(a.tp))
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
      // val newTp = ({
      //   if (elemType.isArray) typePointer(typeCArray(elemType.typeArguments(0)))
      //   else typeArray(typePointer(elemType))
      // }).asInstanceOf[PardisType[Any]]
      // val arr = field(s, "array")(newTp)
      val newTp = ({
        if (elemType.isArray) typeArray(typePointer(elemType.typeArguments(0)))
        else typeArray(elemType)
      }).asInstanceOf[PardisType[Any]]
      val arr = s
      // <<<<<<< HEAD
      // if (elemType.isRecord) {
      // =======
      if (elemType.isPrimitive || elemType == OptimalStringType || elemType.isInstanceOf[SetType[_]] || elemType.isArray) ArrayApply(arr.asInstanceOf[Expression[Array[Any]]], apply(i))(newTp)
      else {
        // >>>>>>> merged-ir-str
        i match {
          case Constant(0) => Cast(arr)(arr.tp, typePointer(newTp))
          case _           => PTRADDRESS(arr.asInstanceOf[Expression[Pointer[Any]]], apply(i))(typePointer(newTp).asInstanceOf[PardisType[Pointer[Any]]])
        }
      }
    // } else ArrayApply(arr.asInstanceOf[Expression[Array[Any]]], apply(i))(newTp.asInstanceOf[PardisType[Any]])
    // class T
    // implicit val typeT = a.tp.typeArguments(0).asInstanceOf[PardisType[T]]
    // val newTp = ({
    //   if (elemType.isArray) typePointer(typeCArray(elemType.typeArguments(0)))
    //   else typeArray(typePointer(elemType))
    // }).asInstanceOf[PardisType[Any]]
    // val arr = field[Array[T]](s, "array")(newTp.asInstanceOf[PardisType[Array[T]]])
    // if (elemType.isPrimitive) arrayApply(arr, i)(newTp.asInstanceOf[PardisType[T]])
    // else &(arr, i)(newTp.typeArguments(0).typeArguments(0).asInstanceOf[PardisType[Array[T]]])
  }

  def __arrayLength[T: TypeRep](arr: Rep[Array[T]]): Rep[Int] = {
    // val s = apply(arr)
    // System.out.println(s"${scala.Console.RED}HAPPENNED HERE!${scala.Console.RESET}")
    // field[Int](s, "length")
    throw new Exception(s"${scala.Console.RED}array length is not supported for $arr!${scala.Console.RESET}")
  }

  rewrite += rule {
    case ArrayLength(a) =>
      __arrayLength(a)
  }
  rewrite += rule {
    case ArrayForeach(a, f) =>
      // val length = toAtom(apply(ArrayLength(a)))(IntType)
      val length = __arrayLength(a)
      // System.out.println(s"symbol for length $length")
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
