package ch.epfl.data
package legobase
package optimization

import scala.language.existentials
import pardis.shallow.OptimalString
import pardis.ir._
import pardis.types._
import pardis.types.PardisTypeImplicits._
import pardis.deep.scalalib._
import pardis.deep.scalalib.collection._
import legobase.deep._
import pardis.optimization._
import scala.language.implicitConversions
import pardis.utils.Utils._
import scala.reflect.runtime.universe
import pardis.ir.StructTags._
import cscala.CLangTypesDeep._
import cscala.GLibTypes._

trait CTransformer extends TopDownTransformerTraverser[LoweringLegoBase] {
  val IR: LoweringLegoBase
  import IR._
  import CNodes._

  implicit class PointerTypeOps[T](tp: TypeRep[T]) {
    def isPointerType: Boolean = tp match {
      case x: CTypes.PointerType[_] => true
      case _                        => false
    }
  }

  override def transformExp[T: TypeRep, S: TypeRep](exp: Rep[T]): Rep[S] = exp match {
    case t: typeOf[_] => typeOf()(apply(t.tp)).asInstanceOf[Rep[S]]
    case _            => super.transformExp[T, S](exp)
  }
}

object CTransformersPipeline extends TransformerHandler {
  def apply[Lang <: Base, T: PardisType](context: Lang)(block: context.Block[T]): context.Block[T] = {
    apply[T](context.asInstanceOf[LoweringLegoBase], block)
  }
  def apply[A: PardisType](context: LoweringLegoBase, b: PardisBlock[A]) = {
    val pipeline = new TransformerPipeline()
    pipeline += new GenericEngineToCTransformer(context)
    pipeline += new ScalaScannerToCFileTransformer(context)
    pipeline += new ScalaArrayToCStructTransformer(context)
    pipeline += new ScalaCollectionsToGLibTransfomer(context)
    pipeline += new Tuple2ToCTransformer(context)
    pipeline += new OptionToCTransformer(context)
    pipeline += new HashEqualsFuncsToCTraansformer(context)
    pipeline += new OptimalStringToCTransformer(context)
    pipeline += new RangeToCTransformer(context)
    pipeline += new ScalaConstructsToCTranformer(context)
    pipeline += new BlockFlattening(context)
    pipeline(context)(b)
  }
}

class GenericEngineToCTransformer(override val IR: LoweringLegoBase) extends RuleBasedTransformer[LoweringLegoBase](IR) {
  import IR._
  import CNodes._
  import CTypes._

  rewrite += rule {
    case GenericEngineRunQueryObject(b) =>
      val init = infix_asInstanceOf[TimeVal](unit(0))
      val diff = readVar(__newVar(init))
      val start = readVar(__newVar(init))
      val end = readVar(__newVar(init))
      gettimeofday(&(start))
      inlineBlock(apply(b))
      gettimeofday(&(end))
      val tm = timeval_subtract(&(diff), &(end), &(start))
      printf(unit("Generated code run in %ld milliseconds."), tm)
  }
  rewrite += rule { case GenericEngineParseStringObject(s) => s }
  rewrite += rule {
    case GenericEngineDateToStringObject(d) => NameAlias[String](None, "ltoa", List(List(d)))
  }
  rewrite += rule {
    case GenericEngineDateToYearObject(d) => d / unit(10000)
  }
  rewrite += rule {
    case GenericEngineParseDateObject(Constant(d)) =>
      val data = d.split("-").map(x => x.toInt)
      unit((data(0) * 10000) + (data(1) * 100) + data(2))
  }
}

// Mapping Scala Scanner operations to C FILE operations
class ScalaScannerToCFileTransformer(override val IR: LoweringLegoBase) extends RuleBasedTransformer[LoweringLegoBase](IR) {
  import IR._
  import CNodes._
  import CTypes._

  // TODO: Brainstorm about rewrite += tp abstraction for transforming types
  override def transformType[T: PardisType]: PardisType[Any] = ({
    val tp = typeRep[T]
    if (tp == typeK2DBScanner) typePointer(typeFile)
    else super.transformType[T]
  }).asInstanceOf[PardisType[Any]]

  rewrite += rule {
    case K2DBScannerNew(f) => CStdIO.fopen(f.asInstanceOf[Rep[LPointer[Char]]], unit("r"))
  }
  rewrite += rule {
    case K2DBScannerNext_int(s) =>
      val v = readVar(__newVar[Int](0))
      __ifThenElse(fscanf(apply(s), unit("%d|"), &(v)) __== eof, break, unit(()))
      v
  }
  rewrite += rule {
    case K2DBScannerNext_double(s) =>
      val v = readVar(__newVar(unit(0.0)))
      __ifThenElse(fscanf(apply(s), unit("%lf|"), &(v)) __== eof, break, unit)
      v
  }
  rewrite += rule {
    case K2DBScannerNext_char(s) =>
      val v = readVar(__newVar(unit('a')))
      __ifThenElse(fscanf(apply(s), unit("%c|"), &(v)) __== eof, break, unit)
      v
  }
  rewrite += rule {
    case nn @ K2DBScannerNext1(s, buf) =>
      var i = __newVar[Int](0)
      __whileDo(unit(true), {
        val v = readVar(__newVar[Byte](unit('a')))
        __ifThenElse(fscanf(apply(s), unit("%c"), &(v)) __== eof, break, unit)
        // have we found the end of line or end of string?
        __ifThenElse((v __== unit('|')) || (v __== unit('\n')), break, unit)
        buf(i) = v
        __assign(i, readVar(i) + unit(1))
      })
      buf(i) = unit('\u0000')
      readVar(i)
  }
  rewrite += rule {
    case K2DBScannerNext_date(s) =>
      val x = readVar(__newVar[Int](unit(0)))
      val y = readVar(__newVar[Int](unit(0)))
      val z = readVar(__newVar[Int](unit(0)))
      __ifThenElse(fscanf(apply(s), unit("%d-%d-%d|"), &(x), &(y), &(z)) __== eof, break, unit)
      (x * unit(10000)) + (y * unit(100)) + z
  }
  rewrite += rule { case K2DBScannerHasNext(s) => unit(true) }
  rewrite += rule {
    case LoaderFileLineCountObject(Constant(x: String)) =>
      val p = popen(unit("wc -l " + x), unit("r"))
      val cnt = readVar(__newVar[Int](0))
      fscanf(p, unit("%d"), &(cnt))
      pclose(p)
      cnt
  }
}

class ScalaArrayToCStructTransformer(override val IR: LoweringLegoBase) extends RuleBasedTransformer[LoweringLegoBase](IR) with CTransformer {
  import IR._
  import CNodes._
  import CTypes._

  override def transformType[T: PardisType]: PardisType[Any] = ({
    val tp = typeRep[T]
    tp match {
      case c if c.isPrimitive    => super.transformType[T]
      case ArrayBufferType(args) => typePointer(typeGArray(transformType(args)))
      // case ArrayType(x) if x == ByteType => typePointer(ByteType)
      case ArrayType(args) => typePointer(typeCArray({
        if (args.isArray) typeCArray(args)
        else args
      }))
      case c if c.isRecord => tp.typeArguments match {
        case Nil     => typePointer(tp)
        case List(t) => typePointer(transformType(t))
      }
      case TreeSetType(args) => typePointer(typeGTree(transformType(args)))
      case SetType(args)     => typePointer(typeLPointer(typeLGList))
      case OptionType(args)  => typePointer(transformType(args))
      case _                 => super.transformType[T]
    }
  }).asInstanceOf[PardisType[Any]]

  rewrite += rule {
    case pc @ PardisCast(x) => PardisCast(apply(x))(apply(pc.castFrom), apply(pc.castTp))
  }

  rewrite += rule {
    case a @ ArrayNew(x) =>
      if (a.tp.typeArguments(0).isArray) {
        // Get type of elements stored in array
        val elemType = typeCArray(a.tp.typeArguments(0).typeArguments(0))
        // Allocate original array
        val array = malloc(x)(elemType)
        // Create wrapper with length
        val am = typeCArray(typeCArray(a.tp.typeArguments(0).typeArguments(0))).asInstanceOf[PardisType[CArray[CArray[Any]]]] //transformType(a.tp)
        val s = toAtom(
          PardisStruct(StructTags.ClassTag(structName(am)),
            List(PardisStructArg("array", false, array), PardisStructArg("length", false, x)),
            List())(am))(am)
        val m = malloc(unit(1))(am)
        structCopy(m.asInstanceOf[Expression[Pointer[Any]]], s)
        m.asInstanceOf[Expression[Any]]
      } else {
        // Get type of elements stored in array
        val elemType = if (a.tp.typeArguments(0).isRecord) a.tp.typeArguments(0) else transformType(a.tp.typeArguments(0))
        // Allocate original array
        val array = malloc(x)(elemType)
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
      }).asInstanceOf[PardisType[Any]] //if (elemType.isPrimitive) elemType else typePointer(elemType)
      // Read array and perform update
      val arr = field(s, "array")(newTp)
      if (elemType.isPrimitive) arrayUpdate(arr.asInstanceOf[Expression[Array[Any]]], i, apply(v))
      else if (elemType.name == "OptimalString")
        pointer_assign(arr.asInstanceOf[Expression[Pointer[Any]]], i, apply(v))
      else if (v match {
        case Def(Cast(Constant(null))) => true
        case Constant(null)            => true
        case _                         => false
      }) {
        class T
        // implicit val typeT = apply(v).tp.typeArguments(0).asInstanceOf[TypeRep[T]]
        implicit val typeT = newTp.typeArguments(0).typeArguments(0).asInstanceOf[TypeRep[T]]
        val tArr = arr.asInstanceOf[Expression[Pointer[T]]]
        pointer_assign(tArr, i, unit(null))
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
        val newV = apply(v).asInstanceOf[Expression[Pointer[T]]]
        __ifThenElse(apply(v) __== unit(null), {
          class T1
          implicit val typeT1 = newTp.typeArguments(0).typeArguments(0).asInstanceOf[TypeRep[T1]]
          val tArr = arr.asInstanceOf[Expression[Pointer[T1]]]
          pointer_assign(tArr, i, unit(null))
        }, {
          val vContent = *(newV)
          val tArr = arr.asInstanceOf[Expression[Pointer[T]]]
          pointer_assign(tArr, i, vContent)
          val pointerVar = newV match {
            case Def(ReadVar(v)) => v.asInstanceOf[Var[Pointer[T]]]
            case x               => Var(x.asInstanceOf[Rep[Var[Pointer[T]]]])
          }
          __assign(pointerVar, (&(tArr, i)).asInstanceOf[Rep[Pointer[T]]])(PointerType(typeT))
        })
      } else {
        pointer_assign(arr.asInstanceOf[Expression[Pointer[Any]]], i, apply(v).asInstanceOf[Expression[Pointer[Any]]])
      }
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
      if (elemType.isRecord) {
        i match {
          case Constant(0) => Cast(arr)(arr.tp, typePointer(newTp))
          case _           => PTRADDRESS(arr.asInstanceOf[Expression[Pointer[Any]]], apply(i))(typePointer(newTp).asInstanceOf[PardisType[Pointer[Any]]])
        }
      } else ArrayApply(arr.asInstanceOf[Expression[Array[Any]]], apply(i))(newTp.asInstanceOf[PardisType[Any]])
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
  rewrite += rule {
    case ArrayLength(a) =>
      val s = apply(a)
      val arr = field(s, "length")(IntType)
      arr
  }
  rewrite += rule {
    case ArrayForeach(a, f) =>
      // TODO if we use recursive rule based, the next line will be cleaner
      Range(unit(0), toAtom(apply(ArrayLength(a)))(IntType)).foreach {
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

class ScalaCollectionsToGLibTransfomer(override val IR: LoweringLegoBase) extends RecursiveRuleBasedTransformer[LoweringLegoBase](IR) {
  import IR._
  import CNodes._
  import CTypes._
  import LGHashTableHeader._
  import LGListHeader._
  import LGTreeHeader._
  import LGArrayHeader._

  override def transformType[T: PardisType]: PardisType[Any] = ({
    val tp = typeRep[T]
    tp match {
      case ArrayBufferType(t)  => typePointer(typeGArray(transformType(t)))
      case SeqType(t)          => typePointer(typeLPointer(typeLGList))
      case TreeSetType(t)      => typePointer(typeGTree(transformType(t)))
      case SetType(t)          => typePointer(typeLPointer(typeLGList))
      case OptionType(t)       => typePointer(transformType(t))
      case HashMapType(t1, t2) => typePointer(typeGHashTable(transformType(t1), transformType(t2)))
      case CArrayType(t1)      => tp
      case _                   => super.transformType[T]
    }
  }).asInstanceOf[PardisType[Any]]

  /* HashMap Operations */
  def getMapKeyType[A: PardisType, B: PardisType](map: Expression[HashMap[A, B]]): PardisType[Any] =
    map.tp.typeArguments(0).typeArguments(1).asInstanceOf[PardisType[Any]]
  def getMapValueType[A: PardisType, B: PardisType](map: Expression[HashMap[A, B]]): PardisType[Any] =
    map.tp.typeArguments(0).typeArguments(1).asInstanceOf[PardisType[Any]]

  rewrite += rule {
    case nm @ HashMapNew3(_, _) =>
      apply(HashMapNew()(nm.typeA, ArrayBufferType(nm.typeB)))
    case nm @ HashMapNew4(_, _) =>
      apply(HashMapNew()(nm.typeA, nm.typeB))
    case nm @ HashMapNew() =>
      val nA = typePointer(transformType(nm.typeA))
      val nB = typePointer(transformType(nm.typeB))
      def hashFunc[T: TypeRep] = transformDef(doLambdaDef((s: Rep[T]) => infix_hashCode(s)))
      def equalsFunc[T: TypeRep] = transformDef(doLambda2Def((s1: Rep[T], s2: Rep[T]) => infix_==(s1, s2)))
      if (nm.typeA.isPrimitive || nm.typeA == StringType || nm.typeA == OptimalStringType)
        GHashTableNew(hashFunc(nm.typeA), equalsFunc(nm.typeA))(nm.typeA, nB, IntType)
      else GHashTableNew(hashFunc(nA), equalsFunc(nA))(nA, nB, IntType)
  }
  rewrite += rule {
    case HashMapSize(map) => g_hash_table_size(map.asInstanceOf[Rep[LPointer[LGHashTable]]])
  }
  rewrite += rule {
    case HashMapKeySet(map) => g_hash_table_get_keys(map.asInstanceOf[Rep[LPointer[LGHashTable]]])
  }
  rewrite += rule {
    case HashMapContains(map, key) =>
      val z = toAtom(transformDef(HashMapApply(map, key)))(getMapValueType(apply(map)))
      infix_!=(z, unit(null))
  }
  rewrite += rule {
    case ma @ HashMapApply(map, key) =>
      g_hash_table_lookup(map.asInstanceOf[Rep[LPointer[LGHashTable]]], key.asInstanceOf[Rep[gconstpointer]])
  }
  rewrite += rule {
    case mu @ HashMapUpdate(map, key, value) =>
      g_hash_table_insert(map.asInstanceOf[Rep[LPointer[LGHashTable]]],
        key.asInstanceOf[Rep[gconstpointer]],
        value.asInstanceOf[Rep[gpointer]])
  }
  rewrite += rule {
    case hmgu @ HashMapGetOrElseUpdate(map, key, value) =>
      val ktp = getMapKeyType(apply(map))
      val vtp = getMapValueType(apply(map))
      val v = toAtom(transformDef(HashMapApply(map, key)(ktp, vtp))(vtp))(vtp)
      __ifThenElse(infix_==(v, unit(null)), {
        val res = inlineBlock(apply(value))
        toAtom(HashMapUpdate(map, key, res))
        res
      }, v)(v.tp)
  }
  rewrite += rule {
    case mr @ HashMapRemove(map, key) =>
      val x = toAtom(transformDef(HashMapApply(map, key)))(getMapValueType(apply(map)))
      g_hash_table_remove(map.asInstanceOf[Rep[LPointer[LGHashTable]]], key.asInstanceOf[Rep[gconstpointer]])
      x
  }
  rewrite += rule {
    case hmfe @ HashMapForeach(map, f) => {
      def func[K: TypeRep, V: TypeRep] = doLambda3((s1: Rep[gpointer], s2: Rep[gpointer], s3: Rep[gpointer]) => {
        lambdaApply(f,
          __newTuple2(infix_asInstanceOf[K](s1),
            infix_asInstanceOf[V](s2)))
      })
      val ktp = getMapKeyType(apply(map))
      val vtp = getMapValueType(apply(map))
      g_hash_table_foreach(
        map.asInstanceOf[Rep[LPointer[LGHashTable]]],
        func(ktp, vtp).asInstanceOf[Rep[GHFunc]],
        CLang.NULL[Any])
    }
  }

  /* Set Operations */
  rewrite += rule { case SetApplyObject1(s) => s }
  rewrite += rule {
    case nm @ SetApplyObject2() =>
      val s = CStdLib.malloc[LPointer[LGList]](1)
      CLang.pointer_assign(s, CLang.NULL[LGList])
      s
  }
  rewrite += rule {
    case sh @ SetHead(s) =>
      val elemType = if (sh.typeA.isRecord) typeLPointer(sh.typeA) else sh.typeA
      val glist = CLang.*(s.asInstanceOf[Rep[LPointer[LPointer[LGList]]]])
      infix_asInstanceOf(g_list_nth_data(glist, unit(0)))(elemType)
  }
  rewrite += rule {
    case SetRemove(s, e) =>
      val glist = CLang.*(s.asInstanceOf[Rep[LPointer[LPointer[LGList]]]])
      val newHead = g_list_remove(glist, apply(e).asInstanceOf[Rep[LPointer[Any]]])
      CLang.pointer_assign(s.asInstanceOf[Rep[LPointer[LPointer[LGList]]]], newHead)
  }
  rewrite += rule {
    case SetToSeq(set) =>
      set
  }
  rewrite += rule {
    case Set$plus$eq(s, e) =>
      val glist = CLang.*(s.asInstanceOf[Rep[LPointer[LPointer[LGList]]]])
      val newHead = g_list_prepend(glist, apply(e).asInstanceOf[Rep[LPointer[Any]]])
      CLang.pointer_assign(s.asInstanceOf[Rep[LPointer[LPointer[LGList]]]], newHead)
  }
  rewrite += rule {
    case sfe @ SetForeach(s, f) =>
      val elemType = if (sfe.typeA.isRecord) typeLPointer(sfe.typeA) else sfe.typeA
      val l = __newVar(CLang.*(s.asInstanceOf[Rep[LPointer[LPointer[LGList]]]]))
      __whileDo(__readVar(l) __!= CLang.NULL[LGList], {
        val elem = infix_asInstanceOf(g_list_nth_data(readVar(l), unit(0)))(elemType)
        __assign(l, g_list_next(readVar(l)))
        inlineFunction(f, elem)
        unit(())
      })
  }
  rewrite += rule {
    case sf @ SetFind(s, f) =>
      val elemType = if (sf.typeA.isRecord) typeLPointer(sf.typeA) else sf.typeA
      val result = __newVar(unit(null).asInstanceOf[Rep[Any]])(elemType.asInstanceOf[TypeRep[Any]])
      val l = __newVar(CLang.*(s.asInstanceOf[Rep[LPointer[LPointer[LGList]]]]))
      __whileDo(__readVar(l) __!= CLang.NULL[LGList], {
        val elem = infix_asInstanceOf(g_list_nth_data(readVar(l), unit(0)))(elemType)
        __assign(l, g_list_next(readVar(l)))
        val found = inlineFunction(f, elem)
        __ifThenElse(found, {
          __assign(result, elem)
          break()
        }, unit(()))
      })
      optionApplyObject(readVar(result)(elemType.asInstanceOf[TypeRep[Any]]))
  }
  rewrite += rule {
    case se @ SetExists(s, f) =>
      class X
      class Y
      implicit val typeX = se.typeA.asInstanceOf[TypeRep[X]]
      val set = s.asInstanceOf[Rep[Set[X]]]
      val fun = f.asInstanceOf[Rep[((X) => Boolean)]]
      val found = set.find(fun)
      found.nonEmpty
  }
  rewrite += rule {
    case sfl @ SetFoldLeft(s, z, f) =>
      val elemType = if (sfl.typeA.isRecord) typeLPointer(sfl.typeA) else sfl.typeA
      val state = __newVar(z)(sfl.typeB)
      val l = __newVar(CLang.*(s.asInstanceOf[Rep[LPointer[LPointer[LGList]]]]))
      __whileDo(__readVar(l) __!= CLang.NULL[LGList], {
        val elem = infix_asInstanceOf(g_list_nth_data(readVar(l), unit(0)))(elemType)
        val newState = inlineFunction(f, __readVar(state)(sfl.typeB), elem)
        __assign(state, newState)(sfl.typeB)
        __assign(l, g_list_next(readVar(l)))
      })
      __readVar(state)(sfl.typeB)
  }
  rewrite += rule {
    case SetSize(s) =>
      val l = CLang.*(s.asInstanceOf[Rep[LPointer[LPointer[LGList]]]])
      g_list_length(l)
  }
  rewrite += rule {
    case sr @ SetRetain(s, f) =>
      val elemType = if (sr.typeA.isRecord) typeLPointer(sr.typeA) else sr.typeA
      val l = __newVar(CLang.*(s.asInstanceOf[Rep[LPointer[LPointer[LGList]]]]))
      val prevPtr = __newVar(s.asInstanceOf[Rep[LPointer[LPointer[LGList]]]])

      __whileDo(__readVar(l) __!= CLang.NULL[LGList], {
        val elem = infix_asInstanceOf(g_list_nth_data(readVar(l), unit(0)))(elemType)
        val keep = inlineFunction(f, elem)
        __ifThenElse(keep, {
          //__assign(prevPtr, CLang.&(CLang.->[LGList, LPointer[LGList]](__readVar(l), unit("next"))))
          __assign(prevPtr, CLang.&(field[LPointer[LGList]](__readVar[LPointer[LGList]](l), "next")))
        }, {
          CLang.pointer_assign(readVar(prevPtr), g_list_next(readVar(l)))
        })
        __assign(l, g_list_next(readVar(l)))
      })
  }

  rewrite += rule {
    case smb @ SetMinBy(s, f) =>
      class X
      class Y
      implicit val typeX = (if (smb.typeA.isRecord) typeLPointer(smb.typeA) else smb.typeA).asInstanceOf[TypeRep[X]]
      implicit val typeY = smb.typeB.asInstanceOf[TypeRep[Y]]
      val set = s.asInstanceOf[Rep[Set[X]]]
      val fun = f.asInstanceOf[Rep[((X) => Y)]]

      val l = __newVar(CLang.*(set.asInstanceOf[Rep[LPointer[LPointer[LGList]]]]))
      val first = infix_asInstanceOf[X](g_list_nth_data(__readVar(l), unit(0)))
      val result = __newVar(first)
      val min = __newVar(inlineFunction(fun, __readVar(result)))

      val cmp = OrderingRep(smb.typeB)

      __whileDo(__readVar(l) __!= CLang.NULL[LGList], {
        val elem = infix_asInstanceOf[X](g_list_nth_data(__readVar(l), unit(0)))
        __assign(l, g_list_next(readVar(l)))
        val newMin = inlineFunction(fun, elem)
        __ifThenElse(cmp.lt(newMin, __readVar[Y](min)), {
          __assign(min, newMin)
          __assign(result, elem)
        }, unit(()))
      })
      __readVar(result)
  }

  /* TreeSet Operations */
  rewrite += remove { case OrderingNew(o) => () }

  rewrite += rule {
    case ts @ TreeSetNew2(Def(OrderingNew(Def(Lambda2(f, i1, i2, o))))) =>
      val compare = Lambda2(f, i2.asInstanceOf[Rep[LPointer[Any]]], i1.asInstanceOf[Rep[LPointer[Any]]], transformBlock(o))
      g_tree_new(CLang.&(compare))
  }
  rewrite += rule {
    case TreeSet$plus$eq(t, s) => g_tree_insert(t.asInstanceOf[Rep[LPointer[LGTree]]], s.asInstanceOf[Rep[gpointer]], s.asInstanceOf[Rep[gpointer]])
  }
  rewrite += rule {
    case TreeSet$minus$eq(self, t) =>
      g_tree_remove(self.asInstanceOf[Rep[LPointer[LGTree]]], t.asInstanceOf[Rep[gconstpointer]])
  }
  rewrite += rule {
    case TreeSetSize(t) => g_tree_nnodes(t.asInstanceOf[Rep[LPointer[LGTree]]])
  }
  rewrite += rule {
    case op @ TreeSetHead(t) =>
      def treeHead[T: TypeRep] = doLambda3((s1: Rep[gpointer], s2: Rep[gpointer], s3: Rep[gpointer]) => {
        CLang.pointer_assign(infix_asInstanceOf[LPointer[T]](s3), infix_asInstanceOf[T](s2))
        unit(0)
      })
      class X
      implicit val elemType = t.tp.typeArguments(0).typeArguments(0).asInstanceOf[TypeRep[X]]
      val init = CLang.NULL[Any]
      g_tree_foreach(t.asInstanceOf[Rep[LPointer[LGTree]]], (treeHead(elemType)).asInstanceOf[Rep[LPointer[(gpointer, gpointer, gpointer) => Int]]], CLang.&(init).asInstanceOf[Rep[gpointer]])
      init.asInstanceOf[Rep[LPointer[Any]]]
      infix_asInstanceOf[X](init)
  }

  /* ArrayBuffer Operations */
  rewrite += rule {
    case abn @ (ArrayBufferNew2() | ArrayBufferNew3()) =>
      class X
      implicit val tpX = abn.tp.typeArguments(0).asInstanceOf[TypeRep[X]]
      g_array_new(0, 1, CLang.sizeof[X])
  }
  rewrite += rule {
    case aba @ ArrayBufferApply(a, i) =>
      class X
      implicit val tp = (if (aba.tp.isPrimitive) aba.tp else typePointer(aba.tp)).asInstanceOf[TypeRep[X]]
      g_array_index[X](a.asInstanceOf[Rep[LPointer[LGArray]]], i)
  }
  rewrite += rule {
    case ArrayBufferAppend(a, e) =>
      g_array_append_vals(apply(a).asInstanceOf[Rep[LPointer[LGArray]]], CLang.&(e.asInstanceOf[Rep[gconstpointer]]), 1)
  }
  rewrite += rule {
    case ArrayBufferSize(a) =>
      CLang.->[LGArray, Int](a.asInstanceOf[Rep[LPointer[LGArray]]], unit("len"))
  }
  rewrite += rule {
    case ArrayBufferRemove(a, e) =>
      g_array_remove_index(a.asInstanceOf[Rep[LPointer[LGArray]]], e)
  }
  rewrite += rule {
    case ArrayBufferMinBy(a, f @ Def(Lambda(fun, input, o))) =>
      val len = transformDef(ArrayBufferSize(a))
      val i = __newVar[Int](1)
      val tp = a.tp.typeArguments(0).typeArguments(0).asInstanceOf[PardisType[Any]]
      val varInit = toAtom(transformDef(ArrayBufferApply(apply(a), unit(0))(tp))(tp))(tp)
      val min = __newVar(varInit)(tp)
      val minBy = __newVar(fun(readVar(min)(tp)).asInstanceOf[Expression[Int]])
      __whileDo(readVar(i) < len, {
        val e = toAtom(transformDef(ArrayBufferApply(apply(a), readVar(i))(tp))(tp))(tp)
        val eminBy = fun(e).asInstanceOf[Expression[Int]]
        __ifThenElse(eminBy < readVar(minBy), {
          __assign(min, e)
          __assign(minBy, eminBy)
        }, unit())
        __assign(i, readVar(i) + unit(1))
      })
      ReadVar(min)(tp)
  }
  rewrite += rule {
    case ArrayBufferFoldLeft(a, cnt, Def(Lambda2(fun, input1, input2, o))) =>
      var idx = __newVar[Int](0)
      val len = transformDef(ArrayBufferSize(a))
      var agg = __newVar(cnt)(cnt.tp)
      __whileDo(readVar(idx) < len, {
        val tp = a.tp.typeArguments(0).typeArguments(0).asInstanceOf[PardisType[Any]]
        val e = toAtom(apply(ArrayBufferApply(apply(a), readVar(idx))(tp)))(tp)
        __assign(agg, fun(readVar(agg)(cnt.tp), e))
        __assign(idx, readVar(idx) + unit(1))
      })
      ReadVar(agg)
  }
}

class HashEqualsFuncsToCTraansformer(override val IR: LoweringLegoBase) extends RecursiveRuleBasedTransformer[LoweringLegoBase](IR) with CTransformer with StructCollector[LoweringLegoBase] {
  import IR._
  import CNodes._
  import CTypes._

  // Handling proper hascode and equals
  def __isRecord(e: Expression[Any]) = e.tp.isRecord || (e.tp.isPointerType && e.tp.typeArguments(0).isRecord)
  object Equals {
    def unapply(node: Def[Any]): Option[(Rep[Any], Rep[Any], Boolean)] = node match {
      case Equal(a, b)    => Some((a, b, true))
      case NotEqual(a, b) => Some((a, b, false))
      case _              => None
    }
  }

  val alreadyEquals = scala.collection.mutable.ArrayBuffer[Rep[Any]]()

  rewrite += rule {
    case Equals(e1, Constant(null), isEqual) if (e1.tp == OptimalStringType || e1.tp == StringType) && !alreadyEquals.contains(e1) =>
      alreadyEquals += e1
      if (isEqual) (e1 __== unit(null)) || !strcmp(e1, unit("")) else (e1 __!= unit(null)) && strcmp(e1, unit(""))
    case Equals(e1, e2, isEqual) if (e1.tp == OptimalStringType || e1.tp == StringType) && !alreadyEquals.contains(e1) =>
      if (isEqual) !strcmp(e1, e2) else strcmp(e1, e2)
    case Equals(e1, Constant(null), isEqual) if __isRecord(e1) && !alreadyEquals.contains(e1) =>
      val structDef = if (e1.tp.isRecord)
        getStructDef(e1.tp).get
      else
        getStructDef(e1.tp.typeArguments(0)).get
      // System.out.println(structDef.fields)
      alreadyEquals += e1
      structDef.fields.filter(_.name != "next").find(f => f.tpe.isPointerType || f.tpe == OptimalStringType || f.tpe == StringType) match {
        case Some(firstField) =>
          def fieldExp = field(e1, firstField.name)(firstField.tpe)
          if (isEqual)
            (e1 __== unit(null)) || (fieldExp __== unit(null))
          else
            (e1 __!= unit(null)) && (fieldExp __!= unit(null))
        case None => {
          if (isEqual)
            (e1 __== unit(null))
          else
            (e1 __!= unit(null))
        }
      }
    // case Equals(e1, e2, isEqual) if __isRecord(e1) && __isRecord(e2) =>
    //   class T
    //   implicit val ttp = (if (e1.tp.isRecord) e1.tp else e1.tp.typeArguments(0)).asInstanceOf[TypeRep[T]]
    //   val eq = getStructEqualsFunc[T]()
    //   val res = inlineFunction(eq, e1.asInstanceOf[Rep[T]], e2.asInstanceOf[Rep[T]])
    //   if (isEqual) res else !res
    case Equals(e1, e2, isEqual) if __isRecord(e1) && __isRecord(e2) =>
      val ttp = (if (e1.tp.isRecord) e1.tp else e1.tp.typeArguments(0))
      val structDef = getStructDef(ttp).get
      val res = structDef.fields.filter(_.name != "next").map(f => field(e1, f.name)(f.tpe) __== field(e2, f.name)(f.tpe)).reduce(_ && _)
      // val eq = getStructEqualsFunc[T]()
      // val res = inlineFunction(eq, e1.asInstanceOf[Rep[T]], e2.asInstanceOf[Rep[T]])
      if (isEqual) res else !res
  }
  rewrite += rule {
    case HashCode(t) if t.tp == StringType => unit(0) // KEY is constant. No need to hash anything
    // case HashCode(t) if __isRecord(t) =>
    //   val tp = t.tp.asInstanceOf[PardisType[Any]]
    //   val hashFunc = {
    //     if (t.tp.isRecord) getStructHashFunc[Any]()(tp)
    //     else getStructHashFunc[Any]()(tp.typeArguments(0).asInstanceOf[TypeRep[Any]])
    //   }
    //   val hf = toAtom(hashFunc)(hashFunc.tp)
    //   inlineFunction(hf.asInstanceOf[Rep[Any => Int]], apply(t))
    case HashCode(e) if __isRecord(e) =>
      val ttp = (if (e.tp.isRecord) e.tp else e.tp.typeArguments(0))
      val structDef = getStructDef(ttp).get
      structDef.fields.map(f => infix_hashCode(field(e, f.name)(f.tpe))).reduce(_ + _)
    case HashCode(t) if t.tp.isPrimitive      => infix_asInstanceOf[Int](t)
    case HashCode(t) if t.tp == CharacterType => infix_asInstanceOf[Int](t)
    case HashCode(t) if t.tp == OptimalStringType =>
      val len = t.asInstanceOf[Expression[OptimalString]].length
      val idx = __newVar[Int](0)
      val h = __newVar[Int](0)
      __whileDo(readVar(idx) < len, {
        __assign(h, readVar(h) + OptimalStringApply(t.asInstanceOf[Expression[OptimalString]], readVar(idx)))
        __assign(idx, readVar(idx) + unit(1));
      })
      readVar(h)
    case HashCode(t) if t.tp.isArray => arrayLength(t.asInstanceOf[Rep[Array[Any]]])
    //case HashCode(t)                 => throw new Exception("Unhandled type " + t.tp.toString + " passed to HashCode")
  }
  rewrite += rule {
    case ToString(obj) => obj.tp.asInstanceOf[PardisType[_]] match {
      case PointerType(tpe) if tpe.isRecord => {
        val structFields = {
          val structDef = getStructDef(tpe).get
          structDef.fields
        }
        def getDescriptor(field: StructElemInformation): String = field.tpe.asInstanceOf[PardisType[_]] match {
          case IntType | ShortType            => "%d"
          case DoubleType | FloatType         => "%f"
          case LongType                       => "%lf"
          case StringType | OptimalStringType => "%s"
          case ArrayType(elemTpe)             => s"Array[$elemTpe]"
          case tp                             => tp.toString
        }
        val fieldsWithDescriptor = structFields.map(f => f -> getDescriptor(f))
        val descriptor = tpe.name + "(" + fieldsWithDescriptor.map(f => f._2).mkString(", ") + ")"
        val fields = fieldsWithDescriptor.collect {
          case f if f._2.startsWith("%") => {
            val tp = f._1.tpe
            field(obj, f._1.name)(tp)
          }
        }
        val str = malloc(4096)(CharType)
        sprintf(str.asInstanceOf[Rep[String]], unit(descriptor), fields: _*)
        str.asInstanceOf[Rep[String]]
      }
      case tp => throw new Exception(s"toString conversion for non-record type $tp is not handled for the moment")
    }
  }
}

class OptimalStringToCTransformer(override val IR: LoweringLegoBase) extends RecursiveRuleBasedTransformer[LoweringLegoBase](IR) with CTransformer {
  import IR._
  import CNodes._
  import CTypes._
  import cscala.CLangTypes

  private[OptimalStringToCTransformer] implicit def optimalStringToCharPointer(x: Rep[OptimalString]) = x.asInstanceOf[Rep[LPointer[Char]]]

  implicit class OptimalStringRep(self: Rep[OptimalString]) {
    def getBaseValue(s: Rep[OptimalString]): Rep[LPointer[Char]] = apply(s).asInstanceOf[Rep[LPointer[Char]]]
  }

  rewrite += rule { case OptimalStringNew(self) => self }
  rewrite += rule {
    case OptimalStringString(self) =>
      new OptimalStringRep(self).getBaseValue(self)
  }
  rewrite += rule {
    case OptimalStringDiff(self, y) =>
      CString.strcmp(new OptimalStringRep(self).getBaseValue(self), new OptimalStringRep(self).getBaseValue(y))
  }
  rewrite += rule {
    case OptimalStringEndsWith(self, y) =>
      {
        val lenx: Rep[ch.epfl.data.cscala.CLangTypes.CSize] = CString.strlen(new OptimalStringRep(self).getBaseValue(self));
        val leny: Rep[ch.epfl.data.cscala.CLangTypes.CSize] = CString.strlen(new OptimalStringRep(self).getBaseValue(y));
        val len: Rep[Int] = lenx.$minus(leny);
        infix_$eq$eq(CString.strncmp(CLang.pointer_add[Char](new OptimalStringRep(self).getBaseValue(self), len)(typeRep[Char], CLangTypes.charType), new OptimalStringRep(self).getBaseValue(y), len), unit(0))
      }
  }
  rewrite += rule {
    case OptimalStringStartsWith(self, y) =>
      infix_$eq$eq(CString.strncmp(new OptimalStringRep(self).getBaseValue(self), new OptimalStringRep(self).getBaseValue(y), CString.strlen(new OptimalStringRep(self).getBaseValue(y))), unit(0))
  }
  rewrite += rule {
    case OptimalStringCompare(self, y) =>
      CString.strcmp(new OptimalStringRep(self).getBaseValue(self), new OptimalStringRep(self).getBaseValue(y))
  }
  rewrite += rule { case OptimalStringCompare(x, y) => strcmp(x, y) }
  rewrite += rule { case OptimalStringCompare(x, y) => strcmp(x, y) }
  rewrite += rule { case OptimalStringLength(x) => strlen(x) }
  rewrite += rule { case OptimalString$eq$eq$eq(x, y) => strcmp(x, y) __== unit(0) }
  rewrite += rule { case OptimalString$eq$bang$eq(x, y) => infix_!=(strcmp(x, y), unit(0)) }
  rewrite += rule { case OptimalStringContainsSlice(x, y) => infix_!=(strstr(x, y), unit(null)) }
  rewrite += rule {
    case OptimalStringLength(self) =>
      CString.strlen(new OptimalStringRep(self).getBaseValue(self))
  }
  rewrite += rule {
    case OptimalString$eq$eq$eq(self, y) =>
      infix_$eq$eq(CString.strcmp(new OptimalStringRep(self).getBaseValue(self), new OptimalStringRep(self).getBaseValue(y)), unit(0))
  }
  rewrite += rule {
    case OptimalString$eq$bang$eq(self, y) =>
      infix_$bang$eq(CString.strcmp(new OptimalStringRep(self).getBaseValue(self), new OptimalStringRep(self).getBaseValue(y)), unit(0))
  }
  rewrite += rule {
    case OptimalStringContainsSlice(self, y) =>
      infix_$bang$eq(CString.strstr(new OptimalStringRep(self).getBaseValue(self), new OptimalStringRep(self).getBaseValue(y)), CLang.NULL[Char])
  }
  rewrite += rule {
    case OptimalStringIndexOfSlice(self, y, idx) =>
      {
        val substr: Rep[ch.epfl.data.cscala.CLangTypes.LPointer[Char]] = CString.strstr(CLang.pointer_add[Char](new OptimalStringRep(self).getBaseValue(self), idx)(typeRep[Char], CLangTypes.charType), new OptimalStringRep(self).getBaseValue(y));
        __ifThenElse(infix_$eq$eq(substr, CLang.NULL[Char]), unit(-1), CString.str_subtract(substr, new OptimalStringRep(self).getBaseValue(self)))
      }
  }
  rewrite += rule {
    case OptimalStringApply(self, idx) =>
      CLang.*(CLang.pointer_add[Char](new OptimalStringRep(self).getBaseValue(self), idx)(typeRep[Char], CLangTypes.charType))
  }
  rewrite += rule {
    case OptimalStringSlice(self, start, end) =>
      {
        val len: Rep[Int] = end.$minus(start).$plus(unit(1));
        val newbuf: Rep[ch.epfl.data.cscala.CLangTypes.LPointer[Char]] = CStdLib.malloc[Char](len);
        CString.strncpy(newbuf, CLang.pointer_add[Char](new OptimalStringRep(self).getBaseValue(self), start)(typeRep[Char], CLangTypes.charType), len.$minus(unit(1)));
        newbuf
      }
  }
}

class RangeToCTransformer(override val IR: LoweringLegoBase) extends RecursiveRuleBasedTransformer[LoweringLegoBase](IR) with CTransformer {
  import IR._
  import CNodes._
  import CTypes._

  rewrite += remove { case RangeApplyObject(_, _) => () }
  rewrite += remove { case RangeNew(start, end, step) => () }
  rewrite += rule {
    case RangeForeach(self @ Def(RangeApplyObject(start, end)), f) =>
      val i = fresh[Int]
      val body = reifyBlock {
        inlineFunction(f.asInstanceOf[Rep[Int => Unit]], i)
      }
      For(start, end, unit(1), i, body)
  }
  rewrite += rule {
    case RangeForeach(Def(RangeNew(start, end, step)), Def(Lambda(f, i1, o))) =>
      PardisFor(start, end, step, i1.asInstanceOf[Expression[Int]], reifyBlock({ o }).asInstanceOf[PardisBlock[Unit]])
  }
}

class OptionToCTransformer(override val IR: LoweringLegoBase) extends RecursiveRuleBasedTransformer[LoweringLegoBase](IR) with CTransformer {
  import IR._
  import CNodes._
  import CTypes._

  rewrite += rule { case OptionApplyObject(x) => x }

  rewrite += rule { case OptionGet(x) => x }

  rewrite += rule { case OptionNonEmpty(x) => infix_!=(x, unit(null)) }

}

class Tuple2ToCTransformer(override val IR: LoweringLegoBase) extends RecursiveRuleBasedTransformer[LoweringLegoBase](IR) with CTransformer {
  import IR._
  import CNodes._
  import CTypes._

  // rewrite += rule { case Tuple2ApplyObject(_1, _2) => __new(("_1", false, _1), ("_2", false, _2))(Tuple2Type(_1.tp, _2.tp)) }

  // rewrite += rule { case n @ Tuple2_Field__1(self) => field(apply(self), "_1")((n.tp)) }

  // rewrite += rule { case n @ Tuple2_Field__2(self) => field(apply(self), "_2")((n.tp)) }

  object Tuple2Create {
    def unapply[T](d: Def[T]): Option[(Rep[Any], Rep[Any])] = d match {
      case Tuple2ApplyObject(_1, _2) => Some(_1 -> _2)
      case Tuple2New(_1, _2)         => Some(_1 -> _2)
      case _                         => None
    }
  }

  rewrite += remove { case Tuple2Create(_1, _2) => () }

  rewrite += rule { case n @ Tuple2_Field__1(Def(Tuple2Create(_1, _2))) => _1 }

  rewrite += rule { case n @ Tuple2_Field__2(Def(Tuple2Create(_1, _2))) => _2 }

}

class ScalaConstructsToCTranformer(override val IR: LoweringLegoBase) extends RecursiveRuleBasedTransformer[LoweringLegoBase](IR) with CTransformer {
  import IR._
  import CNodes._
  import CTypes._

  rewrite += rule {
    case PardisIfThenElse(cond, thenp, elsep) if thenp.tp != UnitType =>
      val res = __newVar(unit(0))(thenp.tp.asInstanceOf[TypeRep[Int]])
      __ifThenElse(cond, {
        __assign(res, inlineBlock(thenp))
      }, {
        __assign(res, inlineBlock(elsep))
      })
      ReadVar(res)(res.tp)
  }
  rewrite += rule {
    case or @ Boolean$bar$bar(case1, b) => {
      __ifThenElse(case1, unit(true), b)
    }
  }
  rewrite += rule {
    case and @ Boolean$amp$amp(case1, b) => {
      __ifThenElse(case1, b, unit(false))
    }
  }
  // rewrite += rule {
  //   case and @ Boolean$amp$amp(case1, b) if b.stmts.forall(stm => stm.rhs.isPure) && b.stmts.nonEmpty => {
  //     val rb = inlineBlock(b)
  //     case1 && rb
  //   }
  // }
  // rewrite += rule {
  //   case and @ Boolean$amp$amp(case1, b) if b.stmts.nonEmpty => {
  //     __ifThenElse(case1, b, unit(false))
  //   }
  // }
  rewrite += rule { case IntUnary_$minus(self) => unit(-1) * self }
  rewrite += rule { case IntToLong(x) => x }
  rewrite += rule { case ByteToInt(x) => x }
  rewrite += rule { case IntToDouble(x) => x }
  rewrite += rule { case DoubleToInt(x) => infix_asInstanceOf[Double](x) }
  rewrite += rule { case BooleanUnary_$bang(b) => NameAlias[Boolean](None, "!", List(List(b))) }
}

class BlockFlattening(override val IR: LoweringLegoBase) extends RecursiveRuleBasedTransformer[LoweringLegoBase](IR) with CTransformer {
  import IR._
  import CNodes._
  import CTypes._

  rewrite += statement {
    case sym -> (blk @ Block(stmts, res)) =>
      inlineBlock(blk)
  }
}
