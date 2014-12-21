package ch.epfl.data
package legobase
package optimization

import scala.language.existentials
import pardis.shallow.OptimalString
import scala.collection.mutable.ArrayBuffer
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
    // val b1 = new GenericEngineToCTransformer(context).optimize(b)
    // val b8 = new OptionToCTransformer(context).optimize(b1)
    // val b9 = new Tuple2ToCTransformer(context).optimize(b8)
    // val b2 = new ScalaScannerToCFileTransformer(context).optimize(b9)
    // val b3 = new ScalaArrayToCStructTransformer(context).optimize(b2)
    // val b4 = new ScalaCollectionsToGLibTransfomer(context).optimize(b3)
    // val b5 = new HashEqualsFuncsToCTraansformer(context).optimize(b4)
    // val b6 = new OptimalStringToCTransformer(context).optimize(b5)
    // val b7 = new RangeToCTransformer(context).optimize(b6)
    // val b10 = new ScalaConstructsToCTranformer(context).optimize(b7)
    // b10
    val pipeline = new TransformerPipeline()
    pipeline += new GenericEngineToCTransformer(context)
    pipeline += new OptionToCTransformer(context)
    pipeline += new Tuple2ToCTransformer(context)
    pipeline += new ScalaScannerToCFileTransformer(context)
    pipeline += new ScalaArrayToCStructTransformer(context)
    pipeline += new ScalaCollectionsToGLibTransfomer(context)
    pipeline += new HashEqualsFuncsToCTraansformer(context)
    pipeline += new OptimalStringToCTransformer(context)
    pipeline += new RangeToCTransformer(context)
    pipeline += new ScalaConstructsToCTranformer(context)
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
    case K2DBScannerNew(f) => NameAlias[FILE](None, "fopen", List(List(f, unit("r"))))
  }
  rewrite += rule {
    case K2DBScannerNext_int(s) =>
      val v = readVar(__newVar[Int](0))
      __ifThenElse(fscanf(s, unit("%d|"), &(v)) __== eof, break, unit(()))
      v
  }
  rewrite += rule {
    case K2DBScannerNext_double(s) =>
      val v = readVar(__newVar(unit(0.0)))
      __ifThenElse(fscanf(s, unit("%lf|"), &(v)) __== eof, break, unit)
      v
  }
  rewrite += rule {
    case K2DBScannerNext_char(s) =>
      val v = readVar(__newVar(unit('a')))
      __ifThenElse(fscanf(s, unit("%c|"), &(v)) __== eof, break, unit)
      v
  }
  rewrite += rule {
    case nn @ K2DBScannerNext1(s, buf) =>
      var i = __newVar[Int](0)
      __whileDo(unit(true), {
        val v = readVar(__newVar[Byte](unit('a')))
        __ifThenElse(fscanf(s, unit("%c"), &(v)) __== eof, break, unit)
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
      __ifThenElse(fscanf(s, unit("%d-%d-%d|"), &(x), &(y), &(z)) __== eof, break, unit)
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
      case ArrayType(args) => typePointer(typeCArray({
        if (args.isArray) typeCArray(args)
        else args
      }))
      case c if c.isRecord => tp.typeArguments match {
        case Nil     => typePointer(tp)
        case List(t) => typePointer(transformType(t))
      }
      case TreeSetType(args) => typePointer(typeGTree(transformType(args)))
      case SetType(args)     => typePointer(typeGList(transformType(args)))
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
        val elemType = a.tp.typeArguments(0)
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
      if (elemType.isPrimitive) ArrayUpdate(arr.asInstanceOf[Expression[Array[Any]]], i, apply(v))
      else if (elemType.name == "OptimalString")
        PTRASSIGN(arr.asInstanceOf[Expression[Pointer[Any]]], i, apply(v))
      else if (elemType.isRecord) {
        class T
        implicit val typeT = apply(v).tp.typeArguments(0).asInstanceOf[TypeRep[T]]
        val newV = apply(v).asInstanceOf[Expression[Pointer[T]]]
        val vContent = *(newV)
        val tArr = arr.asInstanceOf[Expression[Pointer[T]]]
        pointer_assign(tArr, i, vContent)
        val pointerVar = newV match {
          case Def(ReadVar(v)) => v.asInstanceOf[Var[Pointer[T]]]
          case x               => Var(x.asInstanceOf[Rep[Var[Pointer[T]]]])
        }
        Assign(pointerVar, (&(tArr, i)).asInstanceOf[Rep[Pointer[T]]])(PointerType(typeT))
      } else
        PTRASSIGN(arr.asInstanceOf[Expression[Pointer[Any]]], i, *(apply(v).asInstanceOf[Expression[Pointer[Any]]])(v.tp.name match {
          case x if v.tp.isArray            => transformType(v.tp).typeArguments(0).asInstanceOf[PardisType[Any]]
          case x if x.startsWith("Pointer") => v.tp.typeArguments(0).asInstanceOf[PardisType[Any]]
          case _                            => v.tp
        }))
  }
  rewrite += rule {
    case ArrayFilter(a, op) => field(a, "array")(transformType(a.tp)).correspondingNode
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
      if (elemType.isPrimitive) ArrayApply(arr.asInstanceOf[Expression[Array[Any]]], i)(newTp.asInstanceOf[PardisType[Any]])
      else PTRADDRESS(arr.asInstanceOf[Expression[Pointer[Any]]], i)(typePointer(newTp).asInstanceOf[PardisType[Pointer[Any]]])
  }
  rewrite += rule {
    case ArrayLength(a) =>
      val s = apply(a)
      val arr = field(s, "length")(IntType)
      arr
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

class ScalaCollectionsToGLibTransfomer(override val IR: LoweringLegoBase) extends RuleBasedTransformer[LoweringLegoBase](IR) {
  import IR._
  import CNodes._
  import CTypes._

  override def transformType[T: PardisType]: PardisType[Any] = ({
    val tp = typeRep[T]
    tp match {
      case ArrayBufferType(t)  => typePointer(typeGArray(transformType(t)))
      case SeqType(t)          => typePointer(typeGList(transformType(t)))
      case TreeSetType(t)      => typePointer(typeGTree(transformType(t)))
      case SetType(t)          => typePointer(typeGList(transformType(t)))
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
    case HashMapSize(map) => NameAlias[Int](None, "g_hash_table_size", List(List(map)))
  }
  rewrite += rule {
    case HashMapKeySet(map) =>
      NameAlias[Pointer[GList[FILE]]](None, "g_hash_table_get_keys", List(List(map)))
  }
  rewrite += rule {
    case HashMapContains(map, key) =>
      val z = toAtom(transformDef(HashMapApply(map, key)))(getMapValueType(apply(map)))
      infix_!=(z, unit(null))
  }
  rewrite += rule {
    case ma @ HashMapApply(map, key) =>
      NameAlias(None, "g_hash_table_lookup", List(List(map, apply(key))))(getMapValueType(apply(map)))
  }
  rewrite += rule {
    case mu @ HashMapUpdate(map, key, value) =>
      NameAlias[Unit](None, "g_hash_table_insert", List(List(apply(map), apply(key), apply(value))))
  }
  rewrite += rule {
    case hmgu @ HashMapGetOrElseUpdate(map, key, value) =>
      val ktp = getMapKeyType(apply(map))
      val vtp = getMapValueType(apply(map))
      val v = toAtom(transformDef(HashMapApply(map, key)(ktp, vtp))(vtp))(vtp)
      __ifThenElse(infix_==(v, unit(null)), {
        val res = inlineBlock(apply(value))
        toAtom(apply(HashMapUpdate(map, key, res)))
        res
      }, v)(v.tp)
  }
  rewrite += rule {
    case mr @ HashMapRemove(map, key) =>
      val x = toAtom(transformDef(HashMapApply(map, apply(key))))(getMapValueType(apply(map)))
      nameAlias[Unit](None, "g_hash_table_remove", List(List(map, apply(key))))
      x
  }

  /* Set Operaions */
  rewrite += rule { case SetApplyObject1(s) => s }
  rewrite += rule {
    case nm @ SetApplyObject2() => PardisCast(unit(0).asInstanceOf[Expression[Any]])(apply(nm.tp), apply(nm.tp))
  }
  rewrite += rule {
    case SetHead(s) =>
      val x = nameAlias(None, "g_list_first", List(List(s)))(transformType(s.tp))
      field(x, "data")(apply(s).tp.typeArguments(0).typeArguments(0))
  }
  rewrite += rule {
    case SetRemove(s @ Def(PardisReadVar(x)), e) =>
      val newHead = nameAlias(None, "g_list_remove", List(List(s, apply(e))))(transformType(s.tp))
      PardisAssign(x, newHead)
    case SetRemove(s, e) => throw new Exception("Implementation Limitation: Sets should always" +
      " be Vars when using Scala collections to GLib transformer!")
  }
  rewrite += rule { case SetToSeq(set) => set }

  /* TreeSet Operations */
  rewrite += remove { case OrderingNew(o) => () }

  rewrite += rule {
    case ts @ TreeSetNew2(Def(OrderingNew(Def(Lambda2(f, i1, i2, o))))) =>
      NameAlias(None, "g_tree_new", List(List(Lambda2(f, i2, i1, transformBlock(o)))))(transformType(ts.tp))
  }
  rewrite += rule {
    case TreeSet$plus$eq(t, s) => NameAlias[Unit](None, "g_tree_insert", List(List(t, s, s)))
  }
  rewrite += rule {
    case TreeSet$minus$eq(self, t) => NameAlias[Boolean](None, "g_tree_remove", List(List(self, t)))
  }
  rewrite += rule {
    case TreeSetSize(t) => NameAlias[Int](None, "g_tree_nnodes", List(List(t)))
  }
  rewrite += rule {
    case op @ TreeSetHead(t) =>
      def treeHead[A: PardisType, B: PardisType] = doLambda3((s1: Rep[A], s2: Rep[A], s3: Rep[Pointer[B]]) => {
        pointer_assign(s3.asInstanceOf[Expression[Pointer[Any]]], s2)
        unit(0)
      })
      val elemType = t.tp.typeArguments(0).typeArguments(0)
      val init = infix_asInstanceOf(Constant(null))(elemType)
      nameAlias[Unit](None, "g_tree_foreach", List(List(t, treeHead(elemType, elemType), &(init)(elemType.asInstanceOf[PardisType[Any]]))))
      ReadVal(init)(init.tp)
  }

  /* ArrayBuffer Operations */
  rewrite += rule {
    case abn @ ArrayBufferNew2() =>
      val sz = sizeof()(abn.tp.typeArguments(0))
      NameAlias(None, "g_array_new", List(List(unit(null), unit(true), sz)))(apply(abn.tp))
  }
  rewrite += rule {
    case abn @ ArrayBufferNew3() =>
      val sz = sizeof()(abn.tp.typeArguments(0))
      NameAlias(None, "g_array_new", List(List(unit(null), unit(true), sz)))(apply(abn.tp))
  }
  rewrite += rule {
    case aba @ ArrayBufferApply(a, i) =>
      val tp = typeOf()({
        if (aba.tp.isPrimitive) aba.tp else typePointer(aba.tp)
      })
      NameAlias(None, "g_array_index", List(List(a, tp, i)))(apply(aba.tp))
  }
  rewrite += rule {
    case ArrayBufferAppend(a, e) =>
      NameAlias[Unit](None, "g_array_append_val", List(List(apply(a), e)))
  }
  rewrite += rule {
    case ArrayBufferSize(a) =>
      PardisStructFieldGetter(a, "len")(IntType)
  }
  rewrite += rule {
    case ArrayBufferRemove(a, e) =>
      NameAlias[Unit](None, "g_array_remove_index", List(List(a, e)))
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

  rewrite += rule {
    case Equals(e1, e2, isEqual) if e1.tp == OptimalStringType || e1.tp == StringType =>
      if (isEqual) !strcmp(e1, e2) else strcmp(e1, e2)
    case Equals(e1, Constant(null), isEqual) if __isRecord(e1) =>
      val structDef = if (e1.tp.isRecord)
        getStructDef(e1.tp).get
      else
        getStructDef(e1.tp.typeArguments(0)).get
      val firstField = structDef.fields.find(f => f.tpe.isPointerType).get
      val fieldExp = field(e1, firstField.name)(firstField.tpe)
      if (isEqual)
        fieldExp __== unit(null)
      else
        fieldExp __!= unit(null)
    // case Equals(e1, e2, isEqual) if __isRecord(e1) && __isRecord(e2) =>
    //   class T
    //   implicit val ttp = (if (e1.tp.isRecord) e1.tp else e1.tp.typeArguments(0)).asInstanceOf[TypeRep[T]]
    //   val eq = getStructEqualsFunc[T]()
    //   val res = inlineFunction(eq, e1.asInstanceOf[Rep[T]], e2.asInstanceOf[Rep[T]])
    //   if (isEqual) res else !res
    case Equals(e1, e2, isEqual) if __isRecord(e1) && __isRecord(e2) =>
      val ttp = (if (e1.tp.isRecord) e1.tp else e1.tp.typeArguments(0))
      val structDef = getStructDef(ttp).get
      val res = structDef.fields.map(f => field(e1, f.name)(f.tpe) __== field(e2, f.name)(f.tpe)).reduce(_ && _)
      // val eq = getStructEqualsFunc[T]()
      // val res = inlineFunction(eq, e1.asInstanceOf[Rep[T]], e2.asInstanceOf[Rep[T]])
      if (isEqual) res else !res
  }
  rewrite += rule {
    case HashCode(t) if t.tp == StringType => unit(0) // KEY is constant. No need to hash anything
    case HashCode(t) if __isRecord(t) =>
      val tp = t.tp.asInstanceOf[PardisType[Any]]
      val hashFunc = {
        if (t.tp.isRecord) getStructHashFunc[Any]()(tp)
        else getStructHashFunc[Any]()(tp.typeArguments(0).asInstanceOf[TypeRep[Any]])
      }
      val hf = toAtom(hashFunc)(hashFunc.tp)
      inlineFunction(hf.asInstanceOf[Rep[Any => Int]], apply(t))
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
      case PointerType(tp) if tp.isRecord => {
        Apply(getStructToStringFunc(tp), obj)
      }
      case tp => throw new Exception(s"toString conversion for non-record type $tp is not handled for the moment")
    }
  }
}

class OptimalStringToCTransformer(override val IR: LoweringLegoBase) extends RecursiveRuleBasedTransformer[LoweringLegoBase](IR) with CTransformer {
  import IR._
  import CNodes._
  import CTypes._

  rewrite += rule { case OptimalStringNew(x) => x }
  rewrite += rule { case OptimalStringString(x) => x }
  rewrite += rule { case OptimalStringDiff(x, y) => strcmp(x, y) }
  rewrite += rule {
    case OptimalStringEndsWith(x, y) =>
      val lenx = strlen(x)
      val leny = strlen(y)
      val len = lenx - leny
      strncmp(x + len, y, len) __== unit(0)
  }
  rewrite += rule {
    case OptimalStringStartsWith(x, y) =>
      strncmp(x, y, strlen(y)) __== unit(0)
  }
  rewrite += rule { case OptimalStringCompare(x, y) => strcmp(x, y) }
  rewrite += rule { case OptimalStringLength(x) => strlen(x) }
  rewrite += rule { case OptimalString$eq$eq$eq(x, y) => strcmp(x, y) __== unit(0) }
  rewrite += rule { case OptimalString$eq$bang$eq(x, y) => infix_!=(strcmp(x, y), unit(0)) }
  rewrite += rule { case OptimalStringContainsSlice(x, y) => infix_!=(strstr(x, y), unit(null)) }
  rewrite += rule {
    case OptimalStringIndexOfSlice(x, y, idx) =>
      val substr = strstr(x + idx, y)
      __ifThenElse(substr __== unit(null), unit(-1), str_subtract(substr, x))
  }
  rewrite += rule {
    case OptimalStringApply(x, idx) =>
      val stringArray = infix_asInstanceOf(x)(typeArray(typePointer(CharType))).asInstanceOf[Rep[Array[Pointer[Char]]]]
      stringArray(idx)
  }
  rewrite += rule {
    case OptimalStringSlice(x, start, end) =>
      val len = end - start + unit(1)
      val newbuf = malloc(len)(CharType)
      strncpy(newbuf, x + start, len - unit(1))
      newbuf
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

  rewrite += remove { case Tuple2ApplyObject(_1, _2) => () }

  rewrite += rule { case n @ Tuple2_Field__1(Def(Tuple2ApplyObject(_1, _2))) => _1 }

  rewrite += rule { case n @ Tuple2_Field__2(Def(Tuple2ApplyObject(_1, _2))) => _2 }

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
  rewrite += rule { case IntUnary_$minus(self) => unit(-1) * self }
  rewrite += rule { case IntToLong(x) => x }
  rewrite += rule { case ByteToInt(x) => x }
  rewrite += rule { case IntToDouble(x) => x }
  rewrite += rule { case DoubleToInt(x) => infix_asInstanceOf[Double](x) }
  rewrite += rule { case BooleanUnary_$bang(b) => NameAlias[Boolean](None, "!", List(List(b))) }
}
