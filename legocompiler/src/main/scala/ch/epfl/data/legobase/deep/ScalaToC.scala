package ch.epfl.data
package legobase
package deep

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

  override def transformExp[T: TypeRep, S: TypeRep](exp: Rep[T]): Rep[S] = exp match {
    case t: typeOf[_] => typeOf()(apply(t.tp)).asInstanceOf[Rep[S]]
    case _            => super.transformExp[T, S](exp)
  }
}

trait ScalaToC extends DeepDSL with K2DBScannerOps with CFunctions { this: Base =>
  import CNodes._
  override def structName[T](m: PardisType[T]): String = {
    if (m.name.startsWith("CArray")) "ArrayOf" + structName(m.typeArguments.last)
    else if (m.isArray) structName(m.typeArguments.last)
    else if (m.name.startsWith("Pointer")) structName(m.typeArguments.last)
    else if (m.name == "Int") "int"
    else if (m.name == "Byte") "char"
    else if (m.name == "Char") "char"
    else if (m.name == "Double") "double"
    else if (m.name == "OptimalString") "OptimalString"
    //else if (m.name.contains("Record")) m.name.replaceAll("struct ", "")
    else if (m.isRecord) m.name.replaceAll("struct ", "")
    else {
      System.out.println("WARNING: Default structName given: " + m.name);
      super.structName(m)
    }
  }
}

object CTransformersPipeline extends TransformerHandler {
  def apply[Lang <: Base, T: PardisType](context: Lang)(block: context.Block[T]): context.Block[T] = {
    apply[T](context.asInstanceOf[LoweringLegoBase], block)
  }
  def apply[A: PardisType](context: LoweringLegoBase, b: PardisBlock[A]) = {
    val b0 = new GenericEngineToCTransformer(context).transformBlock(b)
    val b1 = new ScalaScannerToCFileTransformer(context).transformBlock(b0)
    val b2 = new ScalaArrayToCStructTransformer(context).optimize(b1)
    val b3 = new ScalaCollectionsToGLibTransfomer(context).optimize(b2)
    val b4 = new OptimalStringToCTransformer(context).transformBlock(b3)
    val b5 = new ScalaConstructsToCTranformer(context).transformBlock(b4)
    b5
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
      buf(i) = unit('\0')
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

class ScalaArrayToCStructTransformer(override val IR: LoweringLegoBase) extends Optimizer[LoweringLegoBase](IR) {
  import IR._
  import CNodes._
  import CTypes._

  override def transformType[T: PardisType]: PardisType[Any] = ({
    val tp = typeRep[T]
    if (tp.isPrimitive) super.transformType[T]
    else if (tp.name.startsWith("ArrayBuffer")) typePointer(typeGArray(transformType(tp.typeArguments(0))))
    else if (tp.isArray) typePointer(typeCArray({
      val ttp = tp.typeArguments(0)
      if (ttp.isArray) typeCArray(ttp.typeArguments(0))
      else ttp
    }))
    else if (tp.isRecord) {
      if (tp.typeArguments == List()) typePointer(tp)
      else typePointer(transformType(tp.typeArguments(0)))
    } else if (tp.name.contains("TreeSet")) typePointer(typeGTree(transformType(tp.typeArguments(0))))
    else if (tp.name.contains("Set")) typePointer(typeGList(transformType(tp.typeArguments(0))))
    else if (tp.name.startsWith("HashMap")) typePointer(typeGHashTable(transformType(tp.typeArguments(0)), transformType(tp.typeArguments(1))))
    else if (tp.name.contains("Option")) typePointer(transformType(tp.typeArguments(0)))
    else {
      //      System.out.println("WARNING: Default transformType called: " + tp)
      super.transformType[T]
    }
  }).asInstanceOf[PardisType[Any]]

  def optimize[T: TypeRep](node: Block[T]): to.Block[T] = {
    traverseBlock(node)
    transformProgram(node)
  }

  override def transformStmToMultiple(stm: Stm[_]): List[to.Stm[_]] = stm match {
    case Stm(s, RangeNew(start, end, step)) => Nil
    case _                                  => super.transformStmToMultiple(stm)
  }

  //val singletonArrays = scala.collection.mutable.Map[Sym[Any], TypeRep[Any]]()
  val singletonArrays = scala.collection.mutable.Map[TypeRep[Any], TypeRep[Any]]()
  def isSingletonArray[T](a: Expression[T]): Boolean = singletonArrays.contains(a.tp.asInstanceOf[TypeRep[Any]])

  override def traverseStm(stm: Stm[_]): Unit = stm match {
    // Hack/Patch -- AGGRecord_Field_Aggs should be PardisStructImmutableField after LBLowering,
    // but it is not. For now this seems to be working
    case Stm(sym, AGGRecord_Field_Aggs(s)) =>
      if (sym.tp.isArray && singletonArrays.contains(s.tp.asInstanceOf[TypeRep[Any]])) {
        singletonArrays(sym.tp.asInstanceOf[TypeRep[Any]]) = sym.tp.typeArguments(0).asInstanceOf[PardisType[Any]]
      }
      super.traverseStm(stm)

    case Stm(sym, PardisStructImmutableField(s, f)) => {
      if (sym.tp.isArray && singletonArrays.contains(s.tp.asInstanceOf[TypeRep[Any]])) {
        singletonArrays(sym.tp.asInstanceOf[TypeRep[Any]]) = sym.tp.typeArguments(0).asInstanceOf[PardisType[Any]]
      }
      super.traverseStm(stm)
    }
    case Stm(sym, ps @ PardisStruct(tag, elems, methods)) => {
      elems.foreach(e =>
        if (e.init.tp.isArray && singletonArrays.contains(e.init.tp.asInstanceOf[TypeRep[Any]])) {
          singletonArrays(sym.tp.asInstanceOf[TypeRep[Any]]) = typePointer(sym.tp).asInstanceOf[PardisType[Any]]
        })
    }
    case Stm(sym, an @ ArrayNew(Constant(1))) =>
      singletonArrays(sym.tp.asInstanceOf[TypeRep[Any]]) = sym.tp.typeArguments(0).asInstanceOf[PardisType[Any]]
    case _ => super.traverseStm(stm)
  }

  override def transformStm(stm: Stm[_]): Stm[_] = stm match {
    case Stm(sym, an @ ArrayNew(Constant(1))) =>
      val dfltV = pardis.shallow.utils.DefaultValue(an.typeT.name)
      if (dfltV == null)
        super.transformStm(Stm(sym, ReadVal(unit(dfltV))))
      else
        super.transformStm(Stm(sym, ReadVal(unit(0))))
    case Stm(sym, ps @ PardisStruct(tag, elems, methods)) => {
      val newElems = elems.map(e =>
        if (e.init.tp.isArray && singletonArrays.contains(e.init.tp.asInstanceOf[TypeRep[Any]])) {
          PardisStructArg(e.name, true, e.init)
        } else e)
      super.transformStm(Stm(sym, PardisStruct(tag, newElems, methods)(ps.tp))(sym.tp))
    }
    case _ => super.transformStm(stm)
  }

  override def newSym[T: TypeRep](sym: Rep[T]): to.Sym[_] = {
    if (singletonArrays.contains(sym.tp.asInstanceOf[TypeRep[Any]])) to.fresh(singletonArrays(sym.tp.asInstanceOf[TypeRep[Any]])).copyFrom(sym.asInstanceOf[Sym[T]])
    else super.newSym[T](sym)
  }

  override def transformDef[T: TypeRep](node: Def[T]): to.Def[T] = (node match {
    case RangeForeach(Def(RangeNew(start, end, step)), Def(Lambda(f, i1, o))) =>
      PardisFor(start, end, step, i1.asInstanceOf[Expression[Int]], reifyBlock({ o }).asInstanceOf[PardisBlock[Unit]])

    case pc @ PardisCast(x) => PardisCast(apply(x))(apply(pc.castFrom), apply(pc.castTp))

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
        ReadVal(m.asInstanceOf[Expression[Any]])(m.tp.asInstanceOf[PardisType[Any]])
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
        ReadVal(m.asInstanceOf[Expression[Any]])(m.tp.asInstanceOf[PardisType[Any]])
      }
    case au @ ArrayUpdate(a, i, v) => {
      if (singletonArrays.contains(a.tp.asInstanceOf[PardisType[Any]])) {
        a match {
          case Def(moo @ PardisStructImmutableField(s, f)) => PardisStructFieldSetter(s, f, v)
        }
      } else {
        val s = transformExp[Any, T](a)
        // Get type of elements stored in array
        val elemType = a.tp.typeArguments(0)
        // Get type of internal array
        val newTp = ({
          if (elemType.isArray) typePointer(typeCArray(elemType.typeArguments(0)))
          else typeArray(typePointer(elemType))
        }).asInstanceOf[PardisType[Any]] //if (elemType.isPrimitive) elemType else typePointer(elemType)
        // Read array and perform update
        val arr = field(s, "array")(newTp)
        if (elemType.isPrimitive) ArrayUpdate(arr.asInstanceOf[Expression[Array[Any]]], i, v)
        else if (elemType.name == "OptimalString")
          PTRASSIGN(arr.asInstanceOf[Expression[Pointer[Any]]], i, v)
        else
          PTRASSIGN(arr.asInstanceOf[Expression[Pointer[Any]]], i, *(v.asInstanceOf[Expression[Pointer[Any]]])(v.tp.name match {
            case x if v.tp.isArray            => transformType(v.tp).typeArguments(0).asInstanceOf[PardisType[Any]]
            case x if x.startsWith("Pointer") => v.tp.typeArguments(0).asInstanceOf[PardisType[Any]]
            case _                            => v.tp
          }))
      }
    }
    case ArrayFilter(a, op) => field(a, "array")(transformType(a.tp)).correspondingNode
    case ArrayApply(a, i) =>
      if (singletonArrays.contains(a.tp.asInstanceOf[PardisType[Any]])) ReadVal(a)
      else {
        val s = transformExp[Any, T](a)
        // Get type of elements stored in array
        val elemType = a.tp.typeArguments(0)
        // Get type of internal array
        val newTp = ({
          if (elemType.isArray) typePointer(typeCArray(elemType.typeArguments(0)))
          else typeArray(typePointer(elemType))
        }).asInstanceOf[PardisType[Any]] // if (elemType.isPrimitive) elemType else typePointer(elemType)
        val arr = field(s, "array")(newTp)
        if (elemType.isPrimitive) ArrayApply(arr.asInstanceOf[Expression[Array[Any]]], i)(newTp.asInstanceOf[PardisType[Any]])
        else PTRADDRESS(arr.asInstanceOf[Expression[Pointer[Any]]], i)(typePointer(newTp).asInstanceOf[PardisType[Pointer[Any]]])
      }
    case ArrayLength(a) =>
      val s = transformExp[Any, T](a)
      val arr = field(s, "length")(IntType)
      ReadVal(arr)(IntType)
    case s @ PardisStruct(tag, elems, methods) =>
      // TODO if needed method generation should be added
      val x = toAtom(Malloc(unit(1))(s.tp))(typePointer(s.tp))
      val newElems = elems.map(el => PardisStructArg(el.name, el.mutable, transformExp(el.init)(el.init.tp, apply(el.init.tp))))
      structCopy(x, PardisStruct(tag, newElems, methods.map(m => m.copy(body =
        transformDef(m.body.asInstanceOf[Def[Any]]).asInstanceOf[PardisLambdaDef])))(s.tp))
      ReadVal(x)(typePointer(s.tp))
    case _ => super.transformDef(node)
  }).asInstanceOf[to.Def[T]]
}

class ScalaCollectionsToGLibTransfomer(override val IR: LoweringLegoBase) extends Optimizer[LoweringLegoBase](IR) with StructCollector[LoweringLegoBase] {
  import IR._
  import CNodes._
  import CTypes._

  def optimize[T: TypeRep](node: Block[T]): to.Block[T] = {
    traverseBlock(node)
    transformProgram(node)
  }

  override def transformType[T: PardisType]: PardisType[Any] = ({
    val tp = typeRep[T]
    if (tp.name.contains("ArrayBuffer")) typePointer(typeGArray(transformType(tp.typeArguments(0))))
    else if (tp.name.startsWith("CArray")) tp
    else if (tp.name.contains("Seq")) typePointer(typeGList(transformType(tp.typeArguments(0))))
    else if (tp.name.contains("TreeSet")) typePointer(typeGTree(transformType(tp.typeArguments(0))))
    else if (tp.name.contains("Set")) typePointer(typeGList(transformType(tp.typeArguments(0))))
    else if (tp.name.contains("Option")) typePointer(transformType(tp.typeArguments(0)))
    else super.transformType[T]
  }).asInstanceOf[PardisType[Any]]

  override def transformStmToMultiple(stm: Stm[_]): List[to.Stm[_]] = stm match {
    case Stm(s, OrderingNew(o)) => Nil
    case _                      => super.transformStmToMultiple(stm)
  }

  // Set of hash and record functions
  def string_hash = doLambdaDef((s: Rep[String]) => infix_hashCode(s))
  def string_eq = doLambda2Def((s1: Rep[String], s2: Rep[String]) => infix_==(strcmp(s1, s2), 0))
  def int_hash = doLambdaDef((s: Rep[Int]) => s)
  def int_eq = doLambda2Def((s1: Rep[Int], s2: Rep[Int]) => infix_==(s1, s2))
  def double_hash = doLambdaDef((s: Rep[Double]) => s / 10)
  def double_eq = doLambda2Def((s1: Rep[Double], s2: Rep[Double]) => infix_==(s1, s2))
  // ------------------------------
  def treeHead[A: PardisType, B: PardisType] = doLambda3((s1: Rep[A], s2: Rep[A], s3: Rep[Pointer[B]]) => {
    pointer_assign(s3.asInstanceOf[Expression[Pointer[Any]]], s2)
    unit(0)
  })

  object Equals {
    def unapply(node: Def[Boolean]): Option[(Rep[Any], Rep[Any], Boolean)] = node match {
      case Equal(a, b)    => Some((a, b, true))
      case NotEqual(a, b) => Some((a, b, false))
      case _              => None
    }
  }

  def __isRecord(e: Expression[Any]) = e.tp.isRecord || (e.tp.name.startsWith("Pointer") && e.tp.typeArguments(0).isRecord)
  override def transformDef[T: PardisType](node: Def[T]): to.Def[T] = (node match {
    /* HashMap Operations */
    case nm @ HashMapNew3(_, _) => transformDef(HashMapNew()(nm.typeA, ArrayBufferType(nm.typeB)))
    case nm @ HashMapNew4(_, _) => transformDef(HashMapNew()(nm.typeA, nm.typeB))
    case nm @ HashMapNew() =>
      val nA = typePointer(transformType(nm.typeA)).asInstanceOf[TypeRep[Any]]
      val nB = typePointer(transformType(nm.typeB))

      if ((nm.typeA == StringType) || (nm.typeA == OptimalStringType))
        GHashTableNew(transformDef(string_hash), transformDef(string_eq))(StringType, nB, IntType)
      else if (nm.typeA == IntType)
        GHashTableNew(transformDef(int_hash), transformDef(int_eq))(IntType, nB, IntType)
      else if (nm.typeA == DoubleType)
        GHashTableNew(transformDef(double_hash), transformDef(double_eq))(DoubleType, nB, DoubleType)
      else GHashTableNew(transformDef(getStructHashFunc()(nm.typeA)), transformDef(getStructEqualsFunc()(nm.typeA)))(nA, nB, IntType)
    case HashMapSize(map)   => NameAlias[Int](None, "g_hash_table_size", List(List(map)))
    case HashMapKeySet(map) => NameAlias[Pointer[GList[FILE]]](None, "g_hash_table_get_keys", List(List(map)))
    case HashMapContains(map, key) =>
      val e = toAtom(transformDef(HashMapApply(map, key)))(transformType(key.tp))
      ReadVal(infix_!=(e, Constant(null)))
    case ma @ HashMapApply(map, key) =>
      NameAlias(None, "g_hash_table_lookup", List(List(map, key)))(ma.tp)
    case mu @ HashMapUpdate(map, key, value) => NameAlias[Unit](None, "g_hash_table_insert", List(List(map, key, value)))
    case hmgu @ HashMapGetOrElseUpdate(map, key, value) =>
      val v = transformDef(HashMapApply(map, key))
      val res = __ifThenElse(infix_==(toAtom(v), Constant(null)), {
        val newB = transformBlockTyped(value)(typeRep[T], transformType(value.tp))
        toAtom(newB)(newB.tp)
        val res = newB.res
        toAtom(transformDef(HashMapUpdate(map.asInstanceOf[Expression[HashMap[Any, Any]]], key, res)))
        res
      }, toAtom(v))(v.tp.asInstanceOf[PardisType[Any]])
      ReadVal(res)(res.tp)
    case mr @ HashMapRemove(map, key) =>
      val x = toAtom(transformDef(HashMapApply(map, key)))(transformType(map.tp.typeArguments(0).typeArguments(1)))
      toAtom(NameAlias[Unit](None, "g_hash_table_remove", List(List(map, key)))(UnitType))
      ReadVal(x)(transformType(map.tp.typeArguments(0).typeArguments(1)))

    /* Set Operaions */
    case nm @ SetNew(s) => ReadVal(transformExp[Any, T](s))(transformType(s.tp).asInstanceOf[PardisType[T]])
    case nm @ SetNew2() =>
      val newType = transformType(nm.tp)
      PardisCast(Constant(0.asInstanceOf[Any]))(newType, newType)
    case sh @ SetHead(s) =>
      val x = toAtom(NameAlias(None, "g_list_first", List(List(s)))(transformType(s.tp)))(transformType(s.tp))
      val f = field(x, "data")(s.tp.typeArguments(0).typeArguments(0))
      ReadVal(f)(f.tp)
    case SetRemove(s, e) =>
      s.correspondingNode match {
        case PardisReadVar(x) =>
          val newHead = nameAlias(None, "g_list_remove", List(List(s, e)))(transformType(s.tp))
          PardisAssign(x, newHead)
        case _ => throw new Exception("SET NOT A VAR, GO CHANGE IT TO A VAR!")
      }
    case SetToSeq(set) => ReadVal(set)(transformType(set.tp).asInstanceOf[PardisType[Set[Any]]])

    /* TreeSet Operations */
    case ts @ TreeSetNew2(Def(OrderingNew(Def(Lambda2(f, i1, i2, o))))) =>
      NameAlias(None, "g_tree_new", List(List(Lambda2(f, i2, i1, transformBlock(o)))))(transformType(ts.tp))
    case tsa @ TreeSet$plus$eq(t, s)    => NameAlias[Unit](None, "g_tree_insert", List(List(t, s, s)))
    case op @ TreeSet$minus$eq(self, t) => NameAlias[Boolean](None, "g_tree_remove", List(List(self, t)))
    case op @ TreeSetSize(t)            => NameAlias[Int](None, "g_tree_nnodes", List(List(t)))
    case op @ TreeSetHead(t) =>
      val elemType = t.tp.typeArguments(0).typeArguments(0)
      val init = infix_asInstanceOf(Constant(null))(elemType)
      toAtom(NameAlias[Unit](None, "g_tree_foreach", List(List(t, treeHead(elemType, elemType), &(init)(elemType.asInstanceOf[PardisType[Any]])))))
      ReadVal(init)(init.tp)

    /* ArrayBuffer Operations */
    case abn @ ArrayBufferNew2() =>
      val x = sizeof()(abn.tp.typeArguments(0))
      NameAlias(None, "g_array_new", List(List(Constant(null), Constant(true), x)))(typeGArray(abn.tp.typeArguments(0)))
    case abn @ ArrayBufferNew3() =>
      val x = sizeof()(abn.tp.typeArguments(0))
      NameAlias(None, "g_array_new", List(List(Constant(null), Constant(true), x)))(typeGArray(abn.tp.typeArguments(0)))
    case aba @ ArrayBufferApply(a, i) =>
      if ((aba.tp.isPrimitive) || (aba.name.contains("DynamicCompositeRecord"))) NameAlias(None, "g_array_index", List(List(a, typeOf()(aba.tp), i)))(transformType(aba.tp))
      else NameAlias(None, "g_array_index", List(List(a, typeOf()(typePointer(aba.tp)), i)))(transformType(aba.tp))
    case abap @ ArrayBufferAppend(a, e) =>
      NameAlias[Unit](None, "g_array_append_val", List(List(a, e)))
    case ArrayBufferSize(a) => PardisStructFieldGetter(a, "len")(IntType)
    case ArrayBufferMinBy(a, f @ Def(Lambda(fun, input, o))) =>
      val len = transformDef(ArrayBufferSize(a))
      var i = __newVar[Int](1)
      var min = __newVar(transformDef(ArrayBufferApply(a, (Constant(0)))))
      var minBy = __newVar(fun(min).asInstanceOf[Expression[Int]])
      __whileDo(readVar(i) < len, {
        val e = transformDef(ArrayBufferApply(a, readVar(i)))
        val eminBy = fun(e).asInstanceOf[Expression[Int]]
        __ifThenElse(eminBy < readVar(minBy), {
          __assign(min, e)
          __assign(minBy, eminBy)
        }, unit())
        __assign(i, readVar(i) + unit(1))
      })
      ReadVar(min)
    case ArrayBufferFoldLeft(a, cnt, Def(Lambda2(fun, input1, input2, o))) =>
      var idx = __newVar[Int](0)
      val len = transformDef(ArrayBufferSize(a))
      var agg = __newVar(cnt)
      __whileDo(readVar(idx) < len, {
        val e = transformDef(ArrayBufferApply(a.asInstanceOf[Expression[scala.collection.mutable.ArrayBuffer[Any]]], readVar(idx))(a.tp.typeArguments(0).typeArguments(0).asInstanceOf[PardisType[Any]]))
        val z = fun(agg, toAtom(e)(a.tp.typeArguments(0).typeArguments(0).asInstanceOf[PardisType[Any]]))
        __assign(agg, z)
        __assign(idx, readVar(idx) + unit(1))
      })
      ReadVar(agg)
    case ArrayBufferRemove(a, e) => NameAlias[Unit](None, "g_array_remove_index", List(List(a, e)))

    /* Other operations */
    case imtf @ PardisStructImmutableField(s, f) =>
      PardisStructImmutableField(apply(s), f)(transformType(imtf.tp))

    // Handling proper hascode and equals
    case Equals(e1, e2, isEqual) => e1.tp match {
      case x if x == OptimalStringType =>
        if (isEqual) transformDef(BooleanUnary_$bang(strcmp(e1, e2)))
        else ReadVal(strcmp(e1, e2))
      case x if __isRecord(e1) && __isRecord(e2) =>
        val ttp = if (x.isRecord) x else x.typeArguments(0)
        val __eq = transformDef(getStructEqualsFunc[Any]()(ttp.asInstanceOf[PardisType[Any]]))
        val eq = toAtom(__eq)(__eq.tp) //(typeRep[(ttp, ttp) => Boolean])
        // TODO should be moved to pardis as it has a lot of use cases
        // val z = eq match {
        //   case Def(PardisLambda2(_, i1, i2, o)) => {
        //     System.out.println(s"$o, $e1, ${apply(e1)}")
        //     substitutationContext(i1 -> e1, i2 -> e2) {
        //       inlineBlock(o)
        //     }
        //   }
        // }
        // Predef.println(s"subst map ${subst.mkString("\n")}")
        val z = inlineFunction(eq.asInstanceOf[Rep[(Any, Any) => Boolean]], apply(e1), apply(e2))
        ReadVal(if (isEqual) z else transformDef(BooleanUnary_$bang(z)))
      // case _ => { /*System.out.println("EQUAL DEFAULT " + e1.tp + "/" + e2.tp); */ node }
      case _ => super.transformDef(node)
    }
    case HashCode(t) =>
      t.tp match {
        case x if x == StringType => ReadVal(unit(0)) // KEY is constant. No need to hash anything
        case x if __isRecord(t) =>
          val __h = transformDef({
            if (x.isRecord) getStructHashFunc[Any]()(x.asInstanceOf[PardisType[Any]])
            else getStructHashFunc[Any]()(x.typeArguments(0).asInstanceOf[PardisType[Any]])
          })
          val h = toAtom(transformDef(__h))(__h.tp)
          // TODO should be moved to pardis as it has a lot of use cases
          val z = h match {
            case Def(PardisLambda(_, i, o)) => {
              subst += i -> t
              for (stm <- o.stmts) {
                transformStmToMultiple(stm)
              }
              apply(o.res)
            }
          }
          //val z = inlineFunction(h.asInstanceOf[Rep[Any => Int]], apply(t))
          ReadVal(z)

        case x if x.isPrimitive      => PardisCast(t)(x, IntType)
        case x if x == CharacterType => PardisCast(t)(x, IntType)
        case x if x == OptimalStringType =>
          val len = toAtom(OptimalStringLength(t.asInstanceOf[Expression[OptimalString]]))(IntType)
          val idx = __newVar[Int](0)
          val h = __newVar[Int](0)
          __whileDo(readVar(idx) < len, {
            __assign(h, readVar(h) + OptimalStringApply(t.asInstanceOf[Expression[OptimalString]], readVar(idx)))
            __assign(idx, readVar(idx) + unit(1));
          })
          ReadVar(h)
        case x if x.isArray => ArrayLength(t.asInstanceOf[Rep[Array[Any]]])
        // Handle any additional cases here
        case x              => super.transformDef(node)
      }
    case BooleanUnary_$bang(b) => NameAlias[Boolean](None, "!", List(List(b)))
    case ToString(obj) => obj.tp.asInstanceOf[PardisType[_]] match {
      case PointerType(tp) if tp.isRecord => {
        // ReadVal(__app(record_toString(tp)).apply(obj))
        Apply(getStructToStringFunc(tp), obj)
      }
      case tp => throw new Exception(s"toString conversion for non-record type $tp is not handled for the moment")
    }
    case _ => super.transformDef(node)
  }).asInstanceOf[to.Def[T]]
}

class OptimalStringToCTransformer(override val IR: LoweringLegoBase) extends RuleBasedTransformer[LoweringLegoBase](IR) with CTransformer {
  import IR._
  import CNodes._
  import CTypes._

  rewrite += rule { case OptimalStringNew(x) => apply(x) }
  rewrite += rule { case OptimalStringString(x) => apply(x) }
  rewrite += rule { case OptimalStringDiff(x, y) => strcmp(apply(x), apply(y)) }
  rewrite += rule {
    case OptimalStringEndsWith(x, y) =>
      val lenx = strlen(apply(x))
      val leny = strlen(apply(y))
      val len = lenx - leny
      strncmp(apply(x) + len, apply(y), len) __== unit(0)
  }
  rewrite += rule {
    case OptimalStringStartsWith(x, y) =>
      strncmp(apply(x), apply(y), strlen(apply(y))) __== unit(0)
  }
  rewrite += rule { case OptimalStringCompare(x, y) => strcmp(apply(x), apply(y)) }
  rewrite += rule { case OptimalStringLength(x) => strlen(apply(x)) }
  rewrite += rule { case OptimalString$eq$eq$eq(x, y) => strcmp(apply(x), apply(y)) __== unit(0) }
  rewrite += rule { case OptimalString$eq$bang$eq(x, y) => infix_!=(strcmp(apply(x), apply(y)), unit(0)) }
  rewrite += rule { case OptimalStringContainsSlice(x, y) => infix_!=(strstr(apply(x), apply(y)), unit(null)) }
  rewrite += rule {
    case OptimalStringIndexOfSlice(x, y, idx) =>
      val substr = strstr(apply(x) + apply(idx), apply(y))
      __ifThenElse(substr __== unit(null), unit(-1), str_subtract(substr, apply(x)))
  }
  rewrite += rule {
    case OptimalStringApply(x, idx) =>
      val z = infix_asInstanceOf(apply(x))(typeArray(typePointer(CharType)))
      arrayApply(z, apply(idx))
  }
  rewrite += rule {
    case OptimalStringSlice(x, start, end) =>
      val len = apply(end) - apply(start) + unit(1)
      val newbuf = malloc(len)(CharType)
      strncpy(newbuf, apply(x) + apply(start), len - unit(1))
      newbuf
  }
}

class ScalaConstructsToCTranformer(override val IR: LoweringLegoBase) extends RuleBasedTransformer[LoweringLegoBase](IR) with CTransformer {
  import IR._
  import CNodes._
  import CTypes._

  rewrite += rule {
    case PardisIfThenElse(cond, thenp, elsep) if thenp.tp != UnitType =>
      val res = __newVar(unit(0))(thenp.tp.asInstanceOf[TypeRep[Int]])
      __ifThenElse(apply(cond), {
        __assign(res, inlineBlock(apply(thenp)))
      }, {
        __assign(res, inlineBlock(apply(elsep)))
      })
      ReadVar(res)(res.tp)
  }
  rewrite += rule {
    case or @ Boolean$bar$bar(case1, b @ PardisBlock(stmt, res)) => {
      val tb = inlineBlock(apply(b))
      apply(case1) || tb
    }
  }
  rewrite += rule {
    case and @ Boolean$amp$amp(case1, b @ PardisBlock(stmt, res)) => {
      apply(IfThenElse(apply(case1), transformBlock(b), Block(Nil, unit(false))))
    }
  }

  rewrite += rule { case OptionGet(x) => x }
  rewrite += rule { case IntUnary_$minus(self) => unit(-1) * self }
  rewrite += rule { case IntToLong(x) => x }
  rewrite += rule { case ByteToInt(x) => x }
  rewrite += rule { case IntToDouble(x) => x }
  rewrite += rule { case DoubleToInt(x) => infix_asInstanceOf[Double](x) }
}