package ch.epfl.data
package legobase
package deep

import scala.language.existentials
import ch.epfl.data.pardis.shallow.OptimalString
import scala.collection.mutable.ArrayBuffer
import ch.epfl.data.pardis.ir._
import ch.epfl.data.pardis.ir.pardisTypeImplicits._
import ch.epfl.data.legobase.deep.scalalib._
import legobase.deep._
import pardis.optimization._
import scala.language.implicitConversions
import ch.epfl.data.pardis.utils.Utils._
import scala.reflect.runtime.universe
import ch.epfl.data.pardis.ir.StructTags._

trait ScalaToC extends DeepDSL with K2DBScannerOps with CFunctions { this: Base =>
  import CNodes._
  override def structName[T](m: PardisType[T]): String = {
    if (m.name.startsWith("CArray")) "ArrayOf" + structName(m.typeArguments.last)
    else if (m.isArray) structName(m.typeArguments.last)
    else if (m.name.startsWith("Pointer")) structName(m.typeArguments.last)
    else if (m.name == "Byte") "char"
    else if (m.name == "Double") "double"
    else {
      System.out.println("WARNING: Default structName given: " + m.name);
      super.structName(m)
    }
  }
}

class ScalaConstructsToCTranformer(override val IR: LoweringLegoBase) extends TopDownTransformerTraverser[LoweringLegoBase] {
  import IR._
  import CNodes._
  import CTypes._

  override def transformType[T: PardisType]: PardisType[Any] = ({
    val tp = typeRep[T]
    if (tp.isPrimitive) super.transformType[T]
    else if (tp.isArray) typePointer(typeCArray(tp.typeArguments(0)))
    else if (tp.name.contains("K2DBScanner")) typeFile
    else if (tp.name.contains("TreeSet")) typePointer(typeGTree(transformType(tp.typeArguments(0))))
    else if (tp.name.contains("Set")) typePointer(typeGList(transformType(tp.typeArguments(0))))
    else if (tp.name.contains("HashMap")) typePointer(typeGHashTable(transformType(tp.typeArguments(0)), transformType(tp.typeArguments(1))))
    else if (tp.isRecord) {
      if (tp.typeArguments == List()) typePointer(tp)
      else typePointer(transformType(tp.typeArguments(0)))
    } else if (tp.name.contains("Option")) typePointer(transformType(tp.typeArguments(0)))
    else {
      System.out.println("WARNING: Default transformType called: " + tp)
      super.transformType[T]
    }
  }).asInstanceOf[PardisType[Any]]

  override def transformDef[T: TypeRep](node: Def[T]): to.Def[T] = (node match {
    // Mapping Scala Scanner operations to C FILE operations
    case K2DBScannerNew(f) => NameAlias[FILE](None, "fopen", List(List(f, unit("r"))))
    case K2DBScannerNext_int(s) =>
      val v = readVar(__newVar[Int](0))
      __ifThenElse[Unit](infix_==(fscanf(s, unit("%d|"), &(v)), eof), break, unit(()))
      ReadVal(v)
    case K2DBScannerNext_double(s) =>
      val v = readVar(__newVar(unit(0.0)))
      __ifThenElse(infix_==(fscanf(s, unit("%lf|"), &(v)), eof), break, unit)
      ReadVal(v)
    case K2DBScannerNext_char(s) =>
      val v = readVar(__newVar(unit('a')))
      __ifThenElse(infix_==(fscanf(s, unit("%c|"), &(v)), eof), break, unit)
      ReadVal(v)
    case nn @ K2DBScannerNext1(s, buf) =>
      var i = __newVar[Int](0)
      __whileDo(unit(true), {
        val v = readVar(__newVar(unit('a')))
        __ifThenElse(infix_==(fscanf(s, unit("%c"), &(v)), eof), break, unit)
        val z = infix_==(ReadVal(v), unit('\n'))
        __ifThenElse[Unit]((infix_==(ReadVal(v), unit('|')) || z), break, unit)
        toAtom(transformDef(ArrayUpdate(buf.asInstanceOf[Expression[Array[AnyVal]]], readVar(i), ReadVal(v))(buf.tp.typeArguments(0).asInstanceOf[TypeRep[AnyVal]])))
        __assign(i, readVar(i) + unit(1))
      })
      toAtom(transformDef(ArrayUpdate(buf.asInstanceOf[Expression[Array[AnyVal]]], readVar(i), unit('\0'))(buf.tp.typeArguments(0).asInstanceOf[TypeRep[AnyVal]])))
      ReadVar(i)
    case K2DBScannerNext_date(s) =>
      val x = readVar(__newVar[Int](unit(0)))
      val y = readVar(__newVar[Int](unit(0)))
      val z = readVar(__newVar[Int](unit(0)))
      __ifThenElse(infix_==(fscanf(s, unit("%d-%d-%d|"), &(x), &(y), &(z)), eof), break, unit)
      ((toAtom(ReadVal(x)) * unit(10000)) + (toAtom(ReadVal(y)) * unit(100)) + toAtom(ReadVal(z))).correspondingNode
    case K2DBScannerHasNext(s) => ReadVal(Constant(true))
    case FileLineCount(Constant(x: String)) =>
      val p = popen(unit("wc -l " + x), unit("r"))
      val cnt = readVar(__newVar[Int](0))
      fscanf(p, unit("%d"), &(cnt))
      pclose(p)
      ReadVal(cnt)
    case OptimalStringNew(x) => x.correspondingNode
    case s @ PardisStruct(tag, elems, methods) =>
      // TODO if needed method generation should be added
      val x = malloc(unit(1))(s.tp)
      structCopy(x, PardisStruct(tag, elems, methods.map(m => m.copy(body = transformDef(m.body.asInstanceOf[Def[Any]]).asInstanceOf[PardisLambdaDef]))))
      ReadVal(x)(typePointer(s.tp))
    // Mapping Scala Array to C Array
    case a @ ArrayNew(x) =>
      // Get type of elements stored in array
      val elemType = a.tp.typeArguments(0)
      // Allocate original array
      val array = malloc(x)(transformType(elemType))
      // Create wrapper with length
      val am = transformType(a.tp)
      val s = __new(("array", false, array), ("length", false, x))(am)
      val m = malloc(unit(1))(am.typeArguments(0))
      structCopy(m.asInstanceOf[Expression[Pointer[Any]]], s)
      ReadVal(m.asInstanceOf[Expression[Any]])(m.tp.asInstanceOf[PardisType[Any]])
    case au @ ArrayUpdate(a, i, v) => {
      val s = transformExp[Any, T](a)
      // Get type of elements stored in array
      val elemType = a.tp.typeArguments(0)
      // Get type of internal array
      val newTp = if (elemType.isPrimitive) elemType else typePointer(elemType)
      // Read array and perform update
      val arr = field(s, "array")(typeArray(typePointer(newTp)))
      ArrayUpdate(arr.asInstanceOf[Expression[Array[Any]]], i, v)
    }
    case ArrayFilter(a, op) =>
      val s = transformExp[Any, T](a)
      // Get type of elements stored in array
      val elemType = a.tp.typeArguments(0)
      // Get type of internal array
      val newTp = if (elemType.isPrimitive) elemType else typePointer(elemType)
      val arr = field(s, "array")(newTp)
      ReadVal(arr)(arr.tp)
    case ArrayApply(a, i) =>
      val s = transformExp[Any, T](a)
      // Get type of elements stored in array
      val elemType = a.tp.typeArguments(0)
      // Get type of internal array
      val newTp = if (elemType.isPrimitive) elemType else typePointer(elemType)
      val arr = field(s, "array")(typeArray(typePointer(newTp)))
      ArrayApply(arr.asInstanceOf[Expression[Array[Any]]], i)(newTp.asInstanceOf[PardisType[Any]])
    case ArrayLength(a) =>
      val s = transformExp[Any, T](a)
      val arr = field(s, "length")(IntType)
      ReadVal(arr)(IntType)
    case PardisReadVal(s) =>
      val ss = transformExp[Any, T](s)
      PardisReadVal(ss)(ss.tp.asInstanceOf[PardisType[T]])
    case IfThenElse(cond, ifStmt, elseStmt) =>
      val ifStmtT = transformBlock(ifStmt)
      val elseStmtT = transformBlock(elseStmt)
      IfThenElse(cond, ifStmtT, elseStmtT)(elseStmtT.res.tp)
    case PardisNewVar(v) =>
      val tv = transformExp[Any, T](v)
      PardisNewVar(tv)(tv.tp)
    case PardisReadVar(PardisVar(s)) =>
      val ss = transformExp[Any, T](s)
      PardisReadVar(PardisVar(ss.asInstanceOf[Expression[PardisVar[Any]]]))(ss.tp.asInstanceOf[PardisType[Any]])
    case or @ Boolean$bar$bar(case1, case2) => {
      case2 match {
        case b @ PardisBlock(stmt, res) =>
          val newB = transformBlockTyped[Boolean, T](b)
          ReadVal(newB)(newB.tp)
          val v = boolean$bar$bar(case1, newB.res.asInstanceOf[Expression[Boolean]])
          ReadVal(v)(BooleanType)
        case _ => or
      }
    }
    case and @ Boolean$amp$amp(case1, case2) => {
      case2 match {
        case b @ PardisBlock(stmt, res) =>
          val newB = transformBlockTyped[Boolean, T](b)
          ReadVal(newB)(newB.tp)
          val v = boolean$amp$amp(case1, newB.res.asInstanceOf[Expression[Boolean]])
          ReadVal(v)(BooleanType)
        case _ => and
      }
    }
    // Profiling and utils functions mapping
    case pc @ PardisCast(Constant(null)) =>
      PardisCast(Constant(0.asInstanceOf[Any]))(transformType(pc.castFrom), transformType(pc.castTp))
    case RunQuery(b) =>
      val diff = readVar(__newVar[TimeVal](PardisCast[Int, TimeVal](unit(0))))
      val start = readVar(__newVar[TimeVal](PardisCast[Int, TimeVal](unit(0))))
      val end = readVar(__newVar[TimeVal](PardisCast[Int, TimeVal](unit(0))))
      gettimeofday(&(start))
      toAtom(transformBlock(b))
      gettimeofday(&(end))
      val tm = timeval_subtract(&(diff), &(end), &(start))
      Printf(unit("Generated code run in %ld milliseconds."), tm)
    case OptionGet(x) => ReadVal(x.asInstanceOf[Expression[Any]])(transformType(x.tp))
    case ParseDate(Constant(d)) =>
      val data = d.split("-").map(x => x.toInt)
      ReadVal(Constant((data(0) * 10000) + (data(1) * 100) + data(2)))
    case imtf @ PardisStructImmutableField(s, f) =>
      PardisStructImmutableField(s, f)(transformType(imtf.tp))
    case _ =>
      super.transformDef(node)
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
    if (tp.name.startsWith("CArray")) tp //typePointer(typeCArray(transformType(tp.typeArguments(0))))
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

  def eq[A: PardisType] = {
    val structDef = getStructDef[A].get
    val eqMethod = transformDef(structDef.methods.find(_.name == "equals").get.body.asInstanceOf[PardisLambda2[A, A, Boolean]])
    toAtom(eqMethod)(eqMethod.tp)
  }
  def hash[A: PardisType] = {
    val structDef = getStructDef[A].get
    val hashMethod = transformDef(structDef.methods.find(_.name == "hash").get.body.asInstanceOf[PardisLambda[A, Int]])
    toAtom(hashMethod)(hashMethod.tp)
  }
  def treeHead[A: PardisType, B: PardisType] = doLambda3((s1: Rep[A], s2: Rep[A], s3: Rep[Pointer[B]]) => {
    pointer_assign(s3.asInstanceOf[Expression[Pointer[Any]]], s2)
    unit(0)
  })
  override def transformDef[T: PardisType](node: Def[T]): to.Def[T] = (node match {
    /* HashMap Operations */
    case nm @ HashMapNew2() =>
      val nA = typePointer(transformType(nm.typeA)).asInstanceOf[TypeRep[Any]]
      val nB = typePointer(transformType(nm.typeB))
      GHashTableNew(hash(nm.typeA), eq(nm.typeA))(nA, nB, IntType)
    case HashMapSize(map)                    => NameAlias[Int](None, "g_hash_table_size", List(List(map)))
    case HashMapKeySet(map)                  => NameAlias[Pointer[GList[FILE]]](None, "g_hash_table_get_keys", List(List(map)))
    case ma @ HashMapApply(map, key)         => NameAlias(None, "g_hash_table_lookup", List(List(map, key)))(transformType(map.tp.typeArguments(0).typeArguments(1)))
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
      NameAlias(None, "g_tree_new", List(List(Lambda2(f, i2, i1, o))))(transformType(ts.tp))
    case tsa @ TreeSet$plus$eq(t, s)    => NameAlias[Unit](None, "g_tree_insert", List(List(t, s, s)))
    case op @ TreeSet$minus$eq(self, t) => NameAlias[Boolean](None, "g_tree_remove", List(List(self, t)))
    case op @ TreeSetSize(t)            => NameAlias[Int](None, "g_tree_nnodes", List(List(t)))
    case op @ TreeSetHead(t) =>
      val elemType = t.tp.typeArguments(0).typeArguments(0)
      val init = infix_asInstanceOf(Constant(null))(elemType)
      toAtom(NameAlias[Unit](None, "g_tree_foreach", List(List(t, treeHead(elemType, elemType), &(init)(elemType.asInstanceOf[PardisType[Any]])))))
      ReadVal(init)(init.tp)

    /* Other operations */
    case imtf @ PardisStructImmutableField(s, f) =>
      PardisStructImmutableField(s, f)(transformType(imtf.tp))
    case HashCode(t) => t.tp match {
      case x if x.isPrimitive             => PardisCast(t)(x, IntType)
      case x if x.name == "Character"     => PardisCast(t)(x, IntType)
      case x if x.name == "OptimalString" => ArrayLength(t.asInstanceOf[Rep[Array[Any]]])
      case x if x.isArray                 => ArrayLength(t.asInstanceOf[Rep[Array[Any]]])
      // Handle any additional cases here
      case x                              => super.transformDef(node)
    }
    case _ => super.transformDef(node)
  }).asInstanceOf[to.Def[T]]
}
