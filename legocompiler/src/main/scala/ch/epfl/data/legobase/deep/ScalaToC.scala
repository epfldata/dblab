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
    else if (m.name.startsWith("Array")) structName(m.typeArguments.last)
    else if (m.name.startsWith("Pointer")) structName(m.typeArguments.last)
    else if (m.name == "Byte") "char"
    else {
      System.out.println("WARNING: Default structName given: " + m.name);
      super.structName(m)
    }
  }
}

class t3(override val IR: LoweringLegoBase) extends TopDownTransformerTraverser[LoweringLegoBase] {
  import IR._
  import CNodes._
  import CTypes._

  def pardisTypeIsPrimitive[T: PardisType] = {
    val tp = typeRep[T]
    tp == IntType || tp == DoubleType || tp == UnitType ||
      tp == BooleanType || tp == LongType || tp == ByteType ||
      tp == CharacterType
  }

  override def transformType[T: PardisType]: PardisType[Any] = ({
    val tp = typeRep[T]
    if (pardisTypeIsPrimitive[T]) super.transformType[T]
    else if (tp.name.startsWith("Array")) typeCArray(transformType(tp.typeArguments(0)))
    else if (tp.name.contains("HashMap")) typePointer(typeGHashTable)
    else if (tp.name.contains("Set")) typePointer(typeGList)
    else if (tp.name.contains("Seq")) typePointer(typeGList)
    else if (tp.name.contains("Record")) typePointer(tp)
    else if (tp.name.contains("Option")) transformType(tp.typeArguments(0))
    else if (tp.isInstanceOf[RecordType[_]])
      if (tp.typeArguments == List()) typePointer(tp)
      else typePointer(transformType(tp.typeArguments(0))) // check that
    else {
      System.out.println("WARNING: Default transformType called: " + tp)
      super.transformType[T]
    }
  }).asInstanceOf[PardisType[Any]]
  /*override def transformType[T: Manifest]: Manifest[Any] = {
    else if (tp.runtimeClass.toString.contains("PardisVar")) {
      val vartype = tp.typeArguments.head
      val res = getVarManifest(transformType(vartype))
      System.out.println("@@@" + res)
      res
    } else { System.out.println("---> " + tp); tp
    }).asInstanceOf[Manifest[Any]]
  }*/

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
    case K2DBScannerNext1(s, buf) =>
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
    case s @ PardisStruct(tag, elems) =>
      val x = malloc(unit(1))(s.tp)
      structCopy(x, s)
      ReadVal(x)(typePointer(s.tp))
    // Mapping Scala Array to C Array
    case a @ ArrayNew2(x) =>
      // Get type of elements stored in array
      val elemType = a.tp.typeArguments(0)
      // Allocate original array
      val array = {
        if (pardisTypeIsPrimitive(elemType)) malloc(x)(elemType)
        else malloc(x)(typePointer(elemType))
      }
      // Create wrapper with length
      val am = typeCArray(typeArray(elemType))
      val s = __new(("array", false, array), ("length", false, x))(am)
      val m = malloc(unit(1))(am)
      structCopy(m.asInstanceOf[Expression[Pointer[Any]]], s)
      ReadVal(m.asInstanceOf[Expression[Any]])(m.tp.asInstanceOf[PardisType[Any]])
    case au @ ArrayUpdate(a, i, v) => {
      val s = transformExp[Any, T](a)
      // Get type of elements stored in array
      val elemType = a.tp.typeArguments(0)
      // Get type of internal array
      val newTp = if (pardisTypeIsPrimitive(elemType)) elemType else typePointer(elemType)
      // Read array and perform update
      val arr = field(s, "array")(typeArray(typePointer(newTp)))
      ArrayUpdate(arr.asInstanceOf[Expression[Array[Any]]], i, v)
    }
    case ArrayFilter(a, op) =>
      val s = transformExp[Any, T](a)
      // Get type of elements stored in array
      val elemType = a.tp.typeArguments(0)
      // Get type of internal array
      val newTp = if (pardisTypeIsPrimitive(elemType)) elemType else typePointer(elemType)
      val arr = field(s, "array")(newTp)
      ReadVal(arr)(arr.tp)
    case ArrayApply(a, i) =>
      val s = transformExp[Any, T](a)
      // Get type of elements stored in array
      val elemType = a.tp.typeArguments(0)
      // Get type of internal array
      val newTp = if (pardisTypeIsPrimitive(elemType)) elemType else typePointer(elemType)
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
    /*case PardisAssign(PardisVar(a), b) =>
      val ta = transformExp(a)
      System.out.println(a.tp + "//" + ta.tp)
      val tb = transformExp(b)
      System.out.println(b.tp + "//" + tb.tp)
      PardisAssign(PardisVar(ta.asInstanceOf[Expression[PardisVar[Any]]]), tb)*/
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
    case _ =>
      super.transformDef(node)
  }).asInstanceOf[to.Def[T]]
}

/*********************/
// OLD CODE GOES HERE
/*********************/

/*abstract class TransformerFunction {
  val isRecursive: Boolean
  def apply(n: PardisNode[Any])(implicit context: DeepDSL): PardisNode[Any]
}

class SerialTransformer(implicit val context: DeepDSL) {
  def printBlock(b: PardisBlock[_]) =
    b.stmts.foreach(st => println(st))

  def transformStmt(st: Statement[_], tl: List[TransformerFunction]): Statement[_] = {
    val n: PardisNode[Any] = st.rhs
    Statement(st.sym, tl.foldLeft(n)((n, t) => {
      val res = t(n)
      res match {
        case b @ PardisBlock(stmt, res) => transformBlock(b, if (!t.isRecursive) tl else tl diff List(t))(b.res.tp)
        case ifte @ PardisIfThenElse(cond, thenp, elsep) =>
          PardisIfThenElse(ifte.cond,
            transformBlock(ifte.thenp, if (!t.isRecursive) tl else tl diff List(t)),
            transformBlock(ifte.elsep, if (!t.isRecursive) tl else tl diff List(t)))(ifte.typeT)
        case w @ PardisWhile(cond, block) =>
          PardisWhile(
            transformBlock(cond, if (!t.isRecursive) tl else tl diff List(t)),
            transformBlock(block, if (!t.isRecursive) tl else tl diff List(t)))
        case _ => res
      }
    }))
  }

  def transformBlock[A: PardisType](b: PardisBlock[A], tl: List[TransformerFunction]): PardisBlock[A] =
    PardisBlock(b.stmts.map(st => transformStmt(st, tl)), b.res)
}

object t1 extends TransformerFunction with HashMapOps with DeepDSL {
  import CNodes._
  val isRecursive = true
  def apply(n: Def[Any])(implicit context: DeepDSL): Def[Any] = {
    n match {
      case geu @ HashMapGetOrElseUpdate(map, key, value) =>
        reifyBlock({
          __ifThenElse(hashMapContains(map, key)(geu.typeB, geu.typeB),
            hashMapApply(map, key)(geu.typeA, geu.typeB),
            hashMapUpdate(map, key, value)(geu.typeA, geu.typeB))(geu.typeB)
        })(geu.typeB)
      case rm @ HashMapRemove(map, key) => reifyBlock({
        val x = hashMapApply(map, key)(rm.typeA, rm.typeB)
        hashMapRemove(map, key)(rm.typeA, rm.typeB)
        x
      })(rm.typeB)
      case _ => n
    }
  }
}
*/

class t2(override val IR: LoweringLegoBase) extends TopDownTransformerTraverser[LoweringLegoBase] {
  import IR._
  import CNodes._
  import CTypes._
  def eq[A: PardisType] = doLambda2((x: Rep[A], y: Rep[A]) => unit(true))
  def hash[A: PardisType] = doLambda((x: Rep[A]) => unit(5))
  override def transformDef[T: PardisType](node: Def[T]): to.Def[T] = (node match {
    case nm @ HashMapNew2_2() =>
      System.out.println(nm.typeA)
      val nA = typePointer(transformType(nm.typeA))
      val nB = typePointer(transformType(nm.typeB))
      GHashTableNew(eq(nA), hash(nA))(nA, nB, IntType)
    case HashMapSize(map)                    => NameAlias[Int](None, "g_hash_table_size", List(List(map)))
    case HashMapKeySet(map)                  => NameAlias[Pointer[GList]](None, "g_hash_table_get_keys", List(List(map)))
    case ma @ HashMapApply(map, key)         => NameAlias(None, "g_hash_table_lookup", List(List(map, key)))(ma.typeA)
    /*case mc @ HashMapContains(map, key)      => NameAlias[Boolean](None, "g_hash_table_contains", List(List(map, key)))*/
    case mu @ HashMapUpdate(map, key, value) => NameAlias[Unit](None, "g_hash_table_insert", List(List(map, key, value)))
    case hmgu @ HashMapGetOrElseUpdate(map, key, value) =>
      val v = transformDef(HashMapApply(map, key))
      val res = __ifThenElse(infix_==(toAtom(v), Constant(null)), {
        val newB = transformBlockTyped[T, T](value)
        ReadVal(newB)(newB.tp)
        transformDef(HashMapUpdate(map, key, newB))
      }, v)
      ReadVal(res)
    case mr @ HashMapRemove(map, key) =>
      val x = NameAlias(None, "g_hash_table_lookup", List(List(map, key)))(transformType(mr.typeB))
      NameAlias[Unit](None, "g_hash_table_remove", List(List(map, key)))(UnitType)
      x
    case nm @ SetNew(s)  => ReadVal(transformExp[Any, T](s))(transformType(s.tp).asInstanceOf[PardisType[T]])
    case nm @ SetNew2()  => PardisCast(Constant(null).asInstanceOf[Expression[Any]])(nm.tp.asInstanceOf[PardisType[Any]], typePointer(typeGList))
    case sh @ SetHead(s) => NameAlias(None, "g_list_first", List(List(s)))(transformType(s.tp))
    case SetRemove(s, e) => NameAlias(None, "g_list_remove", List(List(s, e)))(UnitType)
    case SetToSeq(set)   => ReadVal(set)(transformType(set.tp).asInstanceOf[PardisType[Set[Any]]])
    /*case op @ TreeSetHead(t)            => NameAlias(None, "g_tree_head", List(List(t)))
    case op @ TreeSetSize(t)            => NameAlias(None, "g_tree_nnodes", List(List(t)))
    case op @ TreeSet$minus$eq(self, t) => NameAlias(None, "g_tree_remove", List(List(self, t)))
    case op @ TreeSet$plus$eq(self, t)  => NameAlias(None, "g_tree_insert", List(List(self, t)))*/
    case ar @ AGGRecordNew(k, v) =>
      val data = field(k, "data")(k.tp)
      val s = __new(("key", false, data), ("aggs", false, v))(transformType(ar.tp))
      val x = malloc(unit(1))(s.tp)
      structCopy(x, s)
      ReadVal(x)(typePointer(s.tp).asInstanceOf[PardisType[Pointer[Any]]])
    case _ => super.transformDef(node)
  }).asInstanceOf[to.Def[T]]
}
/*
object t3 extends TransformerFunction with K2DBScannerOps with DeepDSL with CFunctions {
  import CNodes._
  val isRecursive = false
  def defToSym[A: Manifest](x: Def[A])(implicit context: DeepDSL): Sym[_] = {
    context.globalDefs.find(gd => gd._2.rhs == x) match {
      case Some(e) => e._1
      case None    => throw new Exception("symbol not found while looking up " + x + "!")
    }
  }
  def lookup[A: Manifest](x: Sym[A])(implicit context: DeepDSL): Def[_] = {
    context.globalDefs.find(gd => gd._1 == x) match {
      case Some(e) => e._2.rhs
      case None    => throw new Exception("symbol not found while looking up " + x + "!")
    }
  }
}

object DefaultScalaTo2NameAliases extends TransformerFunction with DeepDSL {
  import CNodes._
  val isRecursive = false
  def apply(n: Def[Any])(implicit context: DeepDSL): Def[Any] = n match {
    case Int$less$eq1(self, x)    => NameAlias[Boolean](Some(self), " <= ", List(List(x)))
    case Int$plus1(self, x)       => NameAlias[Int](Some(self), " + ", List(List(x)))
    case Int$plus5(self, x)       => NameAlias[Int](Some(self), " + ", List(List(x)))
    case Int$minus1(self, x)      => NameAlias[Int](Some(self), " - ", List(List(x)))
    case Int$minus4(self, x)      => NameAlias[Int](Some(self), " - ", List(List(x)))
    case Int$times1(self, x)      => NameAlias[Int](Some(self), " * ", List(List(x)))
    case Int$times4(self, x)      => NameAlias[Int](Some(self), " * ", List(List(x)))
    case Int$div1(self, x)        => NameAlias[Int](Some(self), " / ", List(List(x)))
    case Boolean$bar$bar(self, x) => NameAlias[Boolean](Some(self), " || ", List(List(x)))
    case Println(x)               => NameAlias[Unit](None, "printf", List(List(x)))
    case _                        => n
  }
}
*/
