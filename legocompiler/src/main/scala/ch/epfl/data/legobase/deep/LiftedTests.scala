package ch.epfl.data
package legobase
package deep

import scala.language.existentials
import ch.epfl.data.pardis.shallow.OptimalString
import scala.collection.mutable.ArrayBuffer
import ch.epfl.data.pardis.ir._
import ch.epfl.data.legobase.deep.scalalib._
import legobase.deep._
import pardis.optimization._
import scala.language.implicitConversions
import ch.epfl.data.pardis.utils.Utils._
import scala.reflect.runtime.universe
import ch.epfl.data.pardis.ir.StructTags._

trait ScalaToC extends DeepDSL with K2DBScannerOps with CFunctions { this: Base =>
  import CNodes._
  // Hack will be removed once lewis has the manifest thing ready
  override def structName[T](m: Manifest[T]): String = {
    if (m <:< manifest[CArray[Any]]) "ArrayOf" + structName(m.typeArguments.last)
    else if (m <:< manifest[Pointer[Any]]) structName(m.typeArguments.last)
    else if (m.runtimeClass.toString == "byte") "char"
    //"XM"
    //    "LALA"
    else { System.out.println("MPLOYM " + m.runtimeClass.toString); super.structName(m) }
  }
}

class t3(override val IR: LoweringLegoBase) extends TopDownTransformerTraverser[LoweringLegoBase] {
  import IR._
  import CNodes._

  override def transformType[T: Manifest]: Manifest[Any] = {
    val tp = manifest[T].asInstanceOf[Manifest[Any]]
    (if (tp.runtimeClass.isArray) {
      getArrayManifest({
        if (tp.typeArguments.head.runtimeClass.isPrimitive) getPointerManifest(tp.typeArguments.head)
        else getPointerManifest(getPointerManifest(tp.typeArguments.head))
      })
    } else if (tp <:< manifest[pardis.shallow.CaseClassRecord])
      getPointerManifest(tp)
    else if (tp <:< manifest[scala.collection.mutable.HashMap[Any, Any]])
      getPointerManifest(manifest[GHashTable])
    else if (tp <:< manifest[scala.collection.mutable.Set[Any]])
      getPointerManifest(manifest[GList])
    else if (tp <:< manifest[scala.collection.Seq[Any]])
      getPointerManifest(manifest[GList])
    else if (tp.runtimeClass.toString.contains("PardisVar")) {
      val vartype = tp.typeArguments.head
      val res = getVarManifest(transformType(vartype))
      System.out.println("@@@" + res)
      res
    } else { System.out.println("---> " + tp); tp }).asInstanceOf[Manifest[Any]]
  }

  override def transformDef[T: Manifest](node: Def[T]): to.Def[T] = (node match {
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
        __ifThenElse[Unit]((infix_==(ReadVal(v), unit('|')) || infix_==(ReadVal(v), unit('\n'))), break, unit)
        toAtom(transformDef(ArrayUpdate(buf.asInstanceOf[Expression[Array[AnyVal]]], readVar(i), ReadVal(v))))
        __assign(i, readVar(i) + unit(1))
      })
      toAtom(transformDef(ArrayUpdate(buf.asInstanceOf[Expression[Array[AnyVal]]], readVar(i), unit('\0'))))
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
      val x = malloc(unit(1))(s.manifestT)
      structCopy(x, s)
      ReadVal(x)(getPointerManifest(s.tp))
    // Mapping Scala Array to C Array
    case a @ ArrayNew2(x) =>
      /* Allocate original array */
      val array = {
        if (a.manifestT.runtimeClass.isPrimitive) malloc(x)(a.manifestT)
        else malloc(x)(getPointerManifest(a.manifestT))
      }
      /* Create wrapper with length */
      val am = getArrayManifest(a.manifestT)
      val s = Struct(ClassTag(structName(am)), Seq(
        PardisStructArg("array", false, array),
        PardisStructArg("length", false, x)))(am)
      /* Initialize wrapper */
      val m = malloc(unit(1))(am)
      structCopy(m.asInstanceOf[Expression[Pointer[Any]]], Struct(s.tag, s.elems))
      ReadVal(m.asInstanceOf[Expression[Any]])(m.tp.asInstanceOf[Manifest[Any]])
    case au @ ArrayUpdate(a, i, v) => {
      val s = transformExp(a)
      val arr = field(s, "array")(s.tp.typeArguments.head)
      ArrayUpdate(arr.asInstanceOf[Expression[Array[Any]]], i, v)
    }
    case ArrayApply(a, i) =>
      val s = transformExp(a)
      val arr = field(s, "array")(s.tp.typeArguments.head)
      ArrayApply(arr.asInstanceOf[Expression[Array[Any]]], i)(s.tp.typeArguments.head.typeArguments.head.asInstanceOf[Manifest[Any]])
    case ArrayLength(a) =>
      val s = transformExp(a)
      val arr = field(s, "length")(manifest[Int])
      ReadVal(arr)(manifest[Int])
    case PardisReadVal(s) => {
      val ss = transformExp(s)
      val m = {
        if (s.tp.runtimeClass.isArray) {
          getArrayManifest({
            if (s.tp.typeArguments.head.runtimeClass.isPrimitive) s.tp.typeArguments.head
            else (getPointerManifest(s.tp.typeArguments.head))
          })
        } else transformType(s.tp)
      }
      PardisReadVal(ss)(m.asInstanceOf[Manifest[T]])
    }
    case IfThenElse(cond, ifStmt, elseStmt) =>
      val ifStmtT = transformBlock(ifStmt)
      val elseStmtT = transformBlock(elseStmt)
      System.out.println(elseStmtT.res.tp)
      IfThenElse(cond, ifStmtT, elseStmtT)(elseStmtT.res.tp)
    case PardisNewVar(v) =>
      val tv = transformExp(v)
      PardisNewVar(tv)(tv.tp)
    case PardisReadVar(PardisVar(s)) =>
      val ss = transformExp(s)
      val m = {
        if (s.tp.runtimeClass.isArray) {
          getArrayManifest({
            if (s.tp.typeArguments.head.runtimeClass.isPrimitive) s.tp.typeArguments.head
            else (getPointerManifest(s.tp.typeArguments.head))
          })
        } else s.tp
      }
      PardisReadVar(PardisVar(ss.asInstanceOf[Expression[PardisVar[Any]]]))(m.asInstanceOf[Manifest[Any]])
    case PardisAssign(PardisVar(a), b) =>
      val ta = transformExp(a)
      System.out.println(a.tp + "//" + ta.tp)
      val tb = transformExp(b)
      System.out.println(b.tp + "//" + tb.tp)
      PardisAssign(PardisVar(ta.asInstanceOf[Expression[PardisVar[Any]]]), tb)
    case or @ Boolean$bar$bar(case1, case2) => {
      case2 match {
        case b @ PardisBlock(stmt, res) =>
          val newB = transformBlockTyped(b)
          System.out.println(newB.tp)
          ReadVal(newB)(newB.tp)
          val v = boolean$bar$bar(case1, newB.res.asInstanceOf[Expression[Boolean]])
          ReadVal(v)(manifest[Boolean])
        case _ => or
      }
    }
    case ArrayFilter(a, op) =>
      val s = transformExp(a)
      val arr = field(s, "array")(s.tp.typeArguments.head)
      ReadVal(arr)(arr.tp)
    // Profiling and utils functions mapping
    case pc @ PardisCast(Constant(null)) => PardisCast(Constant(0.asInstanceOf[Any]))(pc.castFrom, getPointerManifest(pc.castTp))
    case RunQuery(b) =>
      val diff = readVar(__newVar[TimeVal](PardisCast[Int, TimeVal](unit(0))))
      val start = readVar(__newVar[TimeVal](PardisCast[Int, TimeVal](unit(0))))
      val end = readVar(__newVar[TimeVal](PardisCast[Int, TimeVal](unit(0))))
      gettimeofday(&(start))
      toAtom(transformBlock(b))
      gettimeofday(&(end))
      val tm = timeval_subtract(&(diff), &(end), &(start))
      Printf(unit("Generated code run in %ld milliseconds."), tm)
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
            transformBlock(ifte.elsep, if (!t.isRecursive) tl else tl diff List(t)))(ifte.manifestT)
        case w @ PardisWhile(cond, block) =>
          PardisWhile(
            transformBlock(cond, if (!t.isRecursive) tl else tl diff List(t)),
            transformBlock(block, if (!t.isRecursive) tl else tl diff List(t)))
        case _ => res
      }
    }))
  }

  def transformBlock[A: Manifest](b: PardisBlock[A], tl: List[TransformerFunction]): PardisBlock[A] =
    PardisBlock(b.stmts.map(st => transformStmt(st, tl)), b.res)
}

object t1 extends TransformerFunction with HashMapOps with DeepDSL {
  import CNodes._
  val isRecursive = true
  def apply(n: Def[Any])(implicit context: DeepDSL): Def[Any] = {
    n match {
      case geu @ HashMapGetOrElseUpdate(map, key, value) =>
        reifyBlock({
          __ifThenElse(hashMapContains(map, key)(geu.manifestA, geu.manifestB),
            hashMapApply(map, key)(geu.manifestA, geu.manifestB),
            hashMapUpdate(map, key, value)(geu.manifestA, geu.manifestB))(geu.manifestB)
        })(geu.manifestB)
      case rm @ HashMapRemove(map, key) => reifyBlock({
        val x = hashMapApply(map, key)(rm.manifestA, rm.manifestB)
        hashMapRemove(map, key)(rm.manifestA, rm.manifestB)
        x
      })(rm.manifestB)
      case _ => n
    }
  }
}
*/

class t2(override val IR: LoweringLegoBase) extends TopDownTransformerTraverser[LoweringLegoBase] {
  import IR._
  import CNodes._
  def eq[A: Manifest] = doLambda2((x: Rep[A], y: Rep[A]) => unit(true))
  def hash[A: Manifest] = doLambda((x: Rep[A]) => unit(5))
  override def transformDef[T: Manifest](node: Def[T]): to.Def[T] = (node match {
    case nm @ HashMapNew2_2()                => reifyBlock({ GHashTableNew(eq(nm.manifestA), hash(nm.manifestA))(nm.manifestA, nm.manifestB, manifest[Int]) })
    case HashMapSize(map)                    => NameAlias[Int](None, "g_hash_table_size", List(List(map)))
    case HashMapKeySet(map)                  => NameAlias[Pointer[GList]](None, "g_hash_table_get_keys", List(List(map)))
    case ma @ HashMapApply(map, key)         => NameAlias(None, "g_hash_table_lookup", List(List(map, key)))(ma.manifestB)
    case mc @ HashMapContains(map, key)      => NameAlias[Boolean](None, "g_hash_table_contains", List(List(map, key)))
    case mu @ HashMapUpdate(map, key, value) => NameAlias[Unit](None, "g_hash_table_insert", List(List(map, key, value)))
    case mr @ HashMapRemove(map, key)        => NameAlias[Unit](None, "g_hash_table_remove", List(List(map, key)))
    case nm @ SetNew(s)                      => ReadVal(transformExp(s))(transformType(s.tp).asInstanceOf[Manifest[T]])
    case nm @ SetNew2()                      => NameAlias[Pointer[GList]](None, "g_list_new", List(List()))
    case SetHead(s)                          => NameAlias(None, "g_list_first", List(List(s)))
    case SetRemove(s, e)                     => NameAlias(None, "g_list_remove", List(List(s, e)))
    case SetToSeq(set)                       => ReadVal(set)(transformType(set.tp).asInstanceOf[Manifest[Set[Any]]])
    case op @ TreeSetHead(t)                 => NameAlias(None, "g_tree_head", List(List(t)))
    case op @ TreeSetSize(t)                 => NameAlias(None, "g_tree_nnodes", List(List(t)))
    case op @ TreeSet$minus$eq(self, t)      => NameAlias(None, "g_tree_remove", List(List(self, t)))
    case op @ TreeSet$plus$eq(self, t)       => NameAlias(None, "g_tree_insert", List(List(self, t)))
    case _                                   => super.transformDef(node)
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

  def apply(n: Def[Any])(implicit context: DeepDSL): Def[Any] = {
    n match {
      case K2DBScannerNew(f) => NameAlias[FILE](None, "fopen", List(List(f, unit("r"))))
      case K2DBScannerNext_int(s) => reifyBlock({
        val v = PTRADDRESS(__newVar(0))
        __ifThenElse(infix_==(FScanf(s, unit("%d|"), List(v)), EOF()), Break(), unit())
        v.x
      })
      case K2DBScannerNext_double(s) => reifyBlock({
        val v = PTRADDRESS(__newVar(unit(0.0)))
        __ifThenElse(infix_==(FScanf(s, unit("%lf|"), List(v)), EOF()), Break(), unit())
        v.x
      })
      case K2DBScannerNext_char(s) => reifyBlock({
        val v = PTRADDRESS(__newVar(unit('a')))
        __ifThenElse(infix_==(FScanf(s, unit("%c|"), List(v)), EOF()), Break(), unit())
        v.x
      })
      case K2DBScannerNext1(s, buf) => reifyBlock({
        var i = __newVar[Int](0)
        __whileDo(unit(true), {
          val v = Pointer(ArrayApply(buf, i))
          __ifThenElse(infix_==(FScanf(s, unit("%c"), List(v)), EOF()), Break(), unit())
          __ifThenElse[Unit]((infix_==(buf(i), unit('|')) || infix_==(buf(i), unit('\n'))), toAtom(Break()), unit())
          __assign(i, readVar(i) + unit(1))
        })
        buf(readVar(i)) = unit('\0');
        buf
      })
      case K2DBScannerNext_date(s) => reifyBlock({
        val x = PTRADDRESS(__newVar[Int](0))
        val y = PTRADDRESS(__newVar[Int](0))
        val z = PTRADDRESS(__newVar[Int](0))
        __ifThenElse(infix_==(FScanf(s, unit("%d-%d-%d|"), List(x, y, z)), EOF()), Break(), unit())
        (x.x * unit(10000)) + (y.x * unit(100)) + z.x
      })
      case K2DBScannerHasNext(s) => reifyBlock({ Constant(true) })
      case FileLineCount(x) =>
        reifyBlock({
          val p = popen(x, List(unit("r")))
          val cnt = PTRADDRESS(__newVar[Int](0))
          fscanf(p, unit("%d"), List(cnt))
          pclose(p)
          cnt.x
        })
      case _ => n
    }
  }
}

object DefaultScalaTo2NameAliases extends TransformerFunction with DeepDSL {
  import CNodes._
  val isRecursive = false
  def apply(n: Def[Any])(implicit context: DeepDSL): Def[Any] = n match {
    case Int$less$eq1(self, x)    => NameAlias(Some(self), " <= ", List(List(x)))
    case Int$plus1(self, x)       => NameAlias[Int](Some(self), " + ", List(List(x)))
    case Int$plus5(self, x)       => NameAlias[Int](Some(self), " + ", List(List(x)))
    case Int$minus1(self, x)      => NameAlias(Some(self), " - ", List(List(x)))
    case Int$minus4(self, x)      => NameAlias[Int](Some(self), " - ", List(List(x)))
    case Int$times1(self, x)      => NameAlias(Some(self), " * ", List(List(x)))
    case Int$times4(self, x)      => NameAlias[Int](Some(self), " * ", List(List(x)))
    case Int$div1(self, x)        => NameAlias(Some(self), " / ", List(List(x)))
    case Boolean$bar$bar(self, x) => NameAlias[Boolean](Some(self), " || ", List(List(x)))
    case Println(x)               => NameAlias[Unit](None, "printf", List(List(x)))
    case _                        => n
  }
}

class LiftedTests extends Base {
  def hashMapTest() = {
    new HashMapOps with DeepDSL {
      def hashMapTest = {
        val hm = hashMapNew2()(manifest[Int], manifest[String])
        val f = hashMapGetOrElseUpdate(hm, 5, { unit("lala") })
        println(f)
        val g = hashMapGetOrElseUpdate(hm, 7, { unit("foo") })
        println(f)
        printf(unit("%d\n"), hm.size)
        hashMapRemove(hm, 3)
        printf(unit("%d\n"), hm.size)
      }

      def hashMapTestBlock = {
        val b = reifyBlock(hashMapTest)
        val tt = new SerialTransformer()(this)
        tt.transformBlock(b, List(t1, t2, DefaultScalaTo2NameAliases))
      }
    }.hashMapTestBlock
  }

  def loadingTest() = {
    new HashMapOps with DeepDSL {
      def loadString(size: Int, s: Rep[K2DBScanner]) = {
        val NAME = __newArray[Byte](size)
        s.next(NAME)
        //NAME.filter(y => y != 0)
      }

      def loadingTest = {
        val file = "/mnt/ramdisk/test/lineitem.tbl"
        import scala.sys.process._;
        val size = java.lang.Integer.parseInt((("wc -l " + file) #| "awk {print($1)}" !!).replaceAll("\\s+$", ""))
        // Load Relation 
        val s = __newK2DBScanner(unit(file))
        var i = __newVar[Int](0)
        __whileDo(s.hasNext, {
          printf(unit("%d|%d|%d|%d|%lf|%lf|%lf|%lf|%c|%c|%d|%d|%d|%s|%s|%s|\n"),
            s.next_int,
            s.next_int,
            s.next_int,
            s.next_int,
            s.next_double,
            s.next_double,
            s.next_double,
            s.next_double,
            s.next_char,
            s.next_char,
            s.next_date,
            s.next_date,
            s.next_date,
            loadString(25, s),
            loadString(10, s),
            loadString(44, s))
          __assign(i, readVar(i) + unit(1))
          unit()
        })
        unit()
      }

      def hashMapTestBlock = {
        val b = reifyBlock(loadingTest)
        val tt = new SerialTransformer()(this)
        tt.transformBlock(b, List(t1, t2, t3, DefaultScalaTo2NameAliases))
      }
    }.hashMapTestBlock
  }
}*/
