package ch.epfl.data
package legobase
package deep

import scala.collection.mutable.ArrayBuffer
import ch.epfl.data.pardis.ir._
import ch.epfl.data.legobase.deep.scalalib._

import scala.language.implicitConversions

abstract class TransformerFunction {
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

case class NameAlias[A: Manifest](c: Option[Expression[_]], n: String, args: List[List[Expression[_]]]) extends FunctionNode[A](c, n, args) { this: Product => }

class GHashTable
object t2 extends TransformerFunction with HashMapOps with TreeSetOps with DeepDSL {
  val isRecursive = false
  case class GLibNew[A, B, C](eq: Rep[(A, A) => Boolean], hash: Rep[A => C])(implicit manifestA: Manifest[A], manifestB: Manifest[B], manifestC: Manifest[C]) extends FunctionDef[GHashTable](None, "g_hash_table_new", List(List(eq, hash)))
  def eq[A: Manifest] = doLambda2((x: Rep[A], y: Rep[A]) => unit(true))
  def hash[A: Manifest] = doLambda((x: Rep[A]) => unit(5))
  def apply(n: Def[Any])(implicit context: DeepDSL): Def[Any] = {
    n match {
      case nm @ HashMapNew2()                  => reifyBlock({ GLibNew(eq(nm.manifestA), hash(nm.manifestA))(nm.manifestA, nm.manifestB, manifest[Int]) })
      case HashMapSize(map)                    => NameAlias[Int](None, "g_hash_table_size", List(List(map)))
      case ma @ HashMapApply(map, key)         => NameAlias(None, "g_hash_table_lookup", List(List(map, key)))(ma.manifestB)
      case mc @ HashMapContains(map, key)      => NameAlias[Boolean](None, "g_hash_table_contains", List(List(map, key)))
      case mu @ HashMapUpdate(map, key, value) => NameAlias[Unit](None, "g_hash_table_insert", List(List(map, key, value)))
      case mr @ HashMapRemove(map, key)        => NameAlias[Unit](None, "g_hash_table_remove", List(List(map, key)))
      case op @ TreeSetHead(t)                 => NameAlias(None, "g_tree_head", List(List(t)))
      case op @ TreeSetSize(t)                 => NameAlias(None, "g_tree_nnodes", List(List(t)))
      case op @ TreeSet$minus$eq(self, t)      => NameAlias(None, "g_tree_remove", List(List(self, t)))
      case op @ TreeSet$plus$eq(self, t)       => NameAlias(None, "g_tree_insert", List(List(self, t)))
      case _                                   => n
    }
  }
}

class FILE
//case class Pointer[A: Manifest](x: Expression[A]) extends Expression[A]
case class PTRADDRESS[A: Manifest](x: Expression[A]) extends Expression[A] {
  override def toString() = "&" + x.toString
}

object t3 extends TransformerFunction with K2DBScannerOps with DeepDSL {
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
        toAtom(NameAlias[Unit](None, "fscanf", List(List(s, unit("%d|"), v))))
        v.x
      })
      case K2DBScannerNext_double(s) => reifyBlock({
        val v = PTRADDRESS(__newVar(unit(0.0)))
        toAtom(NameAlias[Unit](None, "fscanf", List(List(s, unit("%lf|"), v))))
        v.x
      })
      case K2DBScannerNext_char(s) => reifyBlock({
        val v = PTRADDRESS(__newVar(unit('a')))
        toAtom(NameAlias[Unit](None, "fscanf", List(List(s, unit("%c|"), v))))
        v.x
      })
      case K2DBScannerNext1(s, buf) => reifyBlock({
        var i = __newVar[Int](0)
        __whileDo(unit(true), {
          val v = Pointer(ArrayApply(buf, i))
          toAtom(FScanf(s, List(unit("%c"), v)))
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
        toAtom(NameAlias[Unit](None, "fscanf", List(List(s, unit("%d-%d-%d|"), x, y, z))))
        (x.x * unit(10000)) + (y.x * unit(100)) + z.x
      })
      case K2DBScannerHasNext(s) => NameAlias[Boolean](None, "!feof", List(List(s)))
      case _                     => n
    }
  }
}

object DefaultScalaTo2NameAliases extends TransformerFunction with DeepDSL {
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
        val hm = hashMapNew2(manifest[Int], manifest[String])
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
        val size = Integer.parseInt((("wc -l " + file) #| "awk {print($1)}" !!).replaceAll("\\s+$", ""))
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
}
