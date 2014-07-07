package ch.epfl.data
package legobase
package deep

import scala.collection.mutable.ArrayBuffer
import ch.epfl.data.pardis.ir._
import ch.epfl.data.legobase.deep.scalalib._
import ch.epfl.data.pardis.ir._

import scala.language.implicitConversions

trait TransformerFunction extends Function1[PardisNode[Any], PardisNode[Any]] with Base {
  val isRecursive: Boolean
}

class SerialTransformer extends Base {
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
        case _ => res
      }
    }))
  }

  def transformBlock[A: Manifest](b: PardisBlock[A], tl: List[TransformerFunction]): PardisBlock[A] =
    PardisBlock(b.stmts.map(st => transformStmt(st, tl)), b.res)
}

class GHashTable
object t1 extends HashMapOps with DeepDSL with TransformerFunction {
  val isRecursive = true
  def apply(n: Def[Any]): Def[Any] = {
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

object t2 extends HashMapOps with TreeSetOps with DeepDSL with TransformerFunction {
  val isRecursive = false
  case class GLibNew[A, B, C](eq: Rep[(A, A) => Boolean], hash: Rep[A => C])(implicit manifestA: Manifest[A], manifestB: Manifest[B], manifestC: Manifest[C]) extends FunctionDef[GHashTable](None, "g_hash_table_new", List(List(eq, hash)))
  def eq[A: Manifest] = doLambda2((x: Rep[A], y: Rep[A]) => unit(true))
  def hash[A: Manifest] = doLambda((x: Rep[A]) => unit(5))
  def apply(n: Def[Any]): Def[Any] = {
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

object DefaultScalaTo2NameAliases extends DeepDSL with TransformerFunction {
  val isRecursive = false
  def apply(n: Def[Any]): Def[Any] = n match {
    case Int$less$eq1(self, x) => NameAlias(Some(self), " <= ", List(List(x)))
    case Int$plus1(self, x)    => NameAlias(Some(self), " + ", List(List(x)))
    case Int$minus1(self, x)   => NameAlias(Some(self), " - ", List(List(x)))
    case Int$times1(self, x)   => NameAlias(Some(self), " * ", List(List(x)))
    case Int$div1(self, x)     => NameAlias(Some(self), " / ", List(List(x)))
    case Println(x)            => NameAlias[Unit](None, "printf", List(List(x)))
    case _                     => n
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
        val tt = new SerialTransformer()
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
        val file = "/mnt/ramdisk/sf1/lineitem.tbl"
        import scala.sys.process._;
        val size = Integer.parseInt((("wc -l " + file) #| "awk {print($1)}" !!).replaceAll("\\s+$", ""))
        // Load Relation 
        val s = __newK2DBScanner(unit(file))
        var i = unit(0)
        __whileDo(s.hasNext, {
          s.next_int
          s.next_int
          s.next_int
          s.next_int
          s.next_double
          s.next_double
          s.next_double
          s.next_double
          s.next_char
          s.next_char
          s.next_date
          s.next_date
          s.next_date
          loadString(25, s)
          loadString(10, s)
          loadString(44, s)
          i += unit(1)
          unit()
        })
        unit()
      }

      def hashMapTestBlock = {
        val b = reifyBlock(loadingTest)
        val tt = new SerialTransformer()
        tt.transformBlock(b, List(t1, t2, DefaultScalaTo2NameAliases))
      }
    }.hashMapTestBlock
  }
}
