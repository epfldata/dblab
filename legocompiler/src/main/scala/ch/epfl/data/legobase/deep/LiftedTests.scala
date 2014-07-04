package ch.epfl.data
package legobase
package deep

import scala.collection.mutable.ArrayBuffer
import ch.epfl.data.pardis.ir._
import ch.epfl.data.legobase.deep.scalalib._

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

object t2 extends HashMapOps with DeepDSL with TransformerFunction {
  val isRecursive = false
  case class GLibNew[A, B, C](eq: Rep[(A, A) => Boolean], hash: Rep[A => C])(implicit manifestA: Manifest[A], manifestB: Manifest[B], manifestC: Manifest[C]) extends FunctionDef[GHashTable](None, "g_hash_table_new", List(List(eq, hash)))
  case class GLibApply[A, B](self: Rep[HashMap[A, B]], key: Rep[A])(implicit manifestA: Manifest[A], manifestB: Manifest[B]) extends FunctionDef[B](Some(self), "g_hash_table_lookup", List(List(key)))
  case class GLibContains[A, B](self: Rep[HashMap[A, B]], key: Rep[A])(implicit manifestA: Manifest[A], manifestB: Manifest[B]) extends FunctionDef[Boolean](Some(self), "g_hash_table_contains", List(List(key)))
  case class GLibRemove[A, B](self: Rep[HashMap[A, B]], key: Rep[A])(implicit val manifestA: Manifest[A], val manifestB: Manifest[B]) extends FunctionDef[Unit](Some(self), "g_hash_table_remove", List(List(key)))
  case class GLibSize[A, B](self: Rep[HashMap[A, B]])(implicit val manifestA: Manifest[A], val manifestB: Manifest[B]) extends FunctionDef[Int](Some(self), "g_hash_table_size", List())
  case class GLibUpdate[A, B](self: Rep[HashMap[A, B]], key: Rep[A], value: Rep[B])(implicit val manifestA: Manifest[A], val manifestB: Manifest[B]) extends FunctionDef[Unit](Some(self), "g_hash_table_insert", List(List(key, value)))
  def eq[A: Manifest] = doLambda2((x: Rep[A], y: Rep[A]) => unit(true))
  def hash[A: Manifest] = doLambda((x: Rep[A]) => unit(5))
  def apply(n: Def[Any]): Def[Any] = {
    n match {
      case nm @ HashMapNew2()                  => reifyBlock({ GLibNew(eq(nm.manifestA), hash(nm.manifestA))(nm.manifestA, nm.manifestB, manifest[Int]) })
      case HashMapSize(map)                    => GLibSize(map)
      case ma @ HashMapApply(map, key)         => GLibApply(map, key)(ma.manifestA, ma.manifestB)
      case mc @ HashMapContains(map, key)      => GLibContains(map, key)
      case mu @ HashMapUpdate(map, key, value) => GLibUpdate(map, key, value)
      case mr @ HashMapRemove(map, key)        => GLibRemove(map, key)
      case _                                   => n
    }
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
        tt.transformBlock(b, List(t1, t2))
      }
    }.hashMapTestBlock
  }
}
