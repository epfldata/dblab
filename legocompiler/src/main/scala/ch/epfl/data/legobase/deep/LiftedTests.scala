package ch.epfl.data
package legobase
package deep

import scala.collection.mutable.ArrayBuffer
import ch.epfl.data.pardis.ir._
import ch.epfl.data.legobase.deep.scalalib._

class SerialTransformer(val ts: List[PardisNode[Any] => PardisNode[Any]]) {
  def printBlock(b: PardisBlock[_]) =
    b.stmts.foreach(st => println(st))

  def transformStmt(st: Statement[_]): Statement[_] =
    Statement(st.sym, ts(1)(ts(0)(st.rhs))) //.foldLeft(st.rhs)((n, t) => t(n)))

  def transformBlock(b: PardisBlock[_]): PardisBlock[_] =
    PardisBlock(b.stmts.map(st => transformStmt(st)), b.res)
}

/* This class is the manually completely lifted version of Query1 which ideally should be lifted by YY */
class GHashTable
class LiftedTests extends Base {
  def hashMapTest() = {
    new HashMapOps with DeepDSL {
      def hashMapTest = {
        val hm = hashMapNew2(manifest[Int], manifest[String])
        val f = hashMapGetOrElseUpdate(hm, 5, { unit("lala") })
        println(f)
        val g = hashMapGetOrElseUpdate(hm, 3, { unit("foo") })
        println(f)
        printf(unit("%d\n"), hm.size)
        hashMapRemove(hm, 3)
        printf(unit("%d\n"), hm.size)
      }

      // Subtransformers
      def t1(a: Def[Any]): Def[Any] = {
        a match {
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
          case _ => a
        }
      }

      def t2(a: Def[Any]): Def[Any] = {
        case class GLibNew[A, B, C](eq: Rep[(A, A) => Boolean], hash: Rep[A => C])(implicit manifestA: Manifest[A], manifestB: Manifest[B], manifestC: Manifest[C]) extends FunctionDef[GHashTable](None, "g_hash_table_new", List(List(eq, hash)))
        case class GLibApply[A, B](self: Rep[HashMap[A, B]], key: Rep[A])(implicit manifestA: Manifest[A], manifestB: Manifest[B]) extends FunctionDef[B](Some(self), "g_hash_table_apply", List(List(key)))
        case class GLibContains[A, B](self: Rep[HashMap[A, B]], key: Rep[A])(implicit manifestA: Manifest[A], manifestB: Manifest[B]) extends FunctionDef[B](Some(self), "g_hash_table_contains", List(List(key)))
        def eq[A: Manifest] = doLambda2((x: Rep[A], y: Rep[A]) => unit(true))
        def hash[A: Manifest] = doLambda((x: Rep[A]) => unit(5))
        a match {
          case nm @ HashMapNew2()        => reifyBlock({ GLibNew(eq(nm.manifestA), hash(nm.manifestA))(nm.manifestA, nm.manifestB, manifest[Int]) })
          case HashMapApply(map, key)    => GLibApply(map, key)
          case HashMapContains(map, key) => GLibContains(map, key)
          case _                         => a
        }
      }

      def hashMapTestBlock = {
        val b = reifyBlock(hashMapTest)
        val tt = new SerialTransformer(List(t1, t2))
        tt.transformBlock(b)
      }
    }.hashMapTestBlock
  }
}
