package ch.epfl.data
package legobase
package deep

import pardis.ir._
import pardis.types.PardisTypeImplicits._
import pardis.deep.scalalib._
import pardis.deep.scalalib.collection._
import pardis.shallow.c.CLangTypes
import pardis.shallow.c.GLibTypes._
import pardis.optimization._

// Additional manual changes needed:
//
//  - Replace self.<init> with __newCHashMap in HashMapNew2 case
//  - Remove the unit(()) at the end of the HashMapNew2 case
//  - Remove (evidence$3, evidence$4) at the end of the HashMapNew2 case
//  - Extend ManualHashMapOptimizations

trait ManualHashMapOptimizations extends RecursiveRuleBasedTransformer[LoweringLegoBase] {
  import IR._

  def shouldUseDirectKeys[K, V](node: HashMapNew[K, V]) = {
    implicit val ktype = transformType(node.typeA).asInstanceOf[TypeRep[K]]
    implicit val vtype = transformType(node.typeB).asInstanceOf[TypeRep[V]]
    typeRep[K].isPrimitive || typeRep[K] == typeRep[String] || typeRep[K] == typeRep[OptimalString]
  }

  object HashMapNew1 {
    def unapply[T: TypeRep](node: Def[T]): Option[Def[T]] = node match {
      case _ => None
    }
  }

  object HashMapNew2 {
    def unapply[T: TypeRep](node: Def[T]): Boolean = node match {
      case n @ HashMapNew() if shouldUseDirectKeys(n) => true
      case _ => false
    }
  }

  rewrite += rule {
    case nm @ HashMapNew3(_, _) =>
      apply(HashMapNew()(nm.typeA, ArrayBufferType(nm.typeB)))
    case nm @ HashMapNew4(_, _) =>
      apply(HashMapNew()(nm.typeA, nm.typeB))
  }

  class K
  class V

  rewrite += rule {
    case node @ HashMapNew() if !shouldUseDirectKeys(node) =>
      implicit val ktype = transformType(node.typeA).asInstanceOf[TypeRep[K]]
      implicit val vtype = transformType(node.typeB).asInstanceOf[TypeRep[V]]
      def hashFunc[T: TypeRep] =
        doLambda[gconstpointer, Int]((s: Rep[gconstpointer]) => {
          infix_hashCode[T](infix_asInstanceOf[T](s))
        })
      def equalsFunc[T: TypeRep] =
        doLambda2[gconstpointer, gconstpointer, Int]((s1: Rep[gconstpointer], s2: Rep[gconstpointer]) => {
          infix_==[T, T](infix_asInstanceOf[T](s1), infix_asInstanceOf[T](s2)).asInstanceOf[Rep[Int]]
        })

      LGHashTableHeader.g_hash_table_new(CLang.&(hashFunc[LPointer[K]]), CLang.&(equalsFunc[LPointer[K]]))
  }
}
