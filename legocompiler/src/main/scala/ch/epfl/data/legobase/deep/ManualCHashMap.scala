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
//  - Replace self.<init> with __newCHashMap[K, V] in HashMapNew2 case
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

  case class HashMapNew1[A, B](gHashTable: Rep[LPointer[LGHashTable]])(implicit val typeA: TypeRep[A], val typeB: TypeRep[B]) extends ConstructorDef[HashMap[A, B]](List(typeA, typeB), "HashMap", List(List())) {
    override def curriedConstructor = (x: Any) => copy[A, B]()
  }

  object HashMapNew2 {
    def unapply[T: TypeRep](node: Def[T]): Boolean = node match {
      case n @ HashMapNew() if shouldUseDirectKeys(n) => true
      case _ => false
    }
  }

  rewrite += statement {
    case sym -> (nm @ HashMapNew3(_, _)) =>
      __newCHashMap()(nm.typeA, ArrayBufferType(nm.typeB), sym)
    case sym -> (nm @ HashMapNew4(_, _)) =>
      __newCHashMap()(nm.typeA, nm.typeB, sym)
  }

  class K
  class V

  def __newCHashMap[K, V]()(implicit typeK: TypeRep[K], typeV: TypeRep[V], sym: Sym[Any]): Rep[HashMap[K, V]]
  def __newCHashMap[K, V](gHashTable: Rep[LPointer[LGHashTable]])(implicit typeK: TypeRep[K], typeV: TypeRep[V], sym: Sym[Any]): Rep[HashMap[K, V]]

  rewrite += statement {
    case sym -> (node @ HashMapNew()) if !shouldUseDirectKeys(node) =>
      System.out.println(s"1: HashMapNew for symbol $sym")
      implicit val ktype = transformType(node.typeA).asInstanceOf[TypeRep[K]]
      implicit val vtype = transformType(node.typeB).asInstanceOf[TypeRep[V]]
      implicit val symany = sym.asInstanceOf[Sym[Any]]
      def hashFunc[T: TypeRep] =
        doLambda[gconstpointer, Int]((s: Rep[gconstpointer]) => {
          infix_hashCode[T](infix_asInstanceOf[T](s))
        })
      def equalsFunc[T: TypeRep] =
        doLambda2[gconstpointer, gconstpointer, Int]((s1: Rep[gconstpointer], s2: Rep[gconstpointer]) => {
          infix_==[T, T](infix_asInstanceOf[T](s1), infix_asInstanceOf[T](s2)).asInstanceOf[Rep[Int]]
        })

      __newCHashMap[K, V](LGHashTableHeader.g_hash_table_new(CLang.&(hashFunc[LPointer[K]]), CLang.&(equalsFunc[LPointer[K]])))
  }
}

// Additional manual changes needed:
//  - replace SetNew with SetNew2
trait ManualSet extends RecursiveRuleBasedTransformer[LoweringLegoBase] {
  import IR._

  /*object SetNew {
    def unapply[T: TypeRep](node: Def[T]): Option[Def[T]] = node match {
      case _ => None
    }
  }*/
}

// Additional manual changes needed:
//  - extend ManualOptimalString
trait ManualOptimalString extends RecursiveRuleBasedTransformer[LoweringLegoBase] {
  import IR._

  def OptimalStringNew(charArray: Rep[LPointer[Char]]): Rep[OptimalString] = new OptimalStringNew(charArray.asInstanceOf[Rep[Array[Byte]]])
}
