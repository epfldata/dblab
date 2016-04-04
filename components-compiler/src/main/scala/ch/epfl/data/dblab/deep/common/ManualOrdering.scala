package ch.epfl.data
package dblab
package deep
package common

import ch.epfl.data.sc.pardis
import pardis.ir._
import pardis.types.PardisTypeImplicits._
import pardis.effects._
import pardis.utils.TypeUtils.pardisTypeToString

// TODO revisit for moving a more generic version to SC
/** This deep class is manually created */
trait OrderingOps extends Base {
  object Ordering {
    def apply[T: TypeRep](comp: Rep[Function2[T, T, Int]]): Rep[Ordering[T]] = OrderingNew(comp)
  }
  case class OrderingNew[T: TypeRep](comp: Rep[Function2[T, T, Int]]) extends FunctionDef[Ordering[T]](None, "OrderingFactory", List(List(comp))) {
    override def curriedConstructor = copy[T] _
  }
  object OrderingRep {
    def apply[T: TypeRep]: Rep[Ordering[T]] = OrderingNew2[T]()
  }
  case class OrderingNew2[T]()(implicit typeT: TypeRep[T]) extends FunctionDef[Ordering[T]](None, s"Ordering[${pardisTypeToString(typeT)}]", Nil) {
    // override def curriedConstructor = copy[T] _
    override def rebuild(children: PardisFunArg*) = OrderingNew2[T]()
    override def isPure = true
  }

  implicit class OrderingOps[T: TypeRep](cmp: Rep[Ordering[T]]) {
    def lt(lhs: Rep[T], rhs: Rep[T]) = ordering_lt(lhs, rhs)
  }
  def ordering_lt[T: TypeRep](lhs: Rep[T], rhs: Rep[T]): Rep[Boolean] = OrderingLT(lhs, rhs)
  case class OrderingLT[T: TypeRep](lhs: Rep[T], rhs: Rep[T]) extends FunctionDef[Boolean](Some(lhs), s"<", List(List(rhs))) {
    override def curriedConstructor = (copy[T] _).curried
  }
}
