package ch.epfl.data
package dblab
package deep
package common

import ch.epfl.data.sc.pardis
import pardis.ir._
import pardis.types.PardisTypeImplicits._
import pardis.effects._
import pardis.deep.scalalib.collection.TreeSetComponent

/** A polymorphic embedding cake containing additional manually lifted methods for Array */
trait TreeSetExtraOps extends TreeSetComponent {
  // TODO implicit parameters should be handled in a correct way to remove this manually lifted one
  def __newTreeSet2[A](ordering: Rep[Ordering[A]])(implicit typeA: TypeRep[A]): Rep[TreeSet[A]] = TreeSetNew2[A](ordering)(typeA)
  // case classes
  case class TreeSetNew2[A](val ordering: Rep[Ordering[A]])(implicit val typeA: TypeRep[A]) extends FunctionDef[TreeSet[A]](None, "new TreeSet", List(Nil, List(ordering))) {
    override def curriedConstructor = (copy[A] _)
  }
}
