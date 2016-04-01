package ch.epfl.data
package dblab
package deep
package common

import scala.language.implicitConversions
import sc.pardis.ir._
import sc.pardis.types.PardisTypeImplicits._

/** A polymorphic embedding cake which chains all cakes responsible for further partial evaluation. */
trait BaseInlined extends Base {
  override def infix_!=[A: TypeRep, B: TypeRep](a: Rep[A], b: Rep[B]): Rep[Boolean] = (a, b) match {
    case (Def(d: ConstructorDef[_]), Constant(null)) => unit(true)
    case (Constant(null), Def(d: ConstructorDef[_])) => unit(true)
    case _ => super.infix_!=(a, b)
  }

  override def infix_==[A: TypeRep, B: TypeRep](a: Rep[A], b: Rep[B]): Rep[Boolean] = (a, b) match {
    case (Def(d: ConstructorDef[_]), Constant(null)) => unit(false)
    case (Constant(null), Def(d: ConstructorDef[_])) => unit(false)
    case _ => super.infix_==(a, b)
  }
}
