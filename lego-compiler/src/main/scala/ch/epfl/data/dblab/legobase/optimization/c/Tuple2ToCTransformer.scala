package ch.epfl.data
package dblab.legobase
package optimization
package c

import deep._
import sc.pardis.optimization._
import sc.pardis.ir._

// TODO add an anlysis phase to check the soundness of this optimization
/**
 * Removes the abstraction overhead of Tuple2.
 *
 * @param IR the polymorphic embedding trait which contains the reified program.
 */
class Tuple2ToCTransformer(override val IR: LegoBaseExp) extends RecursiveRuleBasedTransformer[LegoBaseExp](IR) with CTransformer {
  import IR._
  import CNodes._
  import CTypes._

  object Tuple2Create {
    def unapply[T](d: Def[T]): Option[(Rep[Any], Rep[Any])] = d match {
      case Tuple2ApplyObject(_1, _2) => Some(_1 -> _2)
      case Tuple2New(_1, _2)         => Some(_1 -> _2)
      case _                         => None
    }
  }

  rewrite += remove { case Tuple2Create(_1, _2) => () }

  rewrite += rule { case n @ Tuple2_Field__1(Def(Tuple2Create(_1, _2))) => _1 }

  rewrite += rule { case n @ Tuple2_Field__2(Def(Tuple2Create(_1, _2))) => _2 }

}
