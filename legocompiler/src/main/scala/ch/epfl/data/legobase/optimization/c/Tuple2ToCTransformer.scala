package ch.epfl.data
package legobase
package optimization
package c

import deep._
import pardis.optimization._
import pardis.ir._

class Tuple2ToCTransformer(override val IR: LoweringLegoBase) extends RecursiveRuleBasedTransformer[LoweringLegoBase](IR) with CTransformer {
  import IR._
  import CNodes._
  import CTypes._

  // rewrite += rule { case Tuple2ApplyObject(_1, _2) => __new(("_1", false, _1), ("_2", false, _2))(Tuple2Type(_1.tp, _2.tp)) }

  // rewrite += rule { case n @ Tuple2_Field__1(self) => field(apply(self), "_1")((n.tp)) }

  // rewrite += rule { case n @ Tuple2_Field__2(self) => field(apply(self), "_2")((n.tp)) }

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
