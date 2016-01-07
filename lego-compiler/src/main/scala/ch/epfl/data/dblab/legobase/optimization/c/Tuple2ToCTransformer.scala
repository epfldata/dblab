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

  rewrite += remove { case Tuple2New(_1, _2) => () }

  rewrite += rule { case n @ Tuple2_Field__1(Def(Tuple2New(_1, _2))) => _1 }

  rewrite += rule { case n @ Tuple2_Field__2(Def(Tuple2New(_1, _2))) => _2 }

}
