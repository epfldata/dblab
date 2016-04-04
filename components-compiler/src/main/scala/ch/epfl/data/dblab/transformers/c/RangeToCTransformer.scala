package ch.epfl.data
package dblab
package transformers
package c

import deep._
import sc.pardis.optimization._
import sc.pardis.ir._
import deep.dsls.QueryEngineExp
import sc.pardis.types.PardisTypeImplicits._

/**
 * Transforms Range operations into their C implementation.
 *
 * One of the concrete cases is the following:
 * {{{
 *      for(i <- 0 until n by j) {
 *        foo(i)
 *      }
 * }}}
 * ->
 * {{{
 *      for(int i=0; i<n; i+=j) {
 *        foo(i);
 *      }
 * }}}
 *
 * @param IR the polymorphic embedding trait which contains the reified program.
 */
class RangeToCTransformer(override val IR: QueryEngineExp) extends RecursiveRuleBasedTransformer[QueryEngineExp](IR) with CTransformer {
  import IR._
  import CNodes._
  import CTypes._

  rewrite += remove { case RangeApplyObject(_, _) => () }
  rewrite += remove { case RangeNew(start, end, step) => () }
  rewrite += rule {
    case RangeForeach(self @ Def(RangeApplyObject(start, end)), f) =>
      val i = fresh[Int]
      val body = reifyBlock {
        inlineFunction(f.asInstanceOf[Rep[Int => Unit]], i)
      }
      For(start, end, unit(1), i, body)
  }
  rewrite += rule {
    case RangeForeach(Def(RangeNew(start, end, step)), Def(Lambda(f, i1, o))) =>
      PardisFor(start, end, step, i1.asInstanceOf[Expression[Int]], reifyBlock({ o.asInstanceOf[PardisBlock[Unit]] }))
  }
}
