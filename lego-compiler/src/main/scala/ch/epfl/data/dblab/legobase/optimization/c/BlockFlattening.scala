package ch.epfl.data
package dblab.legobase
package optimization
package c

import deep._
import sc.pardis.optimization._
import sc.pardis.ir._

// TODO should we move it to SC?
/**
 * Flattens the nested block into a single block whenever possible.
 *
 * As an example the following program:
 * {{{
 *     val block1 = {
 *       val x = {
 *         y
 *       }
 *     }
 * }}}
 * is converted into
 * {{{
 *     val block1 = y
 * }}}
 *
 * @param IR the polymorphic embedding trait which contains the reified program.
 */
class BlockFlattening(override val IR: LoweringLegoBase) extends RecursiveRuleBasedTransformer[LoweringLegoBase](IR) with CTransformer {
  import IR._
  import CNodes._
  import CTypes._

  rewrite += statement {
    case sym -> (blk @ Block(stmts, res)) =>
      inlineBlock(blk)(blk.tp)
  }
}
