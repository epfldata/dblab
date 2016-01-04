package ch.epfl.data
package dblab.legobase
package optimization
package c

import deep._
import sc.pardis.optimization._
import sc.pardis.ir._

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
class BlockFlattening(override val IR: LegoBaseExp) extends LetCommutingConversion[LegoBaseExp](IR) with CTransformer {
  import IR._

  rewrite += statement {
    case sym -> (blk @ Block(stmts, res)) =>
      inlineBlock(blk)(blk.tp)
  }
}
