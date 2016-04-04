package ch.epfl.data
package dblab
package transformers
package c

import deep._
import sc.pardis.optimization._
import sc.pardis.ir._
import deep.dsls.QueryEngineExp

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
class BlockFlattening(override val IR: QueryEngineExp) extends LetCommutingConversion[QueryEngineExp](IR) with CTransformer {
  import IR._

  rewrite += statement {
    case sym -> (blk @ Block(stmts, res)) =>
      inlineBlock(blk)(blk.tp)
  }
}
