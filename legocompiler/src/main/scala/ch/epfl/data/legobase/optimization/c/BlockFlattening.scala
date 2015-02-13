package ch.epfl.data
package legobase
package optimization
package c

import deep._
import pardis.optimization._
import pardis.ir._

class BlockFlattening(override val IR: LoweringLegoBase) extends RecursiveRuleBasedTransformer[LoweringLegoBase](IR) with CTransformer {
  import IR._
  import CNodes._
  import CTypes._

  rewrite += statement {
    case sym -> (blk @ Block(stmts, res)) =>
      inlineBlock(blk)(blk.tp)
  }
}
