package ch.epfl.data
package dblab.legobase
package optimization
package c

import deep._
import sc.pardis.optimization._
import sc.pardis.ir._
import sc.pardis.types.PardisTypeImplicits._

/**
 * Removes the abstraction overhead of Options.
 *
 * @param IR the polymorphic embedding trait which contains the reified program.
 */
class OptionToCTransformer(override val IR: LoweringLegoBase) extends RecursiveRuleBasedTransformer[LoweringLegoBase](IR) with CTransformer {
  import IR._
  import CNodes._
  import CTypes._

  rewrite += rule { case OptionApplyObject(x) => x }

  rewrite += rule { case OptionGet(x) => x }

  rewrite += rule { case OptionNonEmpty(x) => infix_!=(x, unit(null)) }

}
