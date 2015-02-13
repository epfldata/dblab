package ch.epfl.data
package legobase
package optimization
package c

import deep._
import pardis.optimization._
import pardis.ir._
import pardis.types.PardisTypeImplicits._

class OptionToCTransformer(override val IR: LoweringLegoBase) extends RecursiveRuleBasedTransformer[LoweringLegoBase](IR) with CTransformer {
  import IR._
  import CNodes._
  import CTypes._

  rewrite += rule { case OptionApplyObject(x) => x }

  rewrite += rule { case OptionGet(x) => x }

  rewrite += rule { case OptionNonEmpty(x) => infix_!=(x, unit(null)) }

}
