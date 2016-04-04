package ch.epfl.data
package dblab
package transformers
package c

import deep._
import sc.pardis.optimization._
import sc.pardis.ir._
import deep.dsls.QueryEngineExp
import sc.pardis.types.PardisTypeImplicits._

// TODO add an anlysis phase to check the soundness of this optimization
/**
 * Removes the abstraction overhead of Options.
 *
 * @param IR the polymorphic embedding trait which contains the reified program.
 */
class OptionToCTransformer(override val IR: QueryEngineExp) extends RecursiveRuleBasedTransformer[QueryEngineExp](IR) with CTransformer {
  import IR._
  import CNodes._
  import CTypes._

  rewrite += rule { case OptionApplyObject(x) => x }

  rewrite += rule { case OptionGet(x) => x }

  rewrite += rule { case OptionNonEmpty(x) => infix_!=(x, unit(null)) }

}
