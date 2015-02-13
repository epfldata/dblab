package ch.epfl.data
package legobase
package optimization
package c

import deep._
import pardis.optimization._
import pardis.ir._
import pardis.types.PardisTypeImplicits._
import pardis.types._
import scala.language.existentials

class ScalaStructToMallocTransformer(override val IR: LoweringLegoBase) extends RuleBasedTransformer[LoweringLegoBase](IR) with CTransformer {
}
