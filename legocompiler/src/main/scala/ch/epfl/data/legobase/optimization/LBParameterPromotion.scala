package ch.epfl.data
package legobase
package optimization

import deep._
import scala.language.implicitConversions
import pardis.ir._
import pardis.optimization._

class LBParameterPromotion(override val IR: LoweringLegoBase) extends ParameterPromotion[LoweringLegoBase](IR) {
  import IR._
  def escapeAnalysis[T](sym: Sym[T], rhs: Def[T]): Unit = {
    rhs.tp match {
      // TODO should be changed to an automatic version using escape analysis
      case AggOpType(_, _) | PrintOpType(_) | ScanOpType(_) | MapOpType(_) | SelectOpType(_) | SortOpType(_) => promotedObjects += sym
      case _ => ()
    }
  }
}
