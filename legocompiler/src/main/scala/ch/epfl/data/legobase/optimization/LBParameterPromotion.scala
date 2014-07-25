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
    rhs match {
      // TODO should be changed to an automatic version using escape analysis
      case _ if List(classOf[AggOp[_, _]], classOf[PrintOp[_]], classOf[ScanOp[_]], classOf[MapOp[_]], classOf[SelectOp[_]], classOf[SortOp[_]]).contains(rhs.tp.runtimeClass) => promotedObjects += sym
      case _ => ()
    }
  }
}
