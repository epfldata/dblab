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
      case AggOpType(_, _) | PrintOpType(_) | ScanOpType(_) | MapOpType(_) | SelectOpType(_) | SortOpType(_) | HashJoinOpType(_, _, _) | WindowOpType(_, _, _) | LeftHashSemiJoinOpType(_, _, _) | NestedLoopsJoinOpType(_, _) | ViewOpType(_) | HashJoinAntiType(_, _, _) => promotedObjects += sym
      case _ => ()
    }
  }

  override def transformDef[T: TypeRep](node: Def[T]): to.Def[T] = node match {
    case _ => super.transformDef(node)
  }
}
