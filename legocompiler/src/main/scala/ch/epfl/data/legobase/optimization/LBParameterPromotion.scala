package ch.epfl.data
package legobase
package optimization

import deep._
import scala.language.implicitConversions
import pardis.ir._
import pardis.optimization._

class LBParameterPromotion(override val IR: LoweringLegoBase) extends ParameterPromotion[LoweringLegoBase](IR) {
  import IR._

  // override def optimize[T: Manifest](node: Block[T]): Block[T] = {
  //   val res = super.optimize[T](node)
  //   System.out.println(promotedObjects.map(x => x -> x.correspondingNode).mkString("\n"))
  //   res
  // }
  def escapeAnalysis[T](sym: Sym[T], rhs: Def[T]): Unit = {
    rhs match {
      // TODO should be changed to an automatic version using escape analysis
      case _ if List(classOf[AggOp[_, _]], classOf[PrintOp[_]], classOf[ScanOp[_]], classOf[MapOp[_]], classOf[SelectOp[_]], classOf[SortOp[_]], classOf[HashJoinOp[_, _, _]], classOf[WindowOp[_, _, _]]).contains(rhs.tp.runtimeClass) => promotedObjects += sym
      case _ => ()
    }
  }

  override def transformDef[T: Manifest](node: Def[T]): to.Def[T] = node match {
    // case pc @ PardisCast(exp) => {
    //   System.out.print("--->")
    //   System.out.print(pc)
    //   System.out.print("<---")
    //   System.out.println(transformType(pc.castTp))
    //   PardisCast(transformExp[Any, Any](exp))(transformType(exp.tp), transformType(pc.castTp)).asInstanceOf[to.Def[T]]
    // }
    case _ => super.transformDef(node)
  }
}
