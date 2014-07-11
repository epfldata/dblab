package ch.epfl.data
package legobase
package deep

import scala.language.implicitConversions
import pardis.ir._

trait Lowering extends TopDownTransformer[InliningLegoBase, LoweringLegoBase] {
  import from._

  override def transformDef[T: Manifest](node: Def[T]): to.Def[T] = node match {
    // case an: AggOpNew[_, _] => {
    //   val ma = an.manifestA
    //   val mb = an.manifestB
    //   val marrDouble = manifest[Array[Double]]
    //   to.reifyBlock({
    //     to.__new[AggOp[_, _]](("hm", false, to.__newHashMap(to.overloaded2, mb, marrDouble)),
    //       ("NullDynamicRecord", false, unit[Any](null)(ma.asInstanceOf[Manifest[Any]])),
    //       ("keySet", true, to.Set()(mb))).asInstanceOf[to.Rep[T]]
    //   }).correspondingNode
    // }
    case po: PrintOpNew[_] => {
      to.reifyBlock({
        to.__new[PrintOp[_]](("numRows", true, to.unit[Int](0)))
      }).correspondingNode.asInstanceOf[to.Def[T]]
    }
    case _ => super.transformDef(node)
  }

  // override def transformStmToMultiple(stm: Stm[_]): List[to.Stm[_]] = stm match {
  //   case Stm(sym, an: AggOpNew[_, _]) => {
  //     val ma = an.manifestA
  //     val mb = an.manifestB
  //     val marrDouble = manifest[Array[Double]]
  //     val hm = to.__newHashMap(to.overloaded2, mb, marrDouble)
  //     val NullDynamicRecord = unit[Any](null)(ma.asInstanceOf[Manifest[Any]])
  //     val keySet = reifyBlock {
  //       __newVar(to.Set[Any]()(mb.asInstanceOf[Manifest[Any]]))
  //     }
  //     List(statementFromExp(hm), statementFromExp(keySet))
  //   }
  //   case _ =>
  //     super.transformStmToMultiple(stm)
  // }

  // def statementFromExp(exp: Rep[_]): Stm[_] = exp match {
  //   case s @ Sym(_) => Stm(s, s.correspondingNode)
  //   // case c @ Constant(v) => Stm(fresh(c.tp), c)
  // }
}

trait LoweringLegoBase extends InliningLegoBase with pardis.ir.InlineFunctions {
}
