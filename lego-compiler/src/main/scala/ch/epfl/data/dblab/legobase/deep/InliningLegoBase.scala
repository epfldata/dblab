package ch.epfl.data
package dblab.legobase
package deep

import scala.language.implicitConversions
import sc.pardis.ir._
import sc.pardis.types.PardisTypeImplicits._

// TODO volcano cakes should be added whenever we have support for both volcano and push engines
// trait InliningLegoBase extends volcano.InliningVolcano with DeepDSL with sc.pardis.ir.InlineFunctions with tpch.QueriesImplementations with LoopUnrolling with schema.InliningLoader
/** A polymorphic embedding cake which chains all cakes responsible for further partial evaluation. */
trait InliningLegoBase extends queryengine.push.InliningPush with DeepDSL with sc.pardis.ir.InlineFunctions with tpch.QueriesImplementations with LoopUnrolling with storagemanager.InliningLoader with tpch.InliningTPCHLoader

/** A polymorphic embedding cake for performing loop unrolling in the case of using var args. */
trait LoopUnrolling extends sc.pardis.ir.InlineFunctions { this: InliningLegoBase =>
  override def seqForeach[A, U](self: Rep[Seq[A]], f: Rep[A => U])(implicit typeA: TypeRep[A], typeU: TypeRep[U]): Rep[Unit] = self match {
    case Def(LiftedSeq(elems)) => elems.toList match {
      case Nil => unit(())
      case elem :: tail => {
        __app(f).apply(elem)
        __liftSeq(tail).foreach(f)
      }
    }
  }
}
