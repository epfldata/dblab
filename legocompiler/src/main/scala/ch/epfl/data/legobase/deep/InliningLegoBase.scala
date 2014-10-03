package ch.epfl.data
package legobase
package deep

import scala.language.implicitConversions
import pardis.ir._
import pardis.types.PardisTypeImplicits._

// trait InliningLegoBase extends volcano.InliningVolcano with DeepDSL with pardis.ir.InlineFunctions with QueriesImplementations with LoopUnrolling with InliningLoader
trait InliningLegoBase extends push.InliningPush with DeepDSL with pardis.ir.InlineFunctions with QueriesImplementations with LoopUnrolling with InliningLoader

trait InliningLoader extends LoaderImplementations { this: InliningLegoBase =>
  override def loaderGetFullPathObject(fileName: Rep[String]): Rep[String] = fileName match {
    case Constant(name: String) => unit(Config.datapath + name)
    case _                      => throw new Exception(s"file name should be constant but here it is $fileName")
  }
}

trait LoopUnrolling extends pardis.ir.InlineFunctions { this: InliningLegoBase =>
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

