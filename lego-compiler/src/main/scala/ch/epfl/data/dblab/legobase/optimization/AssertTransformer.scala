package ch.epfl.data
package dblab.legobase
package optimization

import scala.language.implicitConversions
import sc.pardis.ir._
import reflect.runtime.universe.{ TypeTag, Type }
import sc.pardis.optimization._
import deep._
import sc.pardis.types._
import sc.pardis.types.PardisTypeImplicits._
import sc.pardis.deep.scalalib.collection._

sealed trait Assertion
case class TypeAssertion(p: PardisType[Any] => Boolean) extends Assertion {
  def satisfies[T2](tp: PardisType[T2]): Boolean = p(tp.asInstanceOf[PardisType[Any]])
}

object AssertTransformer {
  def apply(assertions: Assertion*) = new TransformerHandler {
    def apply[Lang <: Base, T: PardisType](context: Lang)(block: context.Block[T]): context.Block[T] = {
      new AssertTransformer(context, assertions.toList).optimize(block)
    }
  }
}

class AssertTransformer[Lang <: Base](override val IR: Lang, val assertions: List[Assertion]) extends sc.pardis.optimization.RecursiveRuleBasedTransformer[Lang](IR) {
  import IR._

  for (tp <- assertions.collect({ case tp: TypeAssertion => tp })) {
    analysis += statement {
      case sym -> node if !tp.satisfies(sym.tp) ||
        node.funArgs.exists(fa =>
          fa.isInstanceOf[Rep[_]] && !tp.satisfies(fa.asInstanceOf[Rep[_]].tp)) =>
        val types = sym.tp :: node.funArgs.collect {
          case fa: Rep[_] =>
            fa.tp
        }
        throw new Exception(s"sym $sym with node $node violates the assertion of type $tp\ntypes: ${types.mkString(",")}")
    }
  }
}
