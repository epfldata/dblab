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

/**
 * Common interface for assertions
 */
sealed trait Assertion

/**
 * An assertion for checking a predicate on types
 *
 * @param predicate the predicate function on a type
 */
case class TypeAssertion(predicate: PardisType[Any] => Boolean) extends Assertion {
  /**
   * Checks if the given type satisifes the predicate function or not
   *
   * @param tp the given type which the predicate should be checked for that
   * @returns whether the predicate is true for the given input type or not
   */
  def satisfies[T2](tp: PardisType[T2]): Boolean = predicate(tp.asInstanceOf[PardisType[Any]])
}

/**
 * Factory for [[AssertTransformer]] class.
 */
object AssertTransformer {
  /**
   * Given several assertions creates a transforming to check if an input program satisfies
   * these assertions or not.
   *
   * @param assertions list of input assertions
   * @returns a new instance of [[AssertTransformer]].
   */
  def apply(assertions: Assertion*) = new TransformerHandler {
    def apply[Lang <: Base, T: PardisType](context: Lang)(block: context.Block[T]): context.Block[T] = {
      new AssertTransformer(context, assertions.toList).optimize(block)
    }
  }
}

/**
 * A transformer which checks the input program to satisfy all the given assertions. This transformer
 * does not change the input program.
 *
 * @param IR the polymorphic embedding trait which contains the reified program.
 * @param assertions the list of assertions which should be checked
 */
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
