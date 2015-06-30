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
import sc.pardis.shallow.utils.DefaultValue
import sc.pardis.quasi.anf._
import quasi._

/**
 * A transformer which rewrites while loops whenever possible to for expressions.
 *
 * As an example the following program:
 * {{{
 *     var i = 0
 *     while(i < size) {
 *       f(i)
 *       i += 1
 *     }
 * }}}
 * is converted into
 * {{{
 *     for(j <- 0 until size) {
 *       f(j)
 *     }
 * }}}
 *
 * Preconditions:
 * a) There should be no further mutation into the variable `i` in `f(i)`.
 * b) There should be no use of the variable `i` outside of the loop.
 * c) The while loop can only iterate ascendingly.
 * d) The step counter can be every positive number.
 *
 * Additional transformations:
 * As we are using administrative normal form (ANF), we are placing a let-binding for
 * every read of a mutable variable. This means that every read from the variable `i`
 * is associated with a new symbol. These symbols in `f(i)` should be substituted
 * by the range index `j`.
 * Moreover, our system ensures that the programs in ANF will not have any issues
 * with name conflicits. Hence, even if in the inital program another variable is
 * defined with the name `i` in the block of f(i), our engine distinguishes them
 * with different symbols.
 *
 * @param IR the polymorphic embedding trait which contains the reified program.
 */
class WhileToRangeForeachTransformer(override val IR: LoweringLegoBase) extends RuleBasedTransformer[LoweringLegoBase](IR)
  with WhileRangeProcessing {
  import IR.{ Range => _, Binding => _, _ }

  /**
   * Keeps the list of while loops that should be converted
   */
  val convertedWhiles = scala.collection.mutable.ArrayBuffer[WhileInfo]()
  /**
   * Specifies that we are in the transformed while loop body and the iterating variable
   * of the while loop should be substituted by the index of foreach which is given
   */
  val substituteVarInsideLoopBody = scala.collection.mutable.Map[Var[Any], Rep[Int]]()

  def whileShouldBeConverted[T](whileSym: Rep[T]): Boolean =
    convertedWhiles.exists(_.whileSym == whileSym)

  def varCorrespondsToRangeWhile[T](variable: Var[T]): Boolean =
    convertedWhiles.exists(_.variable == variable)

  // TODO precondition (b) is not checked yet.

  analysis += statement {
    case sym -> (node @ dsl"""while(${ RangeCondition(indexVariable, size) }) 
                                $block""") if rangeIndexMutatedOnce(block, indexVariable) &&
      rangeIndexMutatesItselfAtTheEnd(block, indexVariable) =>
      val RangeStep(_, step) = block
      convertedWhiles += WhileInfo(sym.asInstanceOf[Rep[Unit]], node.asInstanceOf[While], indexVariable, size, step)
      ()
  }

  // The order of the application of the following rules does not matter. 

  rewrite += rule {
    case ReadVar(v) if varCorrespondsToRangeWhile(v) && substituteVarInsideLoopBody.contains(v) =>
      substituteVarInsideLoopBody(v)
  }

  rewrite += remove {
    case Assign(v, _) if varCorrespondsToRangeWhile(v) =>
      ()
  }

  rewrite += remove {
    case ReadVar(v) if varCorrespondsToRangeWhile(v) && !substituteVarInsideLoopBody.contains(v) =>
      ()
  }

  rewrite += statement {
    case sym -> dsl"while($cond) $body" if whileShouldBeConverted(sym) =>
      val whileInfo = convertedWhiles.find(_.whileSym == sym).get

      def foreachFunction: Rep[Int => Unit] = {
        // implicitly injects __lambda here
        (i: Rep[Int]) =>
          {
            substituteVarInsideLoopBody += whileInfo.variable -> i
            // Triggers rewriting the statements inside the while loop
            inlineBlock(body)
          }
      }
      dsl"""new Range(${whileInfo.start}, ${whileInfo.size}, ${whileInfo.step}).foreach($foreachFunction)"""
  }
}
