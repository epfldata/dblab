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
import quasi._

/**
 * A transformer which rewrites while loops whenever possible to for expressions.
 *
 * @param IR the polymorphic embedding trait which contains the reified program.
 */
class WhileToRangeForeachTransformer(override val IR: LoweringLegoBase) extends RuleBasedTransformer[LoweringLegoBase](IR) {
  import IR.{ Range => _, _ }

  /**
   * Keeps the information about a while loop
   */
  case class WhileInfo(whileSym: Rep[Unit],
                       whileNode: While,
                       variable: Var[Int],
                       size: Rep[Int],
                       step: Rep[Int]) {
    def start: Rep[Int] = variable match {
      case Var(Def(NewVar(init))) => init
      case _ =>
        throw new Exception(s"No initial value can be computed for the" +
          s" while loop with the following info:\n$this")
    }
  }

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

  def varShouldBeRemoved[T](variable: Var[T]): Boolean =
    convertedWhiles.exists(_.variable == variable)

  /**
   * Extracts the condition part of a while loop
   */
  object RangeCondition {
    def unapply(block: Block[Boolean]): Option[(Var[Int], Rep[Int])] = {
      val resultNode = block.stmts.find(stm => stm.sym == block.res).get.rhs
      resultNode match {
        case dsl"true && $block2" => RangeCondition.unapply(block2)
        // TODO needs having readVar in shallow or some other mechanism to detect vars
        case dsl"(${ Def(ReadVar(v)) }: Int) < ($size: Int)" => Some(v -> size)
        case _ => None
      }
    }
  }

  /**
   * Extracts the stepping part of a while loop
   */
  object RangeStep {
    def unapply(block: Block[Unit]): Option[(Var[Int], Rep[Int])] = block.stmts.last.rhs match {
      case Assign(v, Def(Int$plus2(Def(ReadVar(v2)), step))) if v == v2 => Some(v2 -> step)
      case _ => None
    }
  }

  analysis += statement {
    case sym -> (node @ dsl"""while(${ RangeCondition(variable1, size) }) 
                                ${ RangeStep(variable2, step) }""") if variable1 == variable2 =>
      convertedWhiles += WhileInfo(sym.asInstanceOf[Rep[Unit]], node.asInstanceOf[While], variable1, size, step)
      ()
  }

  rewrite += remove {
    case ReadVar(v) if varShouldBeRemoved(v) && !substituteVarInsideLoopBody.contains(v) =>
      ()
  }

  rewrite += rule {
    case ReadVar(v) if varShouldBeRemoved(v) && substituteVarInsideLoopBody.contains(v) =>
      substituteVarInsideLoopBody(v)
  }

  rewrite += remove {
    case Assign(v, _) if varShouldBeRemoved(v) =>
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
            body.stmts.foreach(transformStm)
            unit(())
          }
      }
      dsl"""Range(${whileInfo.start}, ${whileInfo.size}).foreach($foreachFunction)"""
  }
}
