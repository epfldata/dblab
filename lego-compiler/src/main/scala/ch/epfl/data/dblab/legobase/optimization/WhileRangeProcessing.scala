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
 * An interface containing appropriate methods and data structures for processing
 * while loops which are iterating over a range
 */
trait WhileRangeProcessing {
  val IR: LoweringLegoBase
  import IR.{ Range => _, _ }

  private implicit val ImplicitIR = IR

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
   * Extracts the condition part of a while loop, returning the index variable and
   * its upper bound.
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
   * Specifies if the variable `indexVar` is mutated only once in the given block
   */
  def rangeIndexMutatedOnce(block: Block[Unit], indexVar: Var[Int]): Boolean =
    block.stmts.collect({
      case Stm(_, Assign(v, _)) if v == indexVar =>
        v
    }).size == 1

  /**
   * Specifies if the variable `indexVar` mutates itself at the end of the given
   * block by a constant number
   */
  def rangeIndexMutatesItselfAtTheEnd(block: Block[Unit], indexVar: Var[Int]): Boolean =
    RangeStep.unapply(block).exists(_._1 == indexVar)

  /**
   * Extracts the stepping part of a while loop
   */
  object RangeStep {
    def unapply(block: Block[Unit]): Option[(Var[Int], Rep[Int])] = block.stmts.last.rhs match {
      case Assign(v, Def(Int$plus2(Def(ReadVar(v2)), step))) if v == v2 => Some(v2 -> step)
      case _ => None
    }
  }
}
