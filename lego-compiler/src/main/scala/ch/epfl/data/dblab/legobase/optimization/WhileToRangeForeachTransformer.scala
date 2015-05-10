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

/**
 * A transformer which rewrites while loops whenever possible to for expressions.
 *
 * @param IR the polymorphic embedding trait which contains the reified program.
 */
class WhileToRangeForeachTransformer(override val IR: LoweringLegoBase) extends RuleBasedTransformer[LoweringLegoBase](IR) {
  import IR._

  object RangeCondition {
    def unapply(block: Block[Boolean]): Option[(Var[Int], Rep[Int])] = {
      val resultNode = block.stmts.find(stm => stm.sym == block.res).get.rhs
      resultNode match {
        case Boolean$amp$amp(Constant(true), block2) => RangeCondition.unapply(block2)
        case Int$less1(Def(ReadVar(v)), size)        => Some(v -> size)
        case _                                       => None
      }
    }
  }

  object RangeStep {
    def unapply(block: Block[Unit]): Option[(Var[Int], Rep[Int])] = block.stmts.last.rhs match {
      case Assign(v, Def(Int$plus2(Def(ReadVar(v2)), step))) if v == v2 => Some(v2 -> step)
      case _ => None
    }
  }

  // def isConditionForCheckingSize(block: Block[Boolean]): Boolean = {
  //   val resultNode = block.stmts.find(stm => stm.sym == block.res).get.rhs
  //   resultNode match {
  //     case Boolean$amp$amp(Constant(true), block2) => isConditionForCheckingSize(block2)
  //     case Int$less3(Def(ReadVar(v)), size)        => true
  //     case _                                       => false
  //   }
  // }

  case class WhileInfo(whileSym: Rep[Unit], whileNode: While, variable: Var[Int], size: Rep[Int], step: Rep[Int])

  val convertedWhiles = scala.collection.mutable.ArrayBuffer[WhileInfo]()
  val startConds = scala.collection.mutable.Map[Var[Int], Rep[Int]]()
  val fillingPhase = scala.collection.mutable.Map[Var[Any], Rep[Int]]()
  def shouldBeConverted[T](whileSym: Rep[T]): Boolean = convertedWhiles.exists(_.whileSym == whileSym)
  def varShouldBeRemoved[T](variable: Var[T]): Boolean = convertedWhiles.exists(_.variable == variable)

  analysis += statement {
    case sym -> (node @ While(RangeCondition(variable1, size), RangeStep(variable2, step))) if variable1 == variable2 =>
      // val isForEach = isConditionForCheckingSize(cond)
      convertedWhiles += WhileInfo(sym.asInstanceOf[Rep[Unit]], node, variable1, size, step)
      // System.out.println(s"$sym -> $variable1, $size")
      ()
  }

  rewrite += remove {
    case ReadVar(v) if varShouldBeRemoved(v) && !fillingPhase.contains(v) =>
      ()
  }

  rewrite += rule {
    case ReadVar(v) if varShouldBeRemoved(v) && fillingPhase.contains(v) =>
      fillingPhase(v)
  }

  rewrite += remove {
    case Assign(v, _) if varShouldBeRemoved(v) =>
      ()
  }

  analysis += statement {
    case sym -> NewVar(e) =>
      startConds += Var(sym.asInstanceOf[Rep[Var[Int]]]) -> e.asInstanceOf[Rep[Int]]
      ()
  }

  // rewrite += remove {
  //   case node @ NewVar(e) if convertedWhiles.exists(_.variable.e.correspondingNode == node) =>
  //     // startConds += Var(sym.asInstanceOf[Rep[Var[Int]]]) -> e.asInstanceOf[Rep[Int]]
  //     // val flag = varShouldBeRemoved(Var(sym.asInstanceOf[Rep[Var[Any]]]))
  //     System.out.println(s"var sym: $node -> ")
  //     ()
  // }

  rewrite += statement {
    case sym -> While(cond, body) if shouldBeConverted(sym) =>
      val whileInfo = convertedWhiles.find(_.whileSym == sym).get
      // System.out.println(s"startCond: $startConds")
      // System.out.println(s"whileInfo: $whileInfo")
      val start = startConds(whileInfo.variable)
      // we assume step = 1
      Range(start, whileInfo.size).foreach {
        __lambda { (i: Rep[Int]) =>
          fillingPhase += whileInfo.variable -> i
          body.stmts.foreach(transformStm)
          unit(())
        }
      }
  }
}