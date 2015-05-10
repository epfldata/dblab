package ch.epfl.data
package dblab.legobase
package deep
package queryengine
package push

import scala.language.implicitConversions
import sc.pardis.ir._
import sc.pardis.types.PardisTypeImplicits._
import scala.reflect._

/**
 * A polymorphic embedding cake responsible for applying further partial evaluation.
 */
trait InliningPush extends DeepDSL with sc.pardis.ir.InlineFunctions with tpch.QueriesImplementations with OperatorImplementations with ScanOpImplementations with SelectOpImplementations with AggOpImplementations with SortOpImplementations with MapOpImplementations with PrintOpImplementations with WindowOpImplementations with HashJoinOpImplementations with LeftHashSemiJoinOpImplementations with NestedLoopsJoinOpImplementations with SubquerySingleResultImplementations with ViewOpImplementations with HashJoinAntiImplementations with LeftOuterJoinOpImplementations
  with OperatorPartialEvaluation with ScanOpPartialEvaluation with SelectOpPartialEvaluation with AggOpPartialEvaluation with SortOpPartialEvaluation with MapOpPartialEvaluation with PrintOpPartialEvaluation with WindowOpPartialEvaluation with HashJoinOpPartialEvaluation with LeftHashSemiJoinOpPartialEvaluation with NestedLoopsJoinOpPartialEvaluation with SubquerySingleResultPartialEvaluation with ViewOpPartialEvaluation with HashJoinAntiPartialEvaluation with LeftOuterJoinOpPartialEvaluation
  with sc.pardis.deep.scalalib.Tuple2PartialEvaluation
  with OperatorDynamicDispatch with BaseOptimization with sc.pardis.deep.scalalib.ArrayOptimization { this: InliningLegoBase =>
  override def printOp_Field_PrintQueryOutput[A](self: Rep[PrintOp[A]])(implicit typeA: TypeRep[A]): Rep[Boolean] = unit(Config.printQueryOutput)
  override def hashJoinOpNew2[A <: ch.epfl.data.sc.pardis.shallow.Record, B <: ch.epfl.data.sc.pardis.shallow.Record, C](leftParent: Rep[Operator[A]], rightParent: Rep[Operator[B]], joinCond: Rep[((A, B) => Boolean)], leftHash: Rep[((A) => C)], rightHash: Rep[((B) => C)])(implicit typeA: TypeRep[A], typeB: TypeRep[B], typeC: TypeRep[C]): Rep[HashJoinOp[A, B, C]] = hashJoinOpNew1[A, B, C](leftParent, rightParent, unit(""), unit(""), joinCond, leftHash, rightHash)
}
