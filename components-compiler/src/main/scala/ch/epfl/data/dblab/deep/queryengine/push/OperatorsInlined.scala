package ch.epfl.data
package dblab
package deep
package queryengine
package push

import scala.language.implicitConversions
import sc.pardis.ir._
import sc.pardis.types.PardisTypeImplicits._
import scala.reflect._
import deep.dsls._

/**
 * A polymorphic embedding cake responsible for applying further partial evaluation for push-based physical query plan.
 */
trait OperatorsInlined extends queryengine.push.OperatorsComponent with sc.pardis.ir.InlineFunctions
  with OperatorImplementations with ScanOpImplementations with SelectOpImplementations
  with AggOpImplementations with SortOpImplementations with MapOpImplementations
  with PrintOpOps
  with PrintOpImplementations with WindowOpImplementations with HashJoinOpImplementations
  with LeftHashSemiJoinOpImplementations with NestedLoopsJoinOpImplementations
  with SubquerySingleResultImplementations with ViewOpImplementations
  with HashJoinAntiImplementations with LeftOuterJoinOpImplementations
  with MergeJoinOpImplementations
  with OperatorPartialEvaluation with ScanOpPartialEvaluation with SelectOpPartialEvaluation
  with AggOpPartialEvaluation with SortOpPartialEvaluation with MapOpPartialEvaluation
  with PrintOpPartialEvaluation with WindowOpPartialEvaluation with HashJoinOpPartialEvaluation
  with LeftHashSemiJoinOpPartialEvaluation with NestedLoopsJoinOpPartialEvaluation
  with SubquerySingleResultPartialEvaluation with ViewOpPartialEvaluation
  with HashJoinAntiPartialEvaluation with LeftOuterJoinOpPartialEvaluation
  with MergeJoinOpPartialEvaluation
  with sc.pardis.deep.scalalib.Tuple2PartialEvaluation
  with OperatorDynamicDispatch with BaseOptimization with sc.pardis.deep.scalalib.ArrayOptimization {
  override def hashJoinOpNew2[A <: ch.epfl.data.sc.pardis.shallow.Record, B <: ch.epfl.data.sc.pardis.shallow.Record, C](leftParent: Rep[Operator[A]], rightParent: Rep[Operator[B]], joinCond: Rep[((A, B) => Boolean)], leftHash: Rep[((A) => C)], rightHash: Rep[((B) => C)])(implicit typeA: TypeRep[A], typeB: TypeRep[B], typeC: TypeRep[C]): Rep[HashJoinOp[A, B, C]] = hashJoinOpNew1[A, B, C](leftParent, rightParent, unit(""), unit(""), joinCond, leftHash, rightHash)
  override def printOpRun[A](self: Rep[PrintOp[A]])(implicit typeA: TypeRep[A]): Rep[Unit] =
    if (config.Config.specializeEngine) {
      super[PrintOpImplementations].printOpRun(self)
    } else {
      super[PrintOpOps].printOpRun(self)
    }

}
