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
  with OperatorDynamicDispatch { this: InliningLegoBase =>
  override def findSymbol[T: TypeRep](d: Def[T]): Option[Sym[T]] =
    scopeDefs.find(x => x.rhs == d && x.rhs.tp == d.tp).map(x => x.sym.asInstanceOf[Sym[T]])
  override def infix_asInstanceOf[T: TypeRep](exp: Rep[Any]): Rep[T] = {
    // System.out.println(s"asInstanceOf for $exp from ${exp.tp} to ${typeRep[T]}")
    val res = exp match {
      // case _ if exp.tp.isRecord      => exp.asInstanceOf[Rep[T]]
      case Def(PardisCast(exp2))                     => infix_asInstanceOf[T](exp2)
      case Def(ArrayNew(size)) if typeRep[T].isArray => __newArray(size)(typeRep[T].typeArguments(0)).asInstanceOf[Rep[T]]
      case _ if exp.tp == typeRep[T]                 => exp.asInstanceOf[Rep[T]]
      case _                                         => super.infix_asInstanceOf[T](exp)
    }
    // System.out.println(s"res $res")
    res
  }

  // TODO move to SC
  override def arrayLength[T](self: Rep[Array[T]])(implicit typeT: TypeRep[T]): Rep[Int] = self match {
    case Def(ArrayNew(length)) => length
    case _                     => super.arrayLength(self)
  }

  // TODO move to SC
  override def __ifThenElse[T: TypeRep](cond: Rep[Boolean], thenp: => Rep[T], elsep: => Rep[T]): Rep[T] = cond match {
    case Constant(true)  => thenp
    case Constant(false) => elsep
    case _               => super.__ifThenElse(cond, thenp, elsep)
  }

  // TODO move to SC
  override def infix_==[A: TypeRep, B: TypeRep](a: Rep[A], b: Rep[B]): Rep[Boolean] = (a, b) match {
    case (Constant(v1), Constant(v2)) => unit(v1 == v2)
    case _                            => super.infix_==(a, b)
  }

  override def hashJoinOpNew2[A <: ch.epfl.data.sc.pardis.shallow.Record, B <: ch.epfl.data.sc.pardis.shallow.Record, C](leftParent: Rep[Operator[A]], rightParent: Rep[Operator[B]], joinCond: Rep[((A, B) => Boolean)], leftHash: Rep[((A) => C)], rightHash: Rep[((B) => C)])(implicit typeA: TypeRep[A], typeB: TypeRep[B], typeC: TypeRep[C]): Rep[HashJoinOp[A, B, C]] = hashJoinOpNew1[A, B, C](leftParent, rightParent, unit(""), unit(""), joinCond, leftHash, rightHash)
}
