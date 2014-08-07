package ch.epfl.data
package legobase
package deep

import scalalib._
import pardis.ir._

trait HashJoinOpOps extends Base { this: OperatorsComponent =>
  import pardis.shallow.AbstractRecord
  // implicit class HashJoinOpRep[A, B, C](self: Rep[HashJoinOp[A, B, C]])(implicit manifestA: Manifest[A], manifestB: Manifest[B], manifestC: Manifest[C], evidence$12: Manifest[C], evidence$11: Manifest[B], evidence$10: Manifest[A]) {
  //   def foreach(f: Rep[(DynamicCompositeRecord[A, B] => Unit)]): Rep[Unit] = hashJoinOpForeach[A, B, C](self, f)(manifestA, manifestB, manifestC)
  //   def findFirst(cond: Rep[(DynamicCompositeRecord[A, B] => Boolean)]): Rep[DynamicCompositeRecord[A, B]] = hashJoinOpFindFirst[A, B, C](self, cond)(manifestA, manifestB, manifestC)
  //   def NullDynamicRecord[D](implicit manifestD: Manifest[D]): Rep[D] = hashJoinOpNullDynamicRecord[A, B, C, D](self)(manifestA, manifestB, manifestC, manifestD)
  //   def open(): Rep[Unit] = hashJoinOpOpen[A, B, C](self)(manifestA, manifestB, manifestC)
  //   def next(): Rep[DynamicCompositeRecord[A, B]] = hashJoinOpNext[A, B, C](self)(manifestA, manifestB, manifestC)
  //   def close(): Rep[Unit] = hashJoinOpClose[A, B, C](self)(manifestA, manifestB, manifestC)
  //   def reset(): Rep[Unit] = hashJoinOpReset[A, B, C](self)(manifestA, manifestB, manifestC)
  //   def tmpLine_=(x$1: Rep[B]): Rep[Unit] = hashJoinOp_Field_TmpLine_$eq[A, B, C](self, x$1)(manifestA, manifestB, manifestC)
  //   def tmpLine: Rep[B] = hashJoinOp_Field_TmpLine[A, B, C](self)(manifestA, manifestB, manifestC)
  //   def hm: Rep[HashMap[C, ArrayBuffer[A]]] = hashJoinOp_Field_Hm[A, B, C](self)(manifestA, manifestB, manifestC)
  //   def tmpBuffer_=(x$1: Rep[ArrayBuffer[A]]): Rep[Unit] = hashJoinOp_Field_TmpBuffer_$eq[A, B, C](self, x$1)(manifestA, manifestB, manifestC)
  //   def tmpBuffer: Rep[ArrayBuffer[A]] = hashJoinOp_Field_TmpBuffer[A, B, C](self)(manifestA, manifestB, manifestC)
  //   def tmpCount_=(x$1: Rep[Int]): Rep[Unit] = hashJoinOp_Field_TmpCount_$eq[A, B, C](self, x$1)(manifestA, manifestB, manifestC)
  //   def tmpCount: Rep[Int] = hashJoinOp_Field_TmpCount[A, B, C](self)(manifestA, manifestB, manifestC)
  //   def evidence$12(): Rep[Manifest[C]] = hashJoinOp_Field_Evidence$12[A, B, C](self)(manifestA, manifestB, manifestC)
  //   def evidence$11(): Rep[Manifest[B]] = hashJoinOp_Field_Evidence$11[A, B, C](self)(manifestA, manifestB, manifestC)
  //   def evidence$10(): Rep[Manifest[A]] = hashJoinOp_Field_Evidence$10[A, B, C](self)(manifestA, manifestB, manifestC)
  //   def rightHash: Rep[(B => C)] = hashJoinOp_Field_RightHash[A, B, C](self)(manifestA, manifestB, manifestC)
  //   def leftHash: Rep[(A => C)] = hashJoinOp_Field_LeftHash[A, B, C](self)(manifestA, manifestB, manifestC)
  //   def joinCond: Rep[((A, B) => Boolean)] = hashJoinOp_Field_JoinCond[A, B, C](self)(manifestA, manifestB, manifestC)
  //   def rightAlias: Rep[String] = hashJoinOp_Field_RightAlias[A, B, C](self)(manifestA, manifestB, manifestC)
  //   def leftAlias: Rep[String] = hashJoinOp_Field_LeftAlias[A, B, C](self)(manifestA, manifestB, manifestC)
  //   def rightParent: Rep[Operator[B]] = hashJoinOp_Field_RightParent[A, B, C](self)(manifestA, manifestB, manifestC)
  //   def leftParent: Rep[Operator[A]] = hashJoinOp_Field_LeftParent[A, B, C](self)(manifestA, manifestB, manifestC)
  //   def NullDynamicRecord: Rep[DynamicCompositeRecord[A, B]] = hashJoinOp_Field_NullDynamicRecord[A, B, C](self)(manifestA, manifestB, manifestC)
  // }
  // constructors
  def __newHashJoinOp[A <: AbstractRecord, B <: AbstractRecord, C](leftParent: Rep[Operator[A]], rightParent: Rep[Operator[B]], leftAlias: Rep[String], rightAlias: Rep[String])(joinCond: Rep[((A, B) => Boolean)])(leftHash: Rep[(A => C)])(rightHash: Rep[(B => C)])(implicit evidence$10: Manifest[A], evidence$11: Manifest[B], evidence$12: Manifest[C], manifestA: Manifest[A], manifestB: Manifest[B], manifestC: Manifest[C]): Rep[HashJoinOp[A, B, C]] = hashJoinOpNew[A, B, C](leftParent, rightParent, leftAlias, rightAlias, joinCond, leftHash, rightHash)(manifestA, manifestB, manifestC)
  // case classes
  case class HashJoinOpNew[A <: AbstractRecord, B <: AbstractRecord, C](leftParent: Rep[Operator[A]], rightParent: Rep[Operator[B]], leftAlias: Rep[String], rightAlias: Rep[String], joinCond: Rep[((A, B) => Boolean)], leftHash: Rep[((A) => C)], rightHash: Rep[((B) => C)])(implicit val manifestA: Manifest[A], val manifestB: Manifest[B], val manifestC: Manifest[C]) extends FunctionDef[HashJoinOp[A, B, C]](None, "new HashJoinOp", List(List(leftParent, rightParent, leftAlias, rightAlias), List(joinCond), List(leftHash), List(rightHash))) {
    override def curriedConstructor = (copy[A, B, C] _).curried
  }

  // case class HashJoinOpForeach[A, B, C](self: Rep[HashJoinOp[A, B, C]], f: Rep[((DynamicCompositeRecord[A, B]) => Unit)])(implicit val manifestA: Manifest[A], val manifestB: Manifest[B], val manifestC: Manifest[C]) extends FunctionDef[Unit](Some(self), "foreach", List(List(f))) {
  //   override def curriedConstructor = (copy[A, B, C] _).curried
  // }

  // case class HashJoinOpFindFirst[A, B, C](self: Rep[HashJoinOp[A, B, C]], cond: Rep[((DynamicCompositeRecord[A, B]) => Boolean)])(implicit val manifestA: Manifest[A], val manifestB: Manifest[B], val manifestC: Manifest[C]) extends FunctionDef[DynamicCompositeRecord[A, B]](Some(self), "findFirst", List(List(cond))) {
  //   override def curriedConstructor = (copy[A, B, C] _).curried
  // }

  // case class HashJoinOpNullDynamicRecord[A, B, C, D](self: Rep[HashJoinOp[A, B, C]])(implicit val manifestA: Manifest[A], val manifestB: Manifest[B], val manifestC: Manifest[C], val manifestD: Manifest[D]) extends FunctionDef[D](Some(self), "NullDynamicRecord", List()) {
  //   override def curriedConstructor = (copy[A, B, C, D] _)
  // }

  // case class HashJoinOpOpen[A, B, C](self: Rep[HashJoinOp[A, B, C]])(implicit val manifestA: Manifest[A], val manifestB: Manifest[B], val manifestC: Manifest[C]) extends FunctionDef[Unit](Some(self), "open", List()) {
  //   override def curriedConstructor = (copy[A, B, C] _)
  // }

  // case class HashJoinOpNext[A, B, C](self: Rep[HashJoinOp[A, B, C]])(implicit val manifestA: Manifest[A], val manifestB: Manifest[B], val manifestC: Manifest[C]) extends FunctionDef[DynamicCompositeRecord[A, B]](Some(self), "next", List()) {
  //   override def curriedConstructor = (copy[A, B, C] _)
  // }

  // case class HashJoinOpClose[A, B, C](self: Rep[HashJoinOp[A, B, C]])(implicit val manifestA: Manifest[A], val manifestB: Manifest[B], val manifestC: Manifest[C]) extends FunctionDef[Unit](Some(self), "close", List()) {
  //   override def curriedConstructor = (copy[A, B, C] _)
  // }

  // case class HashJoinOpReset[A, B, C](self: Rep[HashJoinOp[A, B, C]])(implicit val manifestA: Manifest[A], val manifestB: Manifest[B], val manifestC: Manifest[C]) extends FunctionDef[Unit](Some(self), "reset", List()) {
  //   override def curriedConstructor = (copy[A, B, C] _)
  // }

  // case class HashJoinOp_Field_TmpLine_$eq[A, B, C](self: Rep[HashJoinOp[A, B, C]], x$1: Rep[B])(implicit val manifestA: Manifest[A], val manifestB: Manifest[B], val manifestC: Manifest[C]) extends FieldSetter[B](self, "tmpLine", x$1) {
  //   override def curriedConstructor = (copy[A, B, C] _).curried
  // }

  // case class HashJoinOp_Field_TmpLine[A, B, C](self: Rep[HashJoinOp[A, B, C]])(implicit val manifestA: Manifest[A], val manifestB: Manifest[B], val manifestC: Manifest[C]) extends FieldGetter[B](self, "tmpLine") {
  //   override def curriedConstructor = (copy[A, B, C] _)
  // }

  // case class HashJoinOp_Field_Hm[A, B, C](self: Rep[HashJoinOp[A, B, C]])(implicit val manifestA: Manifest[A], val manifestB: Manifest[B], val manifestC: Manifest[C]) extends FieldDef[HashMap[C, ArrayBuffer[A]]](self, "hm") {
  //   override def curriedConstructor = (copy[A, B, C] _)
  //   override def isPure = true

  // }

  // case class HashJoinOp_Field_TmpBuffer_$eq[A, B, C](self: Rep[HashJoinOp[A, B, C]], x$1: Rep[ArrayBuffer[A]])(implicit val manifestA: Manifest[A], val manifestB: Manifest[B], val manifestC: Manifest[C]) extends FieldSetter[ArrayBuffer[A]](self, "tmpBuffer", x$1) {
  //   override def curriedConstructor = (copy[A, B, C] _).curried
  // }

  // case class HashJoinOp_Field_TmpBuffer[A, B, C](self: Rep[HashJoinOp[A, B, C]])(implicit val manifestA: Manifest[A], val manifestB: Manifest[B], val manifestC: Manifest[C]) extends FieldGetter[ArrayBuffer[A]](self, "tmpBuffer") {
  //   override def curriedConstructor = (copy[A, B, C] _)
  // }

  // case class HashJoinOp_Field_TmpCount_$eq[A, B, C](self: Rep[HashJoinOp[A, B, C]], x$1: Rep[Int])(implicit val manifestA: Manifest[A], val manifestB: Manifest[B], val manifestC: Manifest[C]) extends FieldSetter[Int](self, "tmpCount", x$1) {
  //   override def curriedConstructor = (copy[A, B, C] _).curried
  // }

  // case class HashJoinOp_Field_TmpCount[A, B, C](self: Rep[HashJoinOp[A, B, C]])(implicit val manifestA: Manifest[A], val manifestB: Manifest[B], val manifestC: Manifest[C]) extends FieldGetter[Int](self, "tmpCount") {
  //   override def curriedConstructor = (copy[A, B, C] _)
  // }

  // case class HashJoinOp_Field_Evidence$12[A, B, C](self: Rep[HashJoinOp[A, B, C]])(implicit val manifestA: Manifest[A], val manifestB: Manifest[B], val manifestC: Manifest[C]) extends FieldDef[Manifest[C]](self, "evidence$12") {
  //   override def curriedConstructor = (copy[A, B, C] _)
  //   override def isPure = true

  // }

  // case class HashJoinOp_Field_Evidence$11[A, B, C](self: Rep[HashJoinOp[A, B, C]])(implicit val manifestA: Manifest[A], val manifestB: Manifest[B], val manifestC: Manifest[C]) extends FieldDef[Manifest[B]](self, "evidence$11") {
  //   override def curriedConstructor = (copy[A, B, C] _)
  //   override def isPure = true

  // }

  // case class HashJoinOp_Field_Evidence$10[A, B, C](self: Rep[HashJoinOp[A, B, C]])(implicit val manifestA: Manifest[A], val manifestB: Manifest[B], val manifestC: Manifest[C]) extends FieldDef[Manifest[A]](self, "evidence$10") {
  //   override def curriedConstructor = (copy[A, B, C] _)
  //   override def isPure = true

  // }

  // case class HashJoinOp_Field_RightHash[A, B, C](self: Rep[HashJoinOp[A, B, C]])(implicit val manifestA: Manifest[A], val manifestB: Manifest[B], val manifestC: Manifest[C]) extends FieldDef[(B => C)](self, "rightHash") {
  //   override def curriedConstructor = (copy[A, B, C] _)
  //   override def isPure = true

  // }

  // case class HashJoinOp_Field_LeftHash[A, B, C](self: Rep[HashJoinOp[A, B, C]])(implicit val manifestA: Manifest[A], val manifestB: Manifest[B], val manifestC: Manifest[C]) extends FieldDef[(A => C)](self, "leftHash") {
  //   override def curriedConstructor = (copy[A, B, C] _)
  //   override def isPure = true

  // }

  // case class HashJoinOp_Field_JoinCond[A, B, C](self: Rep[HashJoinOp[A, B, C]])(implicit val manifestA: Manifest[A], val manifestB: Manifest[B], val manifestC: Manifest[C]) extends FieldDef[((A, B) => Boolean)](self, "joinCond") {
  //   override def curriedConstructor = (copy[A, B, C] _)
  //   override def isPure = true

  // }

  // case class HashJoinOp_Field_RightAlias[A, B, C](self: Rep[HashJoinOp[A, B, C]])(implicit val manifestA: Manifest[A], val manifestB: Manifest[B], val manifestC: Manifest[C]) extends FieldDef[String](self, "rightAlias") {
  //   override def curriedConstructor = (copy[A, B, C] _)
  //   override def isPure = true

  // }

  // case class HashJoinOp_Field_LeftAlias[A, B, C](self: Rep[HashJoinOp[A, B, C]])(implicit val manifestA: Manifest[A], val manifestB: Manifest[B], val manifestC: Manifest[C]) extends FieldDef[String](self, "leftAlias") {
  //   override def curriedConstructor = (copy[A, B, C] _)
  //   override def isPure = true

  // }

  // case class HashJoinOp_Field_RightParent[A, B, C](self: Rep[HashJoinOp[A, B, C]])(implicit val manifestA: Manifest[A], val manifestB: Manifest[B], val manifestC: Manifest[C]) extends FieldDef[Operator[B]](self, "rightParent") {
  //   override def curriedConstructor = (copy[A, B, C] _)
  //   override def isPure = true

  // }

  // case class HashJoinOp_Field_LeftParent[A, B, C](self: Rep[HashJoinOp[A, B, C]])(implicit val manifestA: Manifest[A], val manifestB: Manifest[B], val manifestC: Manifest[C]) extends FieldDef[Operator[A]](self, "leftParent") {
  //   override def curriedConstructor = (copy[A, B, C] _)
  //   override def isPure = true

  // }

  // case class HashJoinOp_Field_NullDynamicRecord[A, B, C](self: Rep[HashJoinOp[A, B, C]])(implicit val manifestA: Manifest[A], val manifestB: Manifest[B], val manifestC: Manifest[C]) extends FieldDef[DynamicCompositeRecord[A, B]](self, "NullDynamicRecord") {
  //   override def curriedConstructor = (copy[A, B, C] _)
  //   override def isPure = true

  // }

  // method definitions
  def hashJoinOpNew[A <: AbstractRecord, B <: AbstractRecord, C](leftParent: Rep[Operator[A]], rightParent: Rep[Operator[B]], leftAlias: Rep[String], rightAlias: Rep[String], joinCond: Rep[((A, B) => Boolean)], leftHash: Rep[((A) => C)], rightHash: Rep[((B) => C)])(implicit manifestA: Manifest[A], manifestB: Manifest[B], manifestC: Manifest[C]): Rep[HashJoinOp[A, B, C]] = HashJoinOpNew[A, B, C](leftParent, rightParent, leftAlias, rightAlias, joinCond, leftHash, rightHash)
  // def hashJoinOpForeach[A, B, C](self: Rep[HashJoinOp[A, B, C]], f: Rep[((DynamicCompositeRecord[A, B]) => Unit)])(implicit manifestA: Manifest[A], manifestB: Manifest[B], manifestC: Manifest[C]): Rep[Unit] = HashJoinOpForeach[A, B, C](self, f)
  // def hashJoinOpFindFirst[A, B, C](self: Rep[HashJoinOp[A, B, C]], cond: Rep[((DynamicCompositeRecord[A, B]) => Boolean)])(implicit manifestA: Manifest[A], manifestB: Manifest[B], manifestC: Manifest[C]): Rep[DynamicCompositeRecord[A, B]] = HashJoinOpFindFirst[A, B, C](self, cond)
  // def hashJoinOpNullDynamicRecord[A, B, C, D](self: Rep[HashJoinOp[A, B, C]])(implicit manifestA: Manifest[A], manifestB: Manifest[B], manifestC: Manifest[C], manifestD: Manifest[D]): Rep[D] = HashJoinOpNullDynamicRecord[A, B, C, D](self)
  // def hashJoinOpOpen[A, B, C](self: Rep[HashJoinOp[A, B, C]])(implicit manifestA: Manifest[A], manifestB: Manifest[B], manifestC: Manifest[C]): Rep[Unit] = HashJoinOpOpen[A, B, C](self)
  // def hashJoinOpNext[A, B, C](self: Rep[HashJoinOp[A, B, C]])(implicit manifestA: Manifest[A], manifestB: Manifest[B], manifestC: Manifest[C]): Rep[DynamicCompositeRecord[A, B]] = HashJoinOpNext[A, B, C](self)
  // def hashJoinOpClose[A, B, C](self: Rep[HashJoinOp[A, B, C]])(implicit manifestA: Manifest[A], manifestB: Manifest[B], manifestC: Manifest[C]): Rep[Unit] = HashJoinOpClose[A, B, C](self)
  // def hashJoinOpReset[A, B, C](self: Rep[HashJoinOp[A, B, C]])(implicit manifestA: Manifest[A], manifestB: Manifest[B], manifestC: Manifest[C]): Rep[Unit] = HashJoinOpReset[A, B, C](self)
  // def hashJoinOp_Field_TmpLine_$eq[A, B, C](self: Rep[HashJoinOp[A, B, C]], x$1: Rep[B])(implicit manifestA: Manifest[A], manifestB: Manifest[B], manifestC: Manifest[C]): Rep[Unit] = HashJoinOp_Field_TmpLine_$eq[A, B, C](self, x$1)
  // def hashJoinOp_Field_TmpLine[A, B, C](self: Rep[HashJoinOp[A, B, C]])(implicit manifestA: Manifest[A], manifestB: Manifest[B], manifestC: Manifest[C]): Rep[B] = HashJoinOp_Field_TmpLine[A, B, C](self)
  // def hashJoinOp_Field_Hm[A, B, C](self: Rep[HashJoinOp[A, B, C]])(implicit manifestA: Manifest[A], manifestB: Manifest[B], manifestC: Manifest[C]): Rep[HashMap[C, ArrayBuffer[A]]] = HashJoinOp_Field_Hm[A, B, C](self)
  // def hashJoinOp_Field_TmpBuffer_$eq[A, B, C](self: Rep[HashJoinOp[A, B, C]], x$1: Rep[ArrayBuffer[A]])(implicit manifestA: Manifest[A], manifestB: Manifest[B], manifestC: Manifest[C]): Rep[Unit] = HashJoinOp_Field_TmpBuffer_$eq[A, B, C](self, x$1)
  // def hashJoinOp_Field_TmpBuffer[A, B, C](self: Rep[HashJoinOp[A, B, C]])(implicit manifestA: Manifest[A], manifestB: Manifest[B], manifestC: Manifest[C]): Rep[ArrayBuffer[A]] = HashJoinOp_Field_TmpBuffer[A, B, C](self)
  // def hashJoinOp_Field_TmpCount_$eq[A, B, C](self: Rep[HashJoinOp[A, B, C]], x$1: Rep[Int])(implicit manifestA: Manifest[A], manifestB: Manifest[B], manifestC: Manifest[C]): Rep[Unit] = HashJoinOp_Field_TmpCount_$eq[A, B, C](self, x$1)
  // def hashJoinOp_Field_TmpCount[A, B, C](self: Rep[HashJoinOp[A, B, C]])(implicit manifestA: Manifest[A], manifestB: Manifest[B], manifestC: Manifest[C]): Rep[Int] = HashJoinOp_Field_TmpCount[A, B, C](self)
  // def hashJoinOp_Field_Evidence$12[A, B, C](self: Rep[HashJoinOp[A, B, C]])(implicit manifestA: Manifest[A], manifestB: Manifest[B], manifestC: Manifest[C]): Rep[Manifest[C]] = HashJoinOp_Field_Evidence$12[A, B, C](self)
  // def hashJoinOp_Field_Evidence$11[A, B, C](self: Rep[HashJoinOp[A, B, C]])(implicit manifestA: Manifest[A], manifestB: Manifest[B], manifestC: Manifest[C]): Rep[Manifest[B]] = HashJoinOp_Field_Evidence$11[A, B, C](self)
  // def hashJoinOp_Field_Evidence$10[A, B, C](self: Rep[HashJoinOp[A, B, C]])(implicit manifestA: Manifest[A], manifestB: Manifest[B], manifestC: Manifest[C]): Rep[Manifest[A]] = HashJoinOp_Field_Evidence$10[A, B, C](self)
  // def hashJoinOp_Field_RightHash[A, B, C](self: Rep[HashJoinOp[A, B, C]])(implicit manifestA: Manifest[A], manifestB: Manifest[B], manifestC: Manifest[C]): Rep[(B => C)] = HashJoinOp_Field_RightHash[A, B, C](self)
  // def hashJoinOp_Field_LeftHash[A, B, C](self: Rep[HashJoinOp[A, B, C]])(implicit manifestA: Manifest[A], manifestB: Manifest[B], manifestC: Manifest[C]): Rep[(A => C)] = HashJoinOp_Field_LeftHash[A, B, C](self)
  // def hashJoinOp_Field_JoinCond[A, B, C](self: Rep[HashJoinOp[A, B, C]])(implicit manifestA: Manifest[A], manifestB: Manifest[B], manifestC: Manifest[C]): Rep[((A, B) => Boolean)] = HashJoinOp_Field_JoinCond[A, B, C](self)
  // def hashJoinOp_Field_RightAlias[A, B, C](self: Rep[HashJoinOp[A, B, C]])(implicit manifestA: Manifest[A], manifestB: Manifest[B], manifestC: Manifest[C]): Rep[String] = HashJoinOp_Field_RightAlias[A, B, C](self)
  // def hashJoinOp_Field_LeftAlias[A, B, C](self: Rep[HashJoinOp[A, B, C]])(implicit manifestA: Manifest[A], manifestB: Manifest[B], manifestC: Manifest[C]): Rep[String] = HashJoinOp_Field_LeftAlias[A, B, C](self)
  // def hashJoinOp_Field_RightParent[A, B, C](self: Rep[HashJoinOp[A, B, C]])(implicit manifestA: Manifest[A], manifestB: Manifest[B], manifestC: Manifest[C]): Rep[Operator[B]] = HashJoinOp_Field_RightParent[A, B, C](self)
  // def hashJoinOp_Field_LeftParent[A, B, C](self: Rep[HashJoinOp[A, B, C]])(implicit manifestA: Manifest[A], manifestB: Manifest[B], manifestC: Manifest[C]): Rep[Operator[A]] = HashJoinOp_Field_LeftParent[A, B, C](self)
  // def hashJoinOp_Field_NullDynamicRecord[A, B, C](self: Rep[HashJoinOp[A, B, C]])(implicit manifestA: Manifest[A], manifestB: Manifest[B], manifestC: Manifest[C]): Rep[DynamicCompositeRecord[A, B]] = HashJoinOp_Field_NullDynamicRecord[A, B, C](self)
  type HashJoinOp[A <: AbstractRecord, B <: AbstractRecord, C] = ch.epfl.data.legobase.queryengine.volcano.HashJoinOp[A, B, C]
  // class HashJoinOp[A, B, C]
}
trait HashJoinOpImplicits { this: HashJoinOpComponent =>
  // Add implicit conversions here!
}
trait HashJoinOpComponent extends HashJoinOpOps with HashJoinOpImplicits { self: OperatorsComponent => }