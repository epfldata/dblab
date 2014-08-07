
package ch.epfl.data
package legobase
package deep

import scalalib._
import pardis.ir._
trait OperatorOps extends Base { this: OperatorsComponent =>
  implicit class OperatorRep[A](self: Rep[Operator[A]])(implicit manifestA: Manifest[A], evidence$1: Manifest[A]) {
    def open(): Rep[Unit] = operatorOpen[A](self)(manifestA)
    def next(): Rep[A] = operatorNext[A](self)(manifestA)
    def close(): Rep[Unit] = operatorClose[A](self)(manifestA)
    def reset(): Rep[Unit] = operatorReset[A](self)(manifestA)
    def foreach(f: Rep[(A => Unit)]): Rep[Unit] = operatorForeach[A](self, f)(manifestA)
    def findFirst(cond: Rep[(A => Boolean)]): Rep[A] = operatorFindFirst[A](self, cond)(manifestA)
    def NullDynamicRecord[D](implicit manifestD: Manifest[D]): Rep[D] = operatorNullDynamicRecord[A, D](self)(manifestA, manifestD)
    def NullDynamicRecord: Rep[A] = operator_Field_NullDynamicRecord[A](self)(manifestA)
    def evidence$1(): Rep[Manifest[A]] = operator_Field_Evidence$1[A](self)(manifestA)
  }
  // constructors

  // case classes
  case class OperatorOpen[A](self: Rep[Operator[A]])(implicit val manifestA: Manifest[A]) extends FunctionDef[Unit](Some(self), "open", List()) {
    override def curriedConstructor = (copy[A] _)
  }

  case class OperatorNext[A](self: Rep[Operator[A]])(implicit val manifestA: Manifest[A]) extends FunctionDef[A](Some(self), "next", List()) {
    override def curriedConstructor = (copy[A] _)
  }

  case class OperatorClose[A](self: Rep[Operator[A]])(implicit val manifestA: Manifest[A]) extends FunctionDef[Unit](Some(self), "close", List()) {
    override def curriedConstructor = (copy[A] _)
  }

  case class OperatorReset[A](self: Rep[Operator[A]])(implicit val manifestA: Manifest[A]) extends FunctionDef[Unit](Some(self), "reset", List()) {
    override def curriedConstructor = (copy[A] _)
  }

  case class OperatorForeach[A](self: Rep[Operator[A]], f: Rep[((A) => Unit)])(implicit val manifestA: Manifest[A]) extends FunctionDef[Unit](Some(self), "foreach", List(List(f))) {
    override def curriedConstructor = (copy[A] _).curried
  }

  case class OperatorFindFirst[A](self: Rep[Operator[A]], cond: Rep[((A) => Boolean)])(implicit val manifestA: Manifest[A]) extends FunctionDef[A](Some(self), "findFirst", List(List(cond))) {
    override def curriedConstructor = (copy[A] _).curried
  }

  case class OperatorNullDynamicRecord[A, D](self: Rep[Operator[A]])(implicit val manifestA: Manifest[A], val manifestD: Manifest[D]) extends FunctionDef[D](Some(self), "NullDynamicRecord", List()) {
    override def curriedConstructor = (copy[A, D] _)
  }

  case class Operator_Field_NullDynamicRecord[A](self: Rep[Operator[A]])(implicit val manifestA: Manifest[A]) extends FieldDef[A](self, "NullDynamicRecord") {
    override def curriedConstructor = (copy[A] _)
    override def isPure = true

  }

  case class Operator_Field_Evidence$1[A](self: Rep[Operator[A]])(implicit val manifestA: Manifest[A]) extends FieldDef[Manifest[A]](self, "evidence$1") {
    override def curriedConstructor = (copy[A] _)
    override def isPure = true

  }

  // method definitions
  def operatorOpen[A](self: Rep[Operator[A]])(implicit manifestA: Manifest[A]): Rep[Unit] = OperatorOpen[A](self)
  def operatorNext[A](self: Rep[Operator[A]])(implicit manifestA: Manifest[A]): Rep[A] = OperatorNext[A](self)
  def operatorClose[A](self: Rep[Operator[A]])(implicit manifestA: Manifest[A]): Rep[Unit] = OperatorClose[A](self)
  def operatorReset[A](self: Rep[Operator[A]])(implicit manifestA: Manifest[A]): Rep[Unit] = OperatorReset[A](self)
  def operatorForeach[A](self: Rep[Operator[A]], f: Rep[((A) => Unit)])(implicit manifestA: Manifest[A]): Rep[Unit] = OperatorForeach[A](self, f)
  def operatorFindFirst[A](self: Rep[Operator[A]], cond: Rep[((A) => Boolean)])(implicit manifestA: Manifest[A]): Rep[A] = OperatorFindFirst[A](self, cond)
  def operatorNullDynamicRecord[A, D](self: Rep[Operator[A]])(implicit manifestA: Manifest[A], manifestD: Manifest[D]): Rep[D] = OperatorNullDynamicRecord[A, D](self)
  def operator_Field_NullDynamicRecord[A](self: Rep[Operator[A]])(implicit manifestA: Manifest[A]): Rep[A] = Operator_Field_NullDynamicRecord[A](self)
  def operator_Field_Evidence$1[A](self: Rep[Operator[A]])(implicit manifestA: Manifest[A]): Rep[Manifest[A]] = Operator_Field_Evidence$1[A](self)
  type Operator[A] = ch.epfl.data.legobase.queryengine.volcano.Operator[A]
}
trait OperatorImplicits { this: OperatorComponent =>
  // Add implicit conversions here!
}
trait OperatorComponent extends OperatorOps with OperatorImplicits { self: OperatorsComponent => }
trait ScanOpOps extends Base { this: OperatorsComponent =>
  implicit class ScanOpRep[A](self: Rep[ScanOp[A]])(implicit manifestA: Manifest[A], evidence$3: Manifest[A]) {
    def foreach(f: Rep[(A => Unit)]): Rep[Unit] = scanOpForeach[A](self, f)(manifestA)
    def findFirst(cond: Rep[(A => Boolean)]): Rep[A] = scanOpFindFirst[A](self, cond)(manifestA)
    def NullDynamicRecord[D](implicit manifestD: Manifest[D]): Rep[D] = scanOpNullDynamicRecord[A, D](self)(manifestA, manifestD)
    def open(): Rep[Unit] = scanOpOpen[A](self)(manifestA)
    def next(): Rep[A] = scanOpNext[A](self)(manifestA)
    def close(): Rep[Unit] = scanOpClose[A](self)(manifestA)
    def reset(): Rep[Unit] = scanOpReset[A](self)(manifestA)
    def i_=(x$1: Rep[Int]): Rep[Unit] = scanOp_Field_I_$eq[A](self, x$1)(manifestA)
    def i: Rep[Int] = scanOp_Field_I[A](self)(manifestA)
    def evidence$3(): Rep[Manifest[A]] = scanOp_Field_Evidence$3[A](self)(manifestA)
    def table: Rep[Array[A]] = scanOp_Field_Table[A](self)(manifestA)
    def NullDynamicRecord: Rep[A] = scanOp_Field_NullDynamicRecord[A](self)(manifestA)
  }
  // constructors
  def __newScanOp[A](table: Rep[Array[A]])(implicit evidence$3: Manifest[A], manifestA: Manifest[A]): Rep[ScanOp[A]] = scanOpNew[A](table)(manifestA)
  // case classes
  case class ScanOpNew[A](table: Rep[Array[A]])(implicit val manifestA: Manifest[A]) extends FunctionDef[ScanOp[A]](None, "new ScanOp", List(List(table))) {
    override def curriedConstructor = (copy[A] _)
  }

  case class ScanOpForeach[A](self: Rep[ScanOp[A]], f: Rep[((A) => Unit)])(implicit val manifestA: Manifest[A]) extends FunctionDef[Unit](Some(self), "foreach", List(List(f))) {
    override def curriedConstructor = (copy[A] _).curried
  }

  case class ScanOpFindFirst[A](self: Rep[ScanOp[A]], cond: Rep[((A) => Boolean)])(implicit val manifestA: Manifest[A]) extends FunctionDef[A](Some(self), "findFirst", List(List(cond))) {
    override def curriedConstructor = (copy[A] _).curried
  }

  case class ScanOpNullDynamicRecord[A, D](self: Rep[ScanOp[A]])(implicit val manifestA: Manifest[A], val manifestD: Manifest[D]) extends FunctionDef[D](Some(self), "NullDynamicRecord", List()) {
    override def curriedConstructor = (copy[A, D] _)
  }

  case class ScanOpOpen[A](self: Rep[ScanOp[A]])(implicit val manifestA: Manifest[A]) extends FunctionDef[Unit](Some(self), "open", List()) {
    override def curriedConstructor = (copy[A] _)
  }

  case class ScanOpNext[A](self: Rep[ScanOp[A]])(implicit val manifestA: Manifest[A]) extends FunctionDef[A](Some(self), "next", List()) {
    override def curriedConstructor = (copy[A] _)
  }

  case class ScanOpClose[A](self: Rep[ScanOp[A]])(implicit val manifestA: Manifest[A]) extends FunctionDef[Unit](Some(self), "close", List()) {
    override def curriedConstructor = (copy[A] _)
  }

  case class ScanOpReset[A](self: Rep[ScanOp[A]])(implicit val manifestA: Manifest[A]) extends FunctionDef[Unit](Some(self), "reset", List()) {
    override def curriedConstructor = (copy[A] _)
  }

  case class ScanOp_Field_I_$eq[A](self: Rep[ScanOp[A]], x$1: Rep[Int])(implicit val manifestA: Manifest[A]) extends FieldSetter[Int](self, "i", x$1) {
    override def curriedConstructor = (copy[A] _).curried
  }

  case class ScanOp_Field_I[A](self: Rep[ScanOp[A]])(implicit val manifestA: Manifest[A]) extends FieldGetter[Int](self, "i") {
    override def curriedConstructor = (copy[A] _)
  }

  case class ScanOp_Field_Evidence$3[A](self: Rep[ScanOp[A]])(implicit val manifestA: Manifest[A]) extends FieldDef[Manifest[A]](self, "evidence$3") {
    override def curriedConstructor = (copy[A] _)
    override def isPure = true

  }

  case class ScanOp_Field_Table[A](self: Rep[ScanOp[A]])(implicit val manifestA: Manifest[A]) extends FieldDef[Array[A]](self, "table") {
    override def curriedConstructor = (copy[A] _)
    override def isPure = true

  }

  case class ScanOp_Field_NullDynamicRecord[A](self: Rep[ScanOp[A]])(implicit val manifestA: Manifest[A]) extends FieldDef[A](self, "NullDynamicRecord") {
    override def curriedConstructor = (copy[A] _)
    override def isPure = true

  }

  // method definitions
  def scanOpNew[A](table: Rep[Array[A]])(implicit manifestA: Manifest[A]): Rep[ScanOp[A]] = ScanOpNew[A](table)
  def scanOpForeach[A](self: Rep[ScanOp[A]], f: Rep[((A) => Unit)])(implicit manifestA: Manifest[A]): Rep[Unit] = ScanOpForeach[A](self, f)
  def scanOpFindFirst[A](self: Rep[ScanOp[A]], cond: Rep[((A) => Boolean)])(implicit manifestA: Manifest[A]): Rep[A] = ScanOpFindFirst[A](self, cond)
  def scanOpNullDynamicRecord[A, D](self: Rep[ScanOp[A]])(implicit manifestA: Manifest[A], manifestD: Manifest[D]): Rep[D] = ScanOpNullDynamicRecord[A, D](self)
  def scanOpOpen[A](self: Rep[ScanOp[A]])(implicit manifestA: Manifest[A]): Rep[Unit] = ScanOpOpen[A](self)
  def scanOpNext[A](self: Rep[ScanOp[A]])(implicit manifestA: Manifest[A]): Rep[A] = ScanOpNext[A](self)
  def scanOpClose[A](self: Rep[ScanOp[A]])(implicit manifestA: Manifest[A]): Rep[Unit] = ScanOpClose[A](self)
  def scanOpReset[A](self: Rep[ScanOp[A]])(implicit manifestA: Manifest[A]): Rep[Unit] = ScanOpReset[A](self)
  def scanOp_Field_I_$eq[A](self: Rep[ScanOp[A]], x$1: Rep[Int])(implicit manifestA: Manifest[A]): Rep[Unit] = ScanOp_Field_I_$eq[A](self, x$1)
  def scanOp_Field_I[A](self: Rep[ScanOp[A]])(implicit manifestA: Manifest[A]): Rep[Int] = ScanOp_Field_I[A](self)
  def scanOp_Field_Evidence$3[A](self: Rep[ScanOp[A]])(implicit manifestA: Manifest[A]): Rep[Manifest[A]] = ScanOp_Field_Evidence$3[A](self)
  def scanOp_Field_Table[A](self: Rep[ScanOp[A]])(implicit manifestA: Manifest[A]): Rep[Array[A]] = ScanOp_Field_Table[A](self)
  def scanOp_Field_NullDynamicRecord[A](self: Rep[ScanOp[A]])(implicit manifestA: Manifest[A]): Rep[A] = ScanOp_Field_NullDynamicRecord[A](self)
  type ScanOp[A] = ch.epfl.data.legobase.queryengine.volcano.ScanOp[A]
}
trait ScanOpImplicits { this: ScanOpComponent =>
  // Add implicit conversions here!
}
trait ScanOpComponent extends ScanOpOps with ScanOpImplicits { self: OperatorsComponent => }
trait SelectOpOps extends Base { this: OperatorsComponent =>
  implicit class SelectOpRep[A](self: Rep[SelectOp[A]])(implicit manifestA: Manifest[A], evidence$4: Manifest[A]) {
    def foreach(f: Rep[(A => Unit)]): Rep[Unit] = selectOpForeach[A](self, f)(manifestA)
    def findFirst(cond: Rep[(A => Boolean)]): Rep[A] = selectOpFindFirst[A](self, cond)(manifestA)
    def NullDynamicRecord[D](implicit manifestD: Manifest[D]): Rep[D] = selectOpNullDynamicRecord[A, D](self)(manifestA, manifestD)
    def open(): Rep[Unit] = selectOpOpen[A](self)(manifestA)
    def next(): Rep[A] = selectOpNext[A](self)(manifestA)
    def close(): Rep[Unit] = selectOpClose[A](self)(manifestA)
    def reset(): Rep[Unit] = selectOpReset[A](self)(manifestA)
    def evidence$4(): Rep[Manifest[A]] = selectOp_Field_Evidence$4[A](self)(manifestA)
    def selectPred(): Rep[(A => Boolean)] = selectOp_Field_SelectPred[A](self)(manifestA)
    def parent: Rep[Operator[A]] = selectOp_Field_Parent[A](self)(manifestA)
    def NullDynamicRecord: Rep[A] = selectOp_Field_NullDynamicRecord[A](self)(manifestA)
  }
  // constructors
  def __newSelectOp[A](parent: Rep[Operator[A]])(selectPred: Rep[(A => Boolean)])(implicit evidence$4: Manifest[A], manifestA: Manifest[A]): Rep[SelectOp[A]] = selectOpNew[A](parent, selectPred)(manifestA)
  // case classes
  case class SelectOpNew[A](parent: Rep[Operator[A]], selectPred: Rep[((A) => Boolean)])(implicit val manifestA: Manifest[A]) extends FunctionDef[SelectOp[A]](None, "new SelectOp", List(List(parent), List(selectPred))) {
    override def curriedConstructor = (copy[A] _).curried
  }

  case class SelectOpForeach[A](self: Rep[SelectOp[A]], f: Rep[((A) => Unit)])(implicit val manifestA: Manifest[A]) extends FunctionDef[Unit](Some(self), "foreach", List(List(f))) {
    override def curriedConstructor = (copy[A] _).curried
  }

  case class SelectOpFindFirst[A](self: Rep[SelectOp[A]], cond: Rep[((A) => Boolean)])(implicit val manifestA: Manifest[A]) extends FunctionDef[A](Some(self), "findFirst", List(List(cond))) {
    override def curriedConstructor = (copy[A] _).curried
  }

  case class SelectOpNullDynamicRecord[A, D](self: Rep[SelectOp[A]])(implicit val manifestA: Manifest[A], val manifestD: Manifest[D]) extends FunctionDef[D](Some(self), "NullDynamicRecord", List()) {
    override def curriedConstructor = (copy[A, D] _)
  }

  case class SelectOpOpen[A](self: Rep[SelectOp[A]])(implicit val manifestA: Manifest[A]) extends FunctionDef[Unit](Some(self), "open", List()) {
    override def curriedConstructor = (copy[A] _)
  }

  case class SelectOpNext[A](self: Rep[SelectOp[A]])(implicit val manifestA: Manifest[A]) extends FunctionDef[A](Some(self), "next", List()) {
    override def curriedConstructor = (copy[A] _)
  }

  case class SelectOpClose[A](self: Rep[SelectOp[A]])(implicit val manifestA: Manifest[A]) extends FunctionDef[Unit](Some(self), "close", List()) {
    override def curriedConstructor = (copy[A] _)
  }

  case class SelectOpReset[A](self: Rep[SelectOp[A]])(implicit val manifestA: Manifest[A]) extends FunctionDef[Unit](Some(self), "reset", List()) {
    override def curriedConstructor = (copy[A] _)
  }

  case class SelectOp_Field_Evidence$4[A](self: Rep[SelectOp[A]])(implicit val manifestA: Manifest[A]) extends FieldDef[Manifest[A]](self, "evidence$4") {
    override def curriedConstructor = (copy[A] _)
    override def isPure = true

  }

  case class SelectOp_Field_SelectPred[A](self: Rep[SelectOp[A]])(implicit val manifestA: Manifest[A]) extends FieldDef[(A => Boolean)](self, "selectPred") {
    override def curriedConstructor = (copy[A] _)
    override def isPure = true

  }

  case class SelectOp_Field_Parent[A](self: Rep[SelectOp[A]])(implicit val manifestA: Manifest[A]) extends FieldDef[Operator[A]](self, "parent") {
    override def curriedConstructor = (copy[A] _)
    override def isPure = true

  }

  case class SelectOp_Field_NullDynamicRecord[A](self: Rep[SelectOp[A]])(implicit val manifestA: Manifest[A]) extends FieldDef[A](self, "NullDynamicRecord") {
    override def curriedConstructor = (copy[A] _)
    override def isPure = true

  }

  // method definitions
  def selectOpNew[A](parent: Rep[Operator[A]], selectPred: Rep[((A) => Boolean)])(implicit manifestA: Manifest[A]): Rep[SelectOp[A]] = SelectOpNew[A](parent, selectPred)
  def selectOpForeach[A](self: Rep[SelectOp[A]], f: Rep[((A) => Unit)])(implicit manifestA: Manifest[A]): Rep[Unit] = SelectOpForeach[A](self, f)
  def selectOpFindFirst[A](self: Rep[SelectOp[A]], cond: Rep[((A) => Boolean)])(implicit manifestA: Manifest[A]): Rep[A] = SelectOpFindFirst[A](self, cond)
  def selectOpNullDynamicRecord[A, D](self: Rep[SelectOp[A]])(implicit manifestA: Manifest[A], manifestD: Manifest[D]): Rep[D] = SelectOpNullDynamicRecord[A, D](self)
  def selectOpOpen[A](self: Rep[SelectOp[A]])(implicit manifestA: Manifest[A]): Rep[Unit] = SelectOpOpen[A](self)
  def selectOpNext[A](self: Rep[SelectOp[A]])(implicit manifestA: Manifest[A]): Rep[A] = SelectOpNext[A](self)
  def selectOpClose[A](self: Rep[SelectOp[A]])(implicit manifestA: Manifest[A]): Rep[Unit] = SelectOpClose[A](self)
  def selectOpReset[A](self: Rep[SelectOp[A]])(implicit manifestA: Manifest[A]): Rep[Unit] = SelectOpReset[A](self)
  def selectOp_Field_Evidence$4[A](self: Rep[SelectOp[A]])(implicit manifestA: Manifest[A]): Rep[Manifest[A]] = SelectOp_Field_Evidence$4[A](self)
  def selectOp_Field_SelectPred[A](self: Rep[SelectOp[A]])(implicit manifestA: Manifest[A]): Rep[(A => Boolean)] = SelectOp_Field_SelectPred[A](self)
  def selectOp_Field_Parent[A](self: Rep[SelectOp[A]])(implicit manifestA: Manifest[A]): Rep[Operator[A]] = SelectOp_Field_Parent[A](self)
  def selectOp_Field_NullDynamicRecord[A](self: Rep[SelectOp[A]])(implicit manifestA: Manifest[A]): Rep[A] = SelectOp_Field_NullDynamicRecord[A](self)
  type SelectOp[A] = ch.epfl.data.legobase.queryengine.volcano.SelectOp[A]
}
trait SelectOpImplicits { this: SelectOpComponent =>
  // Add implicit conversions here!
}
trait SelectOpComponent extends SelectOpOps with SelectOpImplicits { self: OperatorsComponent => }
trait AggOpOps extends Base { this: OperatorsComponent =>
  implicit class AggOpRep[A, B](self: Rep[AggOp[A, B]])(implicit manifestA: Manifest[A], manifestB: Manifest[B], evidence$6: Manifest[B], evidence$5: Manifest[A]) {
    def foreach(f: Rep[(AGGRecord[B] => Unit)]): Rep[Unit] = aggOpForeach[A, B](self, f)(manifestA, manifestB)
    def findFirst(cond: Rep[(AGGRecord[B] => Boolean)]): Rep[AGGRecord[B]] = aggOpFindFirst[A, B](self, cond)(manifestA, manifestB)
    def NullDynamicRecord[D](implicit manifestD: Manifest[D]): Rep[D] = aggOpNullDynamicRecord[A, B, D](self)(manifestA, manifestB, manifestD)
    def open(): Rep[Unit] = aggOpOpen[A, B](self)(manifestA, manifestB)
    def next(): Rep[AGGRecord[B]] = aggOpNext[A, B](self)(manifestA, manifestB)
    def close(): Rep[Unit] = aggOpClose[A, B](self)(manifestA, manifestB)
    def reset(): Rep[Unit] = aggOpReset[A, B](self)(manifestA, manifestB)
    def keySet_=(x$1: Rep[Set[B]]): Rep[Unit] = aggOp_Field_KeySet_$eq[A, B](self, x$1)(manifestA, manifestB)
    def keySet: Rep[Set[B]] = aggOp_Field_KeySet[A, B](self)(manifestA, manifestB)
    def hm: Rep[HashMap[B, Array[Double]]] = aggOp_Field_Hm[A, B](self)(manifestA, manifestB)
    def mB: Rep[Manifest[B]] = aggOp_Field_MB[A, B](self)(manifestA, manifestB)
    def mA: Rep[Manifest[A]] = aggOp_Field_MA[A, B](self)(manifestA, manifestB)
    def evidence$6(): Rep[Manifest[B]] = aggOp_Field_Evidence$6[A, B](self)(manifestA, manifestB)
    def evidence$5(): Rep[Manifest[A]] = aggOp_Field_Evidence$5[A, B](self)(manifestA, manifestB)
    def aggFuncs: Rep[Seq[((A, Double) => Double)]] = aggOp_Field_AggFuncs[A, B](self)(manifestA, manifestB)
    def grp: Rep[(A => B)] = aggOp_Field_Grp[A, B](self)(manifestA, manifestB)
    def numAggs: Rep[Int] = aggOp_Field_NumAggs[A, B](self)(manifestA, manifestB)
    def parent: Rep[Operator[A]] = aggOp_Field_Parent[A, B](self)(manifestA, manifestB)
    def NullDynamicRecord: Rep[AGGRecord[B]] = aggOp_Field_NullDynamicRecord[A, B](self)(manifestA, manifestB)
  }
  // constructors
  def __newAggOp[A, B](parent: Rep[Operator[A]], numAggs: Rep[Int])(grp: Rep[(A => B)])(aggFuncs: Rep[((A, Double) => Double)]*)(implicit evidence$5: Manifest[A], evidence$6: Manifest[B], manifestA: Manifest[A], manifestB: Manifest[B]): Rep[AggOp[A, B]] = aggOpNew[A, B](parent, numAggs, grp, aggFuncs: _*)(manifestA, manifestB)
  // case classes
  case class AggOpNew[A, B](parent: Rep[Operator[A]], numAggs: Rep[Int], grp: Rep[((A) => B)], aggFuncsOutput: Rep[Seq[((A, Double) => Double)]])(implicit val manifestA: Manifest[A], val manifestB: Manifest[B]) extends FunctionDef[AggOp[A, B]](None, "new AggOp", List(List(parent, numAggs), List(grp), List(__varArg(aggFuncsOutput)))) {
    override def curriedConstructor = (copy[A, B] _).curried
  }

  case class AggOpForeach[A, B](self: Rep[AggOp[A, B]], f: Rep[((AGGRecord[B]) => Unit)])(implicit val manifestA: Manifest[A], val manifestB: Manifest[B]) extends FunctionDef[Unit](Some(self), "foreach", List(List(f))) {
    override def curriedConstructor = (copy[A, B] _).curried
  }

  case class AggOpFindFirst[A, B](self: Rep[AggOp[A, B]], cond: Rep[((AGGRecord[B]) => Boolean)])(implicit val manifestA: Manifest[A], val manifestB: Manifest[B]) extends FunctionDef[AGGRecord[B]](Some(self), "findFirst", List(List(cond))) {
    override def curriedConstructor = (copy[A, B] _).curried
  }

  case class AggOpNullDynamicRecord[A, B, D](self: Rep[AggOp[A, B]])(implicit val manifestA: Manifest[A], val manifestB: Manifest[B], val manifestD: Manifest[D]) extends FunctionDef[D](Some(self), "NullDynamicRecord", List()) {
    override def curriedConstructor = (copy[A, B, D] _)
  }

  case class AggOpOpen[A, B](self: Rep[AggOp[A, B]])(implicit val manifestA: Manifest[A], val manifestB: Manifest[B]) extends FunctionDef[Unit](Some(self), "open", List()) {
    override def curriedConstructor = (copy[A, B] _)
  }

  case class AggOpNext[A, B](self: Rep[AggOp[A, B]])(implicit val manifestA: Manifest[A], val manifestB: Manifest[B]) extends FunctionDef[AGGRecord[B]](Some(self), "next", List()) {
    override def curriedConstructor = (copy[A, B] _)
  }

  case class AggOpClose[A, B](self: Rep[AggOp[A, B]])(implicit val manifestA: Manifest[A], val manifestB: Manifest[B]) extends FunctionDef[Unit](Some(self), "close", List()) {
    override def curriedConstructor = (copy[A, B] _)
  }

  case class AggOpReset[A, B](self: Rep[AggOp[A, B]])(implicit val manifestA: Manifest[A], val manifestB: Manifest[B]) extends FunctionDef[Unit](Some(self), "reset", List()) {
    override def curriedConstructor = (copy[A, B] _)
  }

  case class AggOp_Field_KeySet_$eq[A, B](self: Rep[AggOp[A, B]], x$1: Rep[Set[B]])(implicit val manifestA: Manifest[A], val manifestB: Manifest[B]) extends FieldSetter[Set[B]](self, "keySet", x$1) {
    override def curriedConstructor = (copy[A, B] _).curried
  }

  case class AggOp_Field_KeySet[A, B](self: Rep[AggOp[A, B]])(implicit val manifestA: Manifest[A], val manifestB: Manifest[B]) extends FieldGetter[Set[B]](self, "keySet") {
    override def curriedConstructor = (copy[A, B] _)
  }

  case class AggOp_Field_Hm[A, B](self: Rep[AggOp[A, B]])(implicit val manifestA: Manifest[A], val manifestB: Manifest[B]) extends FieldDef[HashMap[B, Array[Double]]](self, "hm") {
    override def curriedConstructor = (copy[A, B] _)
    override def isPure = true

  }

  case class AggOp_Field_MB[A, B](self: Rep[AggOp[A, B]])(implicit val manifestA: Manifest[A], val manifestB: Manifest[B]) extends FieldDef[Manifest[B]](self, "mB") {
    override def curriedConstructor = (copy[A, B] _)
    override def isPure = true

  }

  case class AggOp_Field_MA[A, B](self: Rep[AggOp[A, B]])(implicit val manifestA: Manifest[A], val manifestB: Manifest[B]) extends FieldDef[Manifest[A]](self, "mA") {
    override def curriedConstructor = (copy[A, B] _)
    override def isPure = true

  }

  case class AggOp_Field_Evidence$6[A, B](self: Rep[AggOp[A, B]])(implicit val manifestA: Manifest[A], val manifestB: Manifest[B]) extends FieldDef[Manifest[B]](self, "evidence$6") {
    override def curriedConstructor = (copy[A, B] _)
    override def isPure = true

  }

  case class AggOp_Field_Evidence$5[A, B](self: Rep[AggOp[A, B]])(implicit val manifestA: Manifest[A], val manifestB: Manifest[B]) extends FieldDef[Manifest[A]](self, "evidence$5") {
    override def curriedConstructor = (copy[A, B] _)
    override def isPure = true

  }

  case class AggOp_Field_AggFuncs[A, B](self: Rep[AggOp[A, B]])(implicit val manifestA: Manifest[A], val manifestB: Manifest[B]) extends FieldDef[Seq[((A, Double) => Double)]](self, "aggFuncs") {
    override def curriedConstructor = (copy[A, B] _)
    override def isPure = true

  }

  case class AggOp_Field_Grp[A, B](self: Rep[AggOp[A, B]])(implicit val manifestA: Manifest[A], val manifestB: Manifest[B]) extends FieldDef[(A => B)](self, "grp") {
    override def curriedConstructor = (copy[A, B] _)
    override def isPure = true

  }

  case class AggOp_Field_NumAggs[A, B](self: Rep[AggOp[A, B]])(implicit val manifestA: Manifest[A], val manifestB: Manifest[B]) extends FieldDef[Int](self, "numAggs") {
    override def curriedConstructor = (copy[A, B] _)
    override def isPure = true

  }

  case class AggOp_Field_Parent[A, B](self: Rep[AggOp[A, B]])(implicit val manifestA: Manifest[A], val manifestB: Manifest[B]) extends FieldDef[Operator[A]](self, "parent") {
    override def curriedConstructor = (copy[A, B] _)
    override def isPure = true

  }

  case class AggOp_Field_NullDynamicRecord[A, B](self: Rep[AggOp[A, B]])(implicit val manifestA: Manifest[A], val manifestB: Manifest[B]) extends FieldDef[AGGRecord[B]](self, "NullDynamicRecord") {
    override def curriedConstructor = (copy[A, B] _)
    override def isPure = true

  }

  // method definitions
  def aggOpNew[A, B](parent: Rep[Operator[A]], numAggs: Rep[Int], grp: Rep[((A) => B)], aggFuncs: Rep[((A, Double) => Double)]*)(implicit manifestA: Manifest[A], manifestB: Manifest[B]): Rep[AggOp[A, B]] = {
    val aggFuncsOutput = __liftSeq(aggFuncs.toSeq)
    AggOpNew[A, B](parent, numAggs, grp, aggFuncsOutput)
  }
  def aggOpForeach[A, B](self: Rep[AggOp[A, B]], f: Rep[((AGGRecord[B]) => Unit)])(implicit manifestA: Manifest[A], manifestB: Manifest[B]): Rep[Unit] = AggOpForeach[A, B](self, f)
  def aggOpFindFirst[A, B](self: Rep[AggOp[A, B]], cond: Rep[((AGGRecord[B]) => Boolean)])(implicit manifestA: Manifest[A], manifestB: Manifest[B]): Rep[AGGRecord[B]] = AggOpFindFirst[A, B](self, cond)
  def aggOpNullDynamicRecord[A, B, D](self: Rep[AggOp[A, B]])(implicit manifestA: Manifest[A], manifestB: Manifest[B], manifestD: Manifest[D]): Rep[D] = AggOpNullDynamicRecord[A, B, D](self)
  def aggOpOpen[A, B](self: Rep[AggOp[A, B]])(implicit manifestA: Manifest[A], manifestB: Manifest[B]): Rep[Unit] = AggOpOpen[A, B](self)
  def aggOpNext[A, B](self: Rep[AggOp[A, B]])(implicit manifestA: Manifest[A], manifestB: Manifest[B]): Rep[AGGRecord[B]] = AggOpNext[A, B](self)
  def aggOpClose[A, B](self: Rep[AggOp[A, B]])(implicit manifestA: Manifest[A], manifestB: Manifest[B]): Rep[Unit] = AggOpClose[A, B](self)
  def aggOpReset[A, B](self: Rep[AggOp[A, B]])(implicit manifestA: Manifest[A], manifestB: Manifest[B]): Rep[Unit] = AggOpReset[A, B](self)
  def aggOp_Field_KeySet_$eq[A, B](self: Rep[AggOp[A, B]], x$1: Rep[Set[B]])(implicit manifestA: Manifest[A], manifestB: Manifest[B]): Rep[Unit] = AggOp_Field_KeySet_$eq[A, B](self, x$1)
  def aggOp_Field_KeySet[A, B](self: Rep[AggOp[A, B]])(implicit manifestA: Manifest[A], manifestB: Manifest[B]): Rep[Set[B]] = AggOp_Field_KeySet[A, B](self)
  def aggOp_Field_Hm[A, B](self: Rep[AggOp[A, B]])(implicit manifestA: Manifest[A], manifestB: Manifest[B]): Rep[HashMap[B, Array[Double]]] = AggOp_Field_Hm[A, B](self)
  def aggOp_Field_MB[A, B](self: Rep[AggOp[A, B]])(implicit manifestA: Manifest[A], manifestB: Manifest[B]): Rep[Manifest[B]] = AggOp_Field_MB[A, B](self)
  def aggOp_Field_MA[A, B](self: Rep[AggOp[A, B]])(implicit manifestA: Manifest[A], manifestB: Manifest[B]): Rep[Manifest[A]] = AggOp_Field_MA[A, B](self)
  def aggOp_Field_Evidence$6[A, B](self: Rep[AggOp[A, B]])(implicit manifestA: Manifest[A], manifestB: Manifest[B]): Rep[Manifest[B]] = AggOp_Field_Evidence$6[A, B](self)
  def aggOp_Field_Evidence$5[A, B](self: Rep[AggOp[A, B]])(implicit manifestA: Manifest[A], manifestB: Manifest[B]): Rep[Manifest[A]] = AggOp_Field_Evidence$5[A, B](self)
  def aggOp_Field_AggFuncs[A, B](self: Rep[AggOp[A, B]])(implicit manifestA: Manifest[A], manifestB: Manifest[B]): Rep[Seq[((A, Double) => Double)]] = AggOp_Field_AggFuncs[A, B](self)
  def aggOp_Field_Grp[A, B](self: Rep[AggOp[A, B]])(implicit manifestA: Manifest[A], manifestB: Manifest[B]): Rep[(A => B)] = AggOp_Field_Grp[A, B](self)
  def aggOp_Field_NumAggs[A, B](self: Rep[AggOp[A, B]])(implicit manifestA: Manifest[A], manifestB: Manifest[B]): Rep[Int] = AggOp_Field_NumAggs[A, B](self)
  def aggOp_Field_Parent[A, B](self: Rep[AggOp[A, B]])(implicit manifestA: Manifest[A], manifestB: Manifest[B]): Rep[Operator[A]] = AggOp_Field_Parent[A, B](self)
  def aggOp_Field_NullDynamicRecord[A, B](self: Rep[AggOp[A, B]])(implicit manifestA: Manifest[A], manifestB: Manifest[B]): Rep[AGGRecord[B]] = AggOp_Field_NullDynamicRecord[A, B](self)
  type AggOp[A, B] = ch.epfl.data.legobase.queryengine.volcano.AggOp[A, B]
}
trait AggOpImplicits { this: AggOpComponent =>
  // Add implicit conversions here!
}
trait AggOpComponent extends AggOpOps with AggOpImplicits { self: OperatorsComponent => }
trait SortOpOps extends Base { this: OperatorsComponent =>
  implicit class SortOpRep[A](self: Rep[SortOp[A]])(implicit manifestA: Manifest[A], evidence$7: Manifest[A]) {
    def foreach(f: Rep[(A => Unit)]): Rep[Unit] = sortOpForeach[A](self, f)(manifestA)
    def findFirst(cond: Rep[(A => Boolean)]): Rep[A] = sortOpFindFirst[A](self, cond)(manifestA)
    def NullDynamicRecord[D](implicit manifestD: Manifest[D]): Rep[D] = sortOpNullDynamicRecord[A, D](self)(manifestA, manifestD)
    def open(): Rep[Unit] = sortOpOpen[A](self)(manifestA)
    def next(): Rep[A] = sortOpNext[A](self)(manifestA)
    def close(): Rep[Unit] = sortOpClose[A](self)(manifestA)
    def reset(): Rep[Unit] = sortOpReset[A](self)(manifestA)
    def sortedTree: Rep[TreeSet[A]] = sortOp_Field_SortedTree[A](self)(manifestA)
    def evidence$7(): Rep[Manifest[A]] = sortOp_Field_Evidence$7[A](self)(manifestA)
    def orderingFunc(): Rep[((A, A) => Int)] = sortOp_Field_OrderingFunc[A](self)(manifestA)
    def parent: Rep[Operator[A]] = sortOp_Field_Parent[A](self)(manifestA)
    def NullDynamicRecord: Rep[A] = sortOp_Field_NullDynamicRecord[A](self)(manifestA)
  }
  // constructors
  def __newSortOp[A](parent: Rep[Operator[A]])(orderingFunc: Rep[((A, A) => Int)])(implicit evidence$7: Manifest[A], manifestA: Manifest[A]): Rep[SortOp[A]] = sortOpNew[A](parent, orderingFunc)(manifestA)
  // case classes
  case class SortOpNew[A](parent: Rep[Operator[A]], orderingFunc: Rep[((A, A) => Int)])(implicit val manifestA: Manifest[A]) extends FunctionDef[SortOp[A]](None, "new SortOp", List(List(parent), List(orderingFunc))) {
    override def curriedConstructor = (copy[A] _).curried
  }

  case class SortOpForeach[A](self: Rep[SortOp[A]], f: Rep[((A) => Unit)])(implicit val manifestA: Manifest[A]) extends FunctionDef[Unit](Some(self), "foreach", List(List(f))) {
    override def curriedConstructor = (copy[A] _).curried
  }

  case class SortOpFindFirst[A](self: Rep[SortOp[A]], cond: Rep[((A) => Boolean)])(implicit val manifestA: Manifest[A]) extends FunctionDef[A](Some(self), "findFirst", List(List(cond))) {
    override def curriedConstructor = (copy[A] _).curried
  }

  case class SortOpNullDynamicRecord[A, D](self: Rep[SortOp[A]])(implicit val manifestA: Manifest[A], val manifestD: Manifest[D]) extends FunctionDef[D](Some(self), "NullDynamicRecord", List()) {
    override def curriedConstructor = (copy[A, D] _)
  }

  case class SortOpOpen[A](self: Rep[SortOp[A]])(implicit val manifestA: Manifest[A]) extends FunctionDef[Unit](Some(self), "open", List()) {
    override def curriedConstructor = (copy[A] _)
  }

  case class SortOpNext[A](self: Rep[SortOp[A]])(implicit val manifestA: Manifest[A]) extends FunctionDef[A](Some(self), "next", List()) {
    override def curriedConstructor = (copy[A] _)
  }

  case class SortOpClose[A](self: Rep[SortOp[A]])(implicit val manifestA: Manifest[A]) extends FunctionDef[Unit](Some(self), "close", List()) {
    override def curriedConstructor = (copy[A] _)
  }

  case class SortOpReset[A](self: Rep[SortOp[A]])(implicit val manifestA: Manifest[A]) extends FunctionDef[Unit](Some(self), "reset", List()) {
    override def curriedConstructor = (copy[A] _)
  }

  case class SortOp_Field_SortedTree[A](self: Rep[SortOp[A]])(implicit val manifestA: Manifest[A]) extends FieldDef[TreeSet[A]](self, "sortedTree") {
    override def curriedConstructor = (copy[A] _)
    override def isPure = true

  }

  case class SortOp_Field_Evidence$7[A](self: Rep[SortOp[A]])(implicit val manifestA: Manifest[A]) extends FieldDef[Manifest[A]](self, "evidence$7") {
    override def curriedConstructor = (copy[A] _)
    override def isPure = true

  }

  case class SortOp_Field_OrderingFunc[A](self: Rep[SortOp[A]])(implicit val manifestA: Manifest[A]) extends FieldDef[((A, A) => Int)](self, "orderingFunc") {
    override def curriedConstructor = (copy[A] _)
    override def isPure = true

  }

  case class SortOp_Field_Parent[A](self: Rep[SortOp[A]])(implicit val manifestA: Manifest[A]) extends FieldDef[Operator[A]](self, "parent") {
    override def curriedConstructor = (copy[A] _)
    override def isPure = true

  }

  case class SortOp_Field_NullDynamicRecord[A](self: Rep[SortOp[A]])(implicit val manifestA: Manifest[A]) extends FieldDef[A](self, "NullDynamicRecord") {
    override def curriedConstructor = (copy[A] _)
    override def isPure = true

  }

  // method definitions
  def sortOpNew[A](parent: Rep[Operator[A]], orderingFunc: Rep[((A, A) => Int)])(implicit manifestA: Manifest[A]): Rep[SortOp[A]] = SortOpNew[A](parent, orderingFunc)
  def sortOpForeach[A](self: Rep[SortOp[A]], f: Rep[((A) => Unit)])(implicit manifestA: Manifest[A]): Rep[Unit] = SortOpForeach[A](self, f)
  def sortOpFindFirst[A](self: Rep[SortOp[A]], cond: Rep[((A) => Boolean)])(implicit manifestA: Manifest[A]): Rep[A] = SortOpFindFirst[A](self, cond)
  def sortOpNullDynamicRecord[A, D](self: Rep[SortOp[A]])(implicit manifestA: Manifest[A], manifestD: Manifest[D]): Rep[D] = SortOpNullDynamicRecord[A, D](self)
  def sortOpOpen[A](self: Rep[SortOp[A]])(implicit manifestA: Manifest[A]): Rep[Unit] = SortOpOpen[A](self)
  def sortOpNext[A](self: Rep[SortOp[A]])(implicit manifestA: Manifest[A]): Rep[A] = SortOpNext[A](self)
  def sortOpClose[A](self: Rep[SortOp[A]])(implicit manifestA: Manifest[A]): Rep[Unit] = SortOpClose[A](self)
  def sortOpReset[A](self: Rep[SortOp[A]])(implicit manifestA: Manifest[A]): Rep[Unit] = SortOpReset[A](self)
  def sortOp_Field_SortedTree[A](self: Rep[SortOp[A]])(implicit manifestA: Manifest[A]): Rep[TreeSet[A]] = SortOp_Field_SortedTree[A](self)
  def sortOp_Field_Evidence$7[A](self: Rep[SortOp[A]])(implicit manifestA: Manifest[A]): Rep[Manifest[A]] = SortOp_Field_Evidence$7[A](self)
  def sortOp_Field_OrderingFunc[A](self: Rep[SortOp[A]])(implicit manifestA: Manifest[A]): Rep[((A, A) => Int)] = SortOp_Field_OrderingFunc[A](self)
  def sortOp_Field_Parent[A](self: Rep[SortOp[A]])(implicit manifestA: Manifest[A]): Rep[Operator[A]] = SortOp_Field_Parent[A](self)
  def sortOp_Field_NullDynamicRecord[A](self: Rep[SortOp[A]])(implicit manifestA: Manifest[A]): Rep[A] = SortOp_Field_NullDynamicRecord[A](self)
  type SortOp[A] = ch.epfl.data.legobase.queryengine.volcano.SortOp[A]
}
trait SortOpImplicits { this: SortOpComponent =>
  // Add implicit conversions here!
}
trait SortOpComponent extends SortOpOps with SortOpImplicits { self: OperatorsComponent => }
trait MapOpOps extends Base { this: OperatorsComponent =>
  implicit class MapOpRep[A](self: Rep[MapOp[A]])(implicit manifestA: Manifest[A], evidence$8: Manifest[A]) {
    def foreach(f: Rep[(A => Unit)]): Rep[Unit] = mapOpForeach[A](self, f)(manifestA)
    def findFirst(cond: Rep[(A => Boolean)]): Rep[A] = mapOpFindFirst[A](self, cond)(manifestA)
    def NullDynamicRecord[D](implicit manifestD: Manifest[D]): Rep[D] = mapOpNullDynamicRecord[A, D](self)(manifestA, manifestD)
    def open(): Rep[Unit] = mapOpOpen[A](self)(manifestA)
    def next(): Rep[A] = mapOpNext[A](self)(manifestA)
    def close(): Rep[Unit] = mapOpClose[A](self)(manifestA)
    def reset(): Rep[Unit] = mapOpReset[A](self)(manifestA)
    def evidence$8(): Rep[Manifest[A]] = mapOp_Field_Evidence$8[A](self)(manifestA)
    def aggFuncs(): Rep[Seq[(A => Unit)]] = mapOp_Field_AggFuncs[A](self)(manifestA)
    def parent: Rep[Operator[A]] = mapOp_Field_Parent[A](self)(manifestA)
    def NullDynamicRecord: Rep[A] = mapOp_Field_NullDynamicRecord[A](self)(manifestA)
  }
  // constructors
  def __newMapOp[A](parent: Rep[Operator[A]])(aggFuncs: Rep[(A => Unit)]*)(implicit evidence$8: Manifest[A], manifestA: Manifest[A]): Rep[MapOp[A]] = mapOpNew[A](parent, aggFuncs: _*)(manifestA)
  // case classes
  case class MapOpNew[A](parent: Rep[Operator[A]], aggFuncsOutput: Rep[Seq[((A) => Unit)]])(implicit val manifestA: Manifest[A]) extends FunctionDef[MapOp[A]](None, "new MapOp", List(List(parent), List(__varArg(aggFuncsOutput)))) {
    override def curriedConstructor = (copy[A] _).curried
  }

  case class MapOpForeach[A](self: Rep[MapOp[A]], f: Rep[((A) => Unit)])(implicit val manifestA: Manifest[A]) extends FunctionDef[Unit](Some(self), "foreach", List(List(f))) {
    override def curriedConstructor = (copy[A] _).curried
  }

  case class MapOpFindFirst[A](self: Rep[MapOp[A]], cond: Rep[((A) => Boolean)])(implicit val manifestA: Manifest[A]) extends FunctionDef[A](Some(self), "findFirst", List(List(cond))) {
    override def curriedConstructor = (copy[A] _).curried
  }

  case class MapOpNullDynamicRecord[A, D](self: Rep[MapOp[A]])(implicit val manifestA: Manifest[A], val manifestD: Manifest[D]) extends FunctionDef[D](Some(self), "NullDynamicRecord", List()) {
    override def curriedConstructor = (copy[A, D] _)
  }

  case class MapOpOpen[A](self: Rep[MapOp[A]])(implicit val manifestA: Manifest[A]) extends FunctionDef[Unit](Some(self), "open", List()) {
    override def curriedConstructor = (copy[A] _)
  }

  case class MapOpNext[A](self: Rep[MapOp[A]])(implicit val manifestA: Manifest[A]) extends FunctionDef[A](Some(self), "next", List()) {
    override def curriedConstructor = (copy[A] _)
  }

  case class MapOpClose[A](self: Rep[MapOp[A]])(implicit val manifestA: Manifest[A]) extends FunctionDef[Unit](Some(self), "close", List()) {
    override def curriedConstructor = (copy[A] _)
  }

  case class MapOpReset[A](self: Rep[MapOp[A]])(implicit val manifestA: Manifest[A]) extends FunctionDef[Unit](Some(self), "reset", List()) {
    override def curriedConstructor = (copy[A] _)
  }

  case class MapOp_Field_Evidence$8[A](self: Rep[MapOp[A]])(implicit val manifestA: Manifest[A]) extends FieldDef[Manifest[A]](self, "evidence$8") {
    override def curriedConstructor = (copy[A] _)
    override def isPure = true

  }

  case class MapOp_Field_AggFuncs[A](self: Rep[MapOp[A]])(implicit val manifestA: Manifest[A]) extends FieldDef[Seq[(A => Unit)]](self, "aggFuncs") {
    override def curriedConstructor = (copy[A] _)
    override def isPure = true

  }

  case class MapOp_Field_Parent[A](self: Rep[MapOp[A]])(implicit val manifestA: Manifest[A]) extends FieldDef[Operator[A]](self, "parent") {
    override def curriedConstructor = (copy[A] _)
    override def isPure = true

  }

  case class MapOp_Field_NullDynamicRecord[A](self: Rep[MapOp[A]])(implicit val manifestA: Manifest[A]) extends FieldDef[A](self, "NullDynamicRecord") {
    override def curriedConstructor = (copy[A] _)
    override def isPure = true

  }

  // method definitions
  def mapOpNew[A](parent: Rep[Operator[A]], aggFuncs: Rep[((A) => Unit)]*)(implicit manifestA: Manifest[A]): Rep[MapOp[A]] = {
    val aggFuncsOutput = __liftSeq(aggFuncs.toSeq)
    MapOpNew[A](parent, aggFuncsOutput)
  }
  def mapOpForeach[A](self: Rep[MapOp[A]], f: Rep[((A) => Unit)])(implicit manifestA: Manifest[A]): Rep[Unit] = MapOpForeach[A](self, f)
  def mapOpFindFirst[A](self: Rep[MapOp[A]], cond: Rep[((A) => Boolean)])(implicit manifestA: Manifest[A]): Rep[A] = MapOpFindFirst[A](self, cond)
  def mapOpNullDynamicRecord[A, D](self: Rep[MapOp[A]])(implicit manifestA: Manifest[A], manifestD: Manifest[D]): Rep[D] = MapOpNullDynamicRecord[A, D](self)
  def mapOpOpen[A](self: Rep[MapOp[A]])(implicit manifestA: Manifest[A]): Rep[Unit] = MapOpOpen[A](self)
  def mapOpNext[A](self: Rep[MapOp[A]])(implicit manifestA: Manifest[A]): Rep[A] = MapOpNext[A](self)
  def mapOpClose[A](self: Rep[MapOp[A]])(implicit manifestA: Manifest[A]): Rep[Unit] = MapOpClose[A](self)
  def mapOpReset[A](self: Rep[MapOp[A]])(implicit manifestA: Manifest[A]): Rep[Unit] = MapOpReset[A](self)
  def mapOp_Field_Evidence$8[A](self: Rep[MapOp[A]])(implicit manifestA: Manifest[A]): Rep[Manifest[A]] = MapOp_Field_Evidence$8[A](self)
  def mapOp_Field_AggFuncs[A](self: Rep[MapOp[A]])(implicit manifestA: Manifest[A]): Rep[Seq[(A => Unit)]] = MapOp_Field_AggFuncs[A](self)
  def mapOp_Field_Parent[A](self: Rep[MapOp[A]])(implicit manifestA: Manifest[A]): Rep[Operator[A]] = MapOp_Field_Parent[A](self)
  def mapOp_Field_NullDynamicRecord[A](self: Rep[MapOp[A]])(implicit manifestA: Manifest[A]): Rep[A] = MapOp_Field_NullDynamicRecord[A](self)
  type MapOp[A] = ch.epfl.data.legobase.queryengine.volcano.MapOp[A]
}
trait MapOpImplicits { this: MapOpComponent =>
  // Add implicit conversions here!
}
trait MapOpComponent extends MapOpOps with MapOpImplicits { self: OperatorsComponent => }
trait PrintOpOps extends Base { this: OperatorsComponent =>
  implicit class PrintOpRep[A](self: Rep[PrintOp[A]])(implicit manifestA: Manifest[A], evidence$9: Manifest[A]) {
    def foreach(f: Rep[(A => Unit)]): Rep[Unit] = printOpForeach[A](self, f)(manifestA)
    def findFirst(cond: Rep[(A => Boolean)]): Rep[A] = printOpFindFirst[A](self, cond)(manifestA)
    def NullDynamicRecord[D](implicit manifestD: Manifest[D]): Rep[D] = printOpNullDynamicRecord[A, D](self)(manifestA, manifestD)
    def open(): Rep[Unit] = printOpOpen[A](self)(manifestA)
    def next(): Rep[A] = printOpNext[A](self)(manifestA)
    def close(): Rep[Unit] = printOpClose[A](self)(manifestA)
    def reset(): Rep[Unit] = printOpReset[A](self)(manifestA)
    def numRows_=(x$1: Rep[Int]): Rep[Unit] = printOp_Field_NumRows_$eq[A](self, x$1)(manifestA)
    def numRows: Rep[Int] = printOp_Field_NumRows[A](self)(manifestA)
    def evidence$9(): Rep[Manifest[A]] = printOp_Field_Evidence$9[A](self)(manifestA)
    def limit(): Rep[(() => Boolean)] = printOp_Field_Limit[A](self)(manifestA)
    def printFunc(): Rep[(A => Unit)] = printOp_Field_PrintFunc[A](self)(manifestA)
    def parent_=(x$1: Rep[Operator[A]]): Rep[Unit] = printOp_Field_Parent_$eq[A](self, x$1)(manifestA)
    def parent: Rep[Operator[A]] = printOp_Field_Parent[A](self)(manifestA)
    def NullDynamicRecord: Rep[A] = printOp_Field_NullDynamicRecord[A](self)(manifestA)
  }
  // constructors
  def __newPrintOp[A](parent: Rep[Operator[A]])(printFunc: Rep[(A => Unit)], limit: Rep[(() => Boolean)])(implicit evidence$9: Manifest[A], manifestA: Manifest[A]): Rep[PrintOp[A]] = printOpNew[A](parent, printFunc, limit)(manifestA)
  // case classes
  case class PrintOpNew[A](parent: Rep[Operator[A]], printFunc: Rep[((A) => Unit)], limit: Rep[(() => Boolean)])(implicit val manifestA: Manifest[A]) extends FunctionDef[PrintOp[A]](None, "new PrintOp", List(List(parent), List(printFunc, limit))) {
    override def curriedConstructor = (copy[A] _).curried
  }

  case class PrintOpForeach[A](self: Rep[PrintOp[A]], f: Rep[((A) => Unit)])(implicit val manifestA: Manifest[A]) extends FunctionDef[Unit](Some(self), "foreach", List(List(f))) {
    override def curriedConstructor = (copy[A] _).curried
  }

  case class PrintOpFindFirst[A](self: Rep[PrintOp[A]], cond: Rep[((A) => Boolean)])(implicit val manifestA: Manifest[A]) extends FunctionDef[A](Some(self), "findFirst", List(List(cond))) {
    override def curriedConstructor = (copy[A] _).curried
  }

  case class PrintOpNullDynamicRecord[A, D](self: Rep[PrintOp[A]])(implicit val manifestA: Manifest[A], val manifestD: Manifest[D]) extends FunctionDef[D](Some(self), "NullDynamicRecord", List()) {
    override def curriedConstructor = (copy[A, D] _)
  }

  case class PrintOpOpen[A](self: Rep[PrintOp[A]])(implicit val manifestA: Manifest[A]) extends FunctionDef[Unit](Some(self), "open", List()) {
    override def curriedConstructor = (copy[A] _)
  }

  case class PrintOpNext[A](self: Rep[PrintOp[A]])(implicit val manifestA: Manifest[A]) extends FunctionDef[A](Some(self), "next", List()) {
    override def curriedConstructor = (copy[A] _)
  }

  case class PrintOpClose[A](self: Rep[PrintOp[A]])(implicit val manifestA: Manifest[A]) extends FunctionDef[Unit](Some(self), "close", List()) {
    override def curriedConstructor = (copy[A] _)
  }

  case class PrintOpReset[A](self: Rep[PrintOp[A]])(implicit val manifestA: Manifest[A]) extends FunctionDef[Unit](Some(self), "reset", List()) {
    override def curriedConstructor = (copy[A] _)
  }

  case class PrintOp_Field_NumRows_$eq[A](self: Rep[PrintOp[A]], x$1: Rep[Int])(implicit val manifestA: Manifest[A]) extends FieldSetter[Int](self, "numRows", x$1) {
    override def curriedConstructor = (copy[A] _).curried
  }

  case class PrintOp_Field_NumRows[A](self: Rep[PrintOp[A]])(implicit val manifestA: Manifest[A]) extends FieldGetter[Int](self, "numRows") {
    override def curriedConstructor = (copy[A] _)
  }

  case class PrintOp_Field_Evidence$9[A](self: Rep[PrintOp[A]])(implicit val manifestA: Manifest[A]) extends FieldDef[Manifest[A]](self, "evidence$9") {
    override def curriedConstructor = (copy[A] _)
    override def isPure = true

  }

  case class PrintOp_Field_Limit[A](self: Rep[PrintOp[A]])(implicit val manifestA: Manifest[A]) extends FieldDef[(() => Boolean)](self, "limit") {
    override def curriedConstructor = (copy[A] _)
    override def isPure = true

  }

  case class PrintOp_Field_PrintFunc[A](self: Rep[PrintOp[A]])(implicit val manifestA: Manifest[A]) extends FieldDef[(A => Unit)](self, "printFunc") {
    override def curriedConstructor = (copy[A] _)
    override def isPure = true

  }

  case class PrintOp_Field_Parent_$eq[A](self: Rep[PrintOp[A]], x$1: Rep[Operator[A]])(implicit val manifestA: Manifest[A]) extends FieldSetter[Operator[A]](self, "parent", x$1) {
    override def curriedConstructor = (copy[A] _).curried
  }

  case class PrintOp_Field_Parent[A](self: Rep[PrintOp[A]])(implicit val manifestA: Manifest[A]) extends FieldGetter[Operator[A]](self, "parent") {
    override def curriedConstructor = (copy[A] _)
  }

  case class PrintOp_Field_NullDynamicRecord[A](self: Rep[PrintOp[A]])(implicit val manifestA: Manifest[A]) extends FieldDef[A](self, "NullDynamicRecord") {
    override def curriedConstructor = (copy[A] _)
    override def isPure = true

  }

  // method definitions
  def printOpNew[A](parent: Rep[Operator[A]], printFunc: Rep[((A) => Unit)], limit: Rep[(() => Boolean)])(implicit manifestA: Manifest[A]): Rep[PrintOp[A]] = PrintOpNew[A](parent, printFunc, limit)
  def printOpForeach[A](self: Rep[PrintOp[A]], f: Rep[((A) => Unit)])(implicit manifestA: Manifest[A]): Rep[Unit] = PrintOpForeach[A](self, f)
  def printOpFindFirst[A](self: Rep[PrintOp[A]], cond: Rep[((A) => Boolean)])(implicit manifestA: Manifest[A]): Rep[A] = PrintOpFindFirst[A](self, cond)
  def printOpNullDynamicRecord[A, D](self: Rep[PrintOp[A]])(implicit manifestA: Manifest[A], manifestD: Manifest[D]): Rep[D] = PrintOpNullDynamicRecord[A, D](self)
  def printOpOpen[A](self: Rep[PrintOp[A]])(implicit manifestA: Manifest[A]): Rep[Unit] = PrintOpOpen[A](self)
  def printOpNext[A](self: Rep[PrintOp[A]])(implicit manifestA: Manifest[A]): Rep[A] = PrintOpNext[A](self)
  def printOpClose[A](self: Rep[PrintOp[A]])(implicit manifestA: Manifest[A]): Rep[Unit] = PrintOpClose[A](self)
  def printOpReset[A](self: Rep[PrintOp[A]])(implicit manifestA: Manifest[A]): Rep[Unit] = PrintOpReset[A](self)
  def printOp_Field_NumRows_$eq[A](self: Rep[PrintOp[A]], x$1: Rep[Int])(implicit manifestA: Manifest[A]): Rep[Unit] = PrintOp_Field_NumRows_$eq[A](self, x$1)
  def printOp_Field_NumRows[A](self: Rep[PrintOp[A]])(implicit manifestA: Manifest[A]): Rep[Int] = PrintOp_Field_NumRows[A](self)
  def printOp_Field_Evidence$9[A](self: Rep[PrintOp[A]])(implicit manifestA: Manifest[A]): Rep[Manifest[A]] = PrintOp_Field_Evidence$9[A](self)
  def printOp_Field_Limit[A](self: Rep[PrintOp[A]])(implicit manifestA: Manifest[A]): Rep[(() => Boolean)] = PrintOp_Field_Limit[A](self)
  def printOp_Field_PrintFunc[A](self: Rep[PrintOp[A]])(implicit manifestA: Manifest[A]): Rep[(A => Unit)] = PrintOp_Field_PrintFunc[A](self)
  def printOp_Field_Parent_$eq[A](self: Rep[PrintOp[A]], x$1: Rep[Operator[A]])(implicit manifestA: Manifest[A]): Rep[Unit] = PrintOp_Field_Parent_$eq[A](self, x$1)
  def printOp_Field_Parent[A](self: Rep[PrintOp[A]])(implicit manifestA: Manifest[A]): Rep[Operator[A]] = PrintOp_Field_Parent[A](self)
  def printOp_Field_NullDynamicRecord[A](self: Rep[PrintOp[A]])(implicit manifestA: Manifest[A]): Rep[A] = PrintOp_Field_NullDynamicRecord[A](self)
  type PrintOp[A] = ch.epfl.data.legobase.queryengine.volcano.PrintOp[A]
}
trait PrintOpImplicits { this: PrintOpComponent =>
  // Add implicit conversions here!
}
trait PrintOpComponent extends PrintOpOps with PrintOpImplicits { self: OperatorsComponent => }
trait WindowOpOps extends Base { this: OperatorsComponent =>
  implicit class WindowOpRep[A, B, C](self: Rep[WindowOp[A, B, C]])(implicit manifestA: Manifest[A], manifestB: Manifest[B], manifestC: Manifest[C], evidence$15: Manifest[C], evidence$14: Manifest[B], evidence$13: Manifest[A]) {
    def foreach(f: Rep[(WindowRecord[B, C] => Unit)]): Rep[Unit] = windowOpForeach[A, B, C](self, f)(manifestA, manifestB, manifestC)
    def findFirst(cond: Rep[(WindowRecord[B, C] => Boolean)]): Rep[WindowRecord[B, C]] = windowOpFindFirst[A, B, C](self, cond)(manifestA, manifestB, manifestC)
    def NullDynamicRecord[D](implicit manifestD: Manifest[D]): Rep[D] = windowOpNullDynamicRecord[A, B, C, D](self)(manifestA, manifestB, manifestC, manifestD)
    def open(): Rep[Unit] = windowOpOpen[A, B, C](self)(manifestA, manifestB, manifestC)
    def next(): Rep[WindowRecord[B, C]] = windowOpNext[A, B, C](self)(manifestA, manifestB, manifestC)
    def close(): Rep[Unit] = windowOpClose[A, B, C](self)(manifestA, manifestB, manifestC)
    def reset(): Rep[Unit] = windowOpReset[A, B, C](self)(manifestA, manifestB, manifestC)
    def keySet_=(x$1: Rep[Set[B]]): Rep[Unit] = windowOp_Field_KeySet_$eq[A, B, C](self, x$1)(manifestA, manifestB, manifestC)
    def keySet: Rep[Set[B]] = windowOp_Field_KeySet[A, B, C](self)(manifestA, manifestB, manifestC)
    def hm: Rep[HashMap[B, ArrayBuffer[A]]] = windowOp_Field_Hm[A, B, C](self)(manifestA, manifestB, manifestC)
    def evidence$15(): Rep[Manifest[C]] = windowOp_Field_Evidence$15[A, B, C](self)(manifestA, manifestB, manifestC)
    def evidence$14(): Rep[Manifest[B]] = windowOp_Field_Evidence$14[A, B, C](self)(manifestA, manifestB, manifestC)
    def evidence$13(): Rep[Manifest[A]] = windowOp_Field_Evidence$13[A, B, C](self)(manifestA, manifestB, manifestC)
    def wndf: Rep[(ArrayBuffer[A] => C)] = windowOp_Field_Wndf[A, B, C](self)(manifestA, manifestB, manifestC)
    def grp: Rep[(A => B)] = windowOp_Field_Grp[A, B, C](self)(manifestA, manifestB, manifestC)
    def parent: Rep[Operator[A]] = windowOp_Field_Parent[A, B, C](self)(manifestA, manifestB, manifestC)
    def NullDynamicRecord: Rep[WindowRecord[B, C]] = windowOp_Field_NullDynamicRecord[A, B, C](self)(manifestA, manifestB, manifestC)
  }
  // constructors
  def __newWindowOp[A, B, C](parent: Rep[Operator[A]])(grp: Rep[(A => B)])(wndf: Rep[(ArrayBuffer[A] => C)])(implicit evidence$13: Manifest[A], evidence$14: Manifest[B], evidence$15: Manifest[C], manifestA: Manifest[A], manifestB: Manifest[B], manifestC: Manifest[C]): Rep[WindowOp[A, B, C]] = windowOpNew[A, B, C](parent, grp, wndf)(manifestA, manifestB, manifestC)
  // case classes
  case class WindowOpNew[A, B, C](parent: Rep[Operator[A]], grp: Rep[((A) => B)], wndf: Rep[((ArrayBuffer[A]) => C)])(implicit val manifestA: Manifest[A], val manifestB: Manifest[B], val manifestC: Manifest[C]) extends FunctionDef[WindowOp[A, B, C]](None, "new WindowOp", List(List(parent), List(grp), List(wndf))) {
    override def curriedConstructor = (copy[A, B, C] _).curried
  }

  case class WindowOpForeach[A, B, C](self: Rep[WindowOp[A, B, C]], f: Rep[((WindowRecord[B, C]) => Unit)])(implicit val manifestA: Manifest[A], val manifestB: Manifest[B], val manifestC: Manifest[C]) extends FunctionDef[Unit](Some(self), "foreach", List(List(f))) {
    override def curriedConstructor = (copy[A, B, C] _).curried
  }

  case class WindowOpFindFirst[A, B, C](self: Rep[WindowOp[A, B, C]], cond: Rep[((WindowRecord[B, C]) => Boolean)])(implicit val manifestA: Manifest[A], val manifestB: Manifest[B], val manifestC: Manifest[C]) extends FunctionDef[WindowRecord[B, C]](Some(self), "findFirst", List(List(cond))) {
    override def curriedConstructor = (copy[A, B, C] _).curried
  }

  case class WindowOpNullDynamicRecord[A, B, C, D](self: Rep[WindowOp[A, B, C]])(implicit val manifestA: Manifest[A], val manifestB: Manifest[B], val manifestC: Manifest[C], val manifestD: Manifest[D]) extends FunctionDef[D](Some(self), "NullDynamicRecord", List()) {
    override def curriedConstructor = (copy[A, B, C, D] _)
  }

  case class WindowOpOpen[A, B, C](self: Rep[WindowOp[A, B, C]])(implicit val manifestA: Manifest[A], val manifestB: Manifest[B], val manifestC: Manifest[C]) extends FunctionDef[Unit](Some(self), "open", List()) {
    override def curriedConstructor = (copy[A, B, C] _)
  }

  case class WindowOpNext[A, B, C](self: Rep[WindowOp[A, B, C]])(implicit val manifestA: Manifest[A], val manifestB: Manifest[B], val manifestC: Manifest[C]) extends FunctionDef[WindowRecord[B, C]](Some(self), "next", List()) {
    override def curriedConstructor = (copy[A, B, C] _)
  }

  case class WindowOpClose[A, B, C](self: Rep[WindowOp[A, B, C]])(implicit val manifestA: Manifest[A], val manifestB: Manifest[B], val manifestC: Manifest[C]) extends FunctionDef[Unit](Some(self), "close", List()) {
    override def curriedConstructor = (copy[A, B, C] _)
  }

  case class WindowOpReset[A, B, C](self: Rep[WindowOp[A, B, C]])(implicit val manifestA: Manifest[A], val manifestB: Manifest[B], val manifestC: Manifest[C]) extends FunctionDef[Unit](Some(self), "reset", List()) {
    override def curriedConstructor = (copy[A, B, C] _)
  }

  case class WindowOp_Field_KeySet_$eq[A, B, C](self: Rep[WindowOp[A, B, C]], x$1: Rep[Set[B]])(implicit val manifestA: Manifest[A], val manifestB: Manifest[B], val manifestC: Manifest[C]) extends FieldSetter[Set[B]](self, "keySet", x$1) {
    override def curriedConstructor = (copy[A, B, C] _).curried
  }

  case class WindowOp_Field_KeySet[A, B, C](self: Rep[WindowOp[A, B, C]])(implicit val manifestA: Manifest[A], val manifestB: Manifest[B], val manifestC: Manifest[C]) extends FieldGetter[Set[B]](self, "keySet") {
    override def curriedConstructor = (copy[A, B, C] _)
  }

  case class WindowOp_Field_Hm[A, B, C](self: Rep[WindowOp[A, B, C]])(implicit val manifestA: Manifest[A], val manifestB: Manifest[B], val manifestC: Manifest[C]) extends FieldDef[HashMap[B, ArrayBuffer[A]]](self, "hm") {
    override def curriedConstructor = (copy[A, B, C] _)
    override def isPure = true

  }

  case class WindowOp_Field_Evidence$15[A, B, C](self: Rep[WindowOp[A, B, C]])(implicit val manifestA: Manifest[A], val manifestB: Manifest[B], val manifestC: Manifest[C]) extends FieldDef[Manifest[C]](self, "evidence$15") {
    override def curriedConstructor = (copy[A, B, C] _)
    override def isPure = true

  }

  case class WindowOp_Field_Evidence$14[A, B, C](self: Rep[WindowOp[A, B, C]])(implicit val manifestA: Manifest[A], val manifestB: Manifest[B], val manifestC: Manifest[C]) extends FieldDef[Manifest[B]](self, "evidence$14") {
    override def curriedConstructor = (copy[A, B, C] _)
    override def isPure = true

  }

  case class WindowOp_Field_Evidence$13[A, B, C](self: Rep[WindowOp[A, B, C]])(implicit val manifestA: Manifest[A], val manifestB: Manifest[B], val manifestC: Manifest[C]) extends FieldDef[Manifest[A]](self, "evidence$13") {
    override def curriedConstructor = (copy[A, B, C] _)
    override def isPure = true

  }

  case class WindowOp_Field_Wndf[A, B, C](self: Rep[WindowOp[A, B, C]])(implicit val manifestA: Manifest[A], val manifestB: Manifest[B], val manifestC: Manifest[C]) extends FieldDef[(ArrayBuffer[A] => C)](self, "wndf") {
    override def curriedConstructor = (copy[A, B, C] _)
    override def isPure = true

  }

  case class WindowOp_Field_Grp[A, B, C](self: Rep[WindowOp[A, B, C]])(implicit val manifestA: Manifest[A], val manifestB: Manifest[B], val manifestC: Manifest[C]) extends FieldDef[(A => B)](self, "grp") {
    override def curriedConstructor = (copy[A, B, C] _)
    override def isPure = true

  }

  case class WindowOp_Field_Parent[A, B, C](self: Rep[WindowOp[A, B, C]])(implicit val manifestA: Manifest[A], val manifestB: Manifest[B], val manifestC: Manifest[C]) extends FieldDef[Operator[A]](self, "parent") {
    override def curriedConstructor = (copy[A, B, C] _)
    override def isPure = true

  }

  case class WindowOp_Field_NullDynamicRecord[A, B, C](self: Rep[WindowOp[A, B, C]])(implicit val manifestA: Manifest[A], val manifestB: Manifest[B], val manifestC: Manifest[C]) extends FieldDef[WindowRecord[B, C]](self, "NullDynamicRecord") {
    override def curriedConstructor = (copy[A, B, C] _)
    override def isPure = true

  }

  // method definitions
  def windowOpNew[A, B, C](parent: Rep[Operator[A]], grp: Rep[((A) => B)], wndf: Rep[((ArrayBuffer[A]) => C)])(implicit manifestA: Manifest[A], manifestB: Manifest[B], manifestC: Manifest[C]): Rep[WindowOp[A, B, C]] = WindowOpNew[A, B, C](parent, grp, wndf)
  def windowOpForeach[A, B, C](self: Rep[WindowOp[A, B, C]], f: Rep[((WindowRecord[B, C]) => Unit)])(implicit manifestA: Manifest[A], manifestB: Manifest[B], manifestC: Manifest[C]): Rep[Unit] = WindowOpForeach[A, B, C](self, f)
  def windowOpFindFirst[A, B, C](self: Rep[WindowOp[A, B, C]], cond: Rep[((WindowRecord[B, C]) => Boolean)])(implicit manifestA: Manifest[A], manifestB: Manifest[B], manifestC: Manifest[C]): Rep[WindowRecord[B, C]] = WindowOpFindFirst[A, B, C](self, cond)
  def windowOpNullDynamicRecord[A, B, C, D](self: Rep[WindowOp[A, B, C]])(implicit manifestA: Manifest[A], manifestB: Manifest[B], manifestC: Manifest[C], manifestD: Manifest[D]): Rep[D] = WindowOpNullDynamicRecord[A, B, C, D](self)
  def windowOpOpen[A, B, C](self: Rep[WindowOp[A, B, C]])(implicit manifestA: Manifest[A], manifestB: Manifest[B], manifestC: Manifest[C]): Rep[Unit] = WindowOpOpen[A, B, C](self)
  def windowOpNext[A, B, C](self: Rep[WindowOp[A, B, C]])(implicit manifestA: Manifest[A], manifestB: Manifest[B], manifestC: Manifest[C]): Rep[WindowRecord[B, C]] = WindowOpNext[A, B, C](self)
  def windowOpClose[A, B, C](self: Rep[WindowOp[A, B, C]])(implicit manifestA: Manifest[A], manifestB: Manifest[B], manifestC: Manifest[C]): Rep[Unit] = WindowOpClose[A, B, C](self)
  def windowOpReset[A, B, C](self: Rep[WindowOp[A, B, C]])(implicit manifestA: Manifest[A], manifestB: Manifest[B], manifestC: Manifest[C]): Rep[Unit] = WindowOpReset[A, B, C](self)
  def windowOp_Field_KeySet_$eq[A, B, C](self: Rep[WindowOp[A, B, C]], x$1: Rep[Set[B]])(implicit manifestA: Manifest[A], manifestB: Manifest[B], manifestC: Manifest[C]): Rep[Unit] = WindowOp_Field_KeySet_$eq[A, B, C](self, x$1)
  def windowOp_Field_KeySet[A, B, C](self: Rep[WindowOp[A, B, C]])(implicit manifestA: Manifest[A], manifestB: Manifest[B], manifestC: Manifest[C]): Rep[Set[B]] = WindowOp_Field_KeySet[A, B, C](self)
  def windowOp_Field_Hm[A, B, C](self: Rep[WindowOp[A, B, C]])(implicit manifestA: Manifest[A], manifestB: Manifest[B], manifestC: Manifest[C]): Rep[HashMap[B, ArrayBuffer[A]]] = WindowOp_Field_Hm[A, B, C](self)
  def windowOp_Field_Evidence$15[A, B, C](self: Rep[WindowOp[A, B, C]])(implicit manifestA: Manifest[A], manifestB: Manifest[B], manifestC: Manifest[C]): Rep[Manifest[C]] = WindowOp_Field_Evidence$15[A, B, C](self)
  def windowOp_Field_Evidence$14[A, B, C](self: Rep[WindowOp[A, B, C]])(implicit manifestA: Manifest[A], manifestB: Manifest[B], manifestC: Manifest[C]): Rep[Manifest[B]] = WindowOp_Field_Evidence$14[A, B, C](self)
  def windowOp_Field_Evidence$13[A, B, C](self: Rep[WindowOp[A, B, C]])(implicit manifestA: Manifest[A], manifestB: Manifest[B], manifestC: Manifest[C]): Rep[Manifest[A]] = WindowOp_Field_Evidence$13[A, B, C](self)
  def windowOp_Field_Wndf[A, B, C](self: Rep[WindowOp[A, B, C]])(implicit manifestA: Manifest[A], manifestB: Manifest[B], manifestC: Manifest[C]): Rep[(ArrayBuffer[A] => C)] = WindowOp_Field_Wndf[A, B, C](self)
  def windowOp_Field_Grp[A, B, C](self: Rep[WindowOp[A, B, C]])(implicit manifestA: Manifest[A], manifestB: Manifest[B], manifestC: Manifest[C]): Rep[(A => B)] = WindowOp_Field_Grp[A, B, C](self)
  def windowOp_Field_Parent[A, B, C](self: Rep[WindowOp[A, B, C]])(implicit manifestA: Manifest[A], manifestB: Manifest[B], manifestC: Manifest[C]): Rep[Operator[A]] = WindowOp_Field_Parent[A, B, C](self)
  def windowOp_Field_NullDynamicRecord[A, B, C](self: Rep[WindowOp[A, B, C]])(implicit manifestA: Manifest[A], manifestB: Manifest[B], manifestC: Manifest[C]): Rep[WindowRecord[B, C]] = WindowOp_Field_NullDynamicRecord[A, B, C](self)
  type WindowOp[A, B, C] = ch.epfl.data.legobase.queryengine.volcano.WindowOp[A, B, C]
}
trait WindowOpImplicits { this: WindowOpComponent =>
  // Add implicit conversions here!
}
trait WindowOpComponent extends WindowOpOps with WindowOpImplicits { self: OperatorsComponent => }
trait OperatorsComponent extends OperatorComponent with ScanOpComponent with SelectOpComponent with AggOpComponent with SortOpComponent with MapOpComponent with PrintOpComponent with WindowOpComponent with AGGRecordComponent with WindowRecordComponent with CharacterComponent with DoubleComponent with IntComponent with LongComponent with ArrayComponent with LINEITEMRecordComponent with K2DBScannerComponent with IntegerComponent with BooleanComponent with HashMapComponent with SetComponent with TreeSetComponent with DefaultEntryComponent with ArrayBufferComponent with ManualLiftedLegoBase { self: DeepDSL => }