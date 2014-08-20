
package ch.epfl.data
package legobase
package deep

import scalalib._
import pardis.ir._
import pardis.ir.pardisTypeImplicits._
import pardis.deep.scalalib._
trait OperatorOps extends Base { this: OperatorsComponent =>
  implicit class OperatorRep[A](self: Rep[Operator[A]])(implicit typeA: TypeRep[A]) {
    def open(): Rep[Unit] = operatorOpen[A](self)(typeA)
    def next(): Rep[A] = operatorNext[A](self)(typeA)
    def close(): Rep[Unit] = operatorClose[A](self)(typeA)
    def reset(): Rep[Unit] = operatorReset[A](self)(typeA)
    def foreach(f: Rep[(A => Unit)]): Rep[Unit] = operatorForeach[A](self, f)(typeA)
    def findFirst(cond: Rep[(A => Boolean)]): Rep[A] = operatorFindFirst[A](self, cond)(typeA)
    def NullDynamicRecord[D](implicit typeD: TypeRep[D], di: DummyImplicit): Rep[D] = operatorNullDynamicRecord[A, D](self)(typeA, typeD, di)
    def NullDynamicRecord: Rep[A] = operator_Field_NullDynamicRecord[A](self)(typeA)
  }
  object Operator {

  }
  // constructors

  // case classes
  case class OperatorOpen[A](self: Rep[Operator[A]])(implicit val typeA: TypeRep[A]) extends FunctionDef[Unit](Some(self), "open", List(List())) {
    override def curriedConstructor = (copy[A] _)
  }

  case class OperatorNext[A](self: Rep[Operator[A]])(implicit val typeA: TypeRep[A]) extends FunctionDef[A](Some(self), "next", List(List())) {
    override def curriedConstructor = (copy[A] _)
  }

  case class OperatorClose[A](self: Rep[Operator[A]])(implicit val typeA: TypeRep[A]) extends FunctionDef[Unit](Some(self), "close", List(List())) {
    override def curriedConstructor = (copy[A] _)
  }

  case class OperatorReset[A](self: Rep[Operator[A]])(implicit val typeA: TypeRep[A]) extends FunctionDef[Unit](Some(self), "reset", List(List())) {
    override def curriedConstructor = (copy[A] _)
  }

  case class OperatorForeach[A](self: Rep[Operator[A]], f: Rep[((A) => Unit)])(implicit val typeA: TypeRep[A]) extends FunctionDef[Unit](Some(self), "foreach", List(List(f))) {
    override def curriedConstructor = (copy[A] _).curried
  }

  case class OperatorFindFirst[A](self: Rep[Operator[A]], cond: Rep[((A) => Boolean)])(implicit val typeA: TypeRep[A]) extends FunctionDef[A](Some(self), "findFirst", List(List(cond))) {
    override def curriedConstructor = (copy[A] _).curried
  }

  case class OperatorNullDynamicRecord[A, D](self: Rep[Operator[A]])(implicit val typeA: TypeRep[A], val typeD: TypeRep[D], val di: DummyImplicit) extends FunctionDef[D](Some(self), "NullDynamicRecord", List()) {
    override def curriedConstructor = (copy[A, D] _)
  }

  case class Operator_Field_NullDynamicRecord[A](self: Rep[Operator[A]])(implicit val typeA: TypeRep[A]) extends FieldDef[A](self, "NullDynamicRecord") {
    override def curriedConstructor = (copy[A] _)
    override def isPure = true

  }

  // method definitions
  def operatorOpen[A](self: Rep[Operator[A]])(implicit typeA: TypeRep[A]): Rep[Unit] = OperatorOpen[A](self)
  def operatorNext[A](self: Rep[Operator[A]])(implicit typeA: TypeRep[A]): Rep[A] = OperatorNext[A](self)
  def operatorClose[A](self: Rep[Operator[A]])(implicit typeA: TypeRep[A]): Rep[Unit] = OperatorClose[A](self)
  def operatorReset[A](self: Rep[Operator[A]])(implicit typeA: TypeRep[A]): Rep[Unit] = OperatorReset[A](self)
  def operatorForeach[A](self: Rep[Operator[A]], f: Rep[((A) => Unit)])(implicit typeA: TypeRep[A]): Rep[Unit] = OperatorForeach[A](self, f)
  def operatorFindFirst[A](self: Rep[Operator[A]], cond: Rep[((A) => Boolean)])(implicit typeA: TypeRep[A]): Rep[A] = OperatorFindFirst[A](self, cond)
  def operatorNullDynamicRecord[A, D](self: Rep[Operator[A]])(implicit typeA: TypeRep[A], typeD: TypeRep[D], di: DummyImplicit): Rep[D] = OperatorNullDynamicRecord[A, D](self)
  def operator_Field_NullDynamicRecord[A](self: Rep[Operator[A]])(implicit typeA: TypeRep[A]): Rep[A] = Operator_Field_NullDynamicRecord[A](self)
  type Operator[A] = ch.epfl.data.legobase.queryengine.volcano.Operator[A]
  case class OperatorType[A](typeA: TypeRep[A]) extends TypeRep[Operator[A]] {
    def rebuild(newArguments: TypeRep[_]*): TypeRep[_] = OperatorType(newArguments(0).asInstanceOf[TypeRep[_]])
    private implicit val tagA = typeA.typeTag
    val name = s"Operator[${typeA.name}]"
    val typeArguments = List(typeA)

    val typeTag = scala.reflect.runtime.universe.typeTag[Operator[A]]
  }
  implicit def typeOperator[A: TypeRep] = OperatorType(implicitly[TypeRep[A]])
}
trait OperatorImplicits { this: OperatorComponent =>
  // Add implicit conversions here!
}
trait OperatorImplementations { self: DeepDSL =>
  override def operatorForeach[A](self: Rep[Operator[A]], f: Rep[((A) => Unit)])(implicit typeA: TypeRep[A]): Rep[Unit] = {
    {
      var exit: this.Var[Boolean] = __newVar(unit(false));
      __whileDo(infix_$bang$eq(readVar(exit), unit(true)), {
        val t: this.Rep[A] = self.next();
        __ifThenElse(infix_$eq$eq(t, self.NullDynamicRecord), __assign(exit, unit(true)), {
          __app(f).apply(t);
          unit(())
        })
      })
    }
  }
  override def operatorFindFirst[A](self: Rep[Operator[A]], cond: Rep[((A) => Boolean)])(implicit typeA: TypeRep[A]): Rep[A] = {
    {
      var exit: this.Var[Boolean] = __newVar(unit(false));
      var res: this.Var[A] = __newVar(self.NullDynamicRecord);
      __whileDo(infix_$bang$eq(readVar(exit), unit(true)), {
        __assign(res, self.next());
        __ifThenElse(infix_$eq$eq(readVar(res), self.NullDynamicRecord), __assign(exit, unit(true)), __assign(exit, __app(cond).apply(res)))
      });
      readVar(res)
    }
  }
  override def operatorNullDynamicRecord[A, D](self: Rep[Operator[A]])(implicit typeA: TypeRep[A], typeD: TypeRep[D], di: DummyImplicit): Rep[D] = {
    infix_asInstanceOf[D](unit(null))
  }
}
trait OperatorComponent extends OperatorOps with OperatorImplicits { self: OperatorsComponent => }
trait ScanOpOps extends Base { this: OperatorsComponent =>
  implicit class ScanOpRep[A](self: Rep[ScanOp[A]])(implicit typeA: TypeRep[A]) {
    def foreach(f: Rep[(A => Unit)]): Rep[Unit] = scanOpForeach[A](self, f)(typeA)
    def findFirst(cond: Rep[(A => Boolean)]): Rep[A] = scanOpFindFirst[A](self, cond)(typeA)
    def NullDynamicRecord[D](implicit typeD: TypeRep[D], di: DummyImplicit): Rep[D] = scanOpNullDynamicRecord[A, D](self)(typeA, typeD, di)
    def open(): Rep[Unit] = scanOpOpen[A](self)(typeA)
    def next(): Rep[A] = scanOpNext[A](self)(typeA)
    def close(): Rep[Unit] = scanOpClose[A](self)(typeA)
    def reset(): Rep[Unit] = scanOpReset[A](self)(typeA)
    def i_=(x$1: Rep[Int]): Rep[Unit] = scanOp_Field_I_$eq[A](self, x$1)(typeA)
    def i: Rep[Int] = scanOp_Field_I[A](self)(typeA)
    def table: Rep[Array[A]] = scanOp_Field_Table[A](self)(typeA)
    def NullDynamicRecord: Rep[A] = scanOp_Field_NullDynamicRecord[A](self)(typeA)
  }
  object ScanOp {

  }
  // constructors
  def __newScanOp[A](table: Rep[Array[A]])(implicit typeA: TypeRep[A]): Rep[ScanOp[A]] = scanOpNew[A](table)(typeA)
  // case classes
  case class ScanOpNew[A](table: Rep[Array[A]])(implicit val typeA: TypeRep[A]) extends ConstructorDef[ScanOp[A]](List(typeA), "ScanOp", List(List(table))) {
    override def curriedConstructor = (copy[A] _)
  }

  case class ScanOpForeach[A](self: Rep[ScanOp[A]], f: Rep[((A) => Unit)])(implicit val typeA: TypeRep[A]) extends FunctionDef[Unit](Some(self), "foreach", List(List(f))) {
    override def curriedConstructor = (copy[A] _).curried
  }

  case class ScanOpFindFirst[A](self: Rep[ScanOp[A]], cond: Rep[((A) => Boolean)])(implicit val typeA: TypeRep[A]) extends FunctionDef[A](Some(self), "findFirst", List(List(cond))) {
    override def curriedConstructor = (copy[A] _).curried
  }

  case class ScanOpNullDynamicRecord[A, D](self: Rep[ScanOp[A]])(implicit val typeA: TypeRep[A], val typeD: TypeRep[D], val di: DummyImplicit) extends FunctionDef[D](Some(self), "NullDynamicRecord", List()) {
    override def curriedConstructor = (copy[A, D] _)
  }

  case class ScanOpOpen[A](self: Rep[ScanOp[A]])(implicit val typeA: TypeRep[A]) extends FunctionDef[Unit](Some(self), "open", List(List())) {
    override def curriedConstructor = (copy[A] _)
  }

  case class ScanOpNext[A](self: Rep[ScanOp[A]])(implicit val typeA: TypeRep[A]) extends FunctionDef[A](Some(self), "next", List(List())) {
    override def curriedConstructor = (copy[A] _)
  }

  case class ScanOpClose[A](self: Rep[ScanOp[A]])(implicit val typeA: TypeRep[A]) extends FunctionDef[Unit](Some(self), "close", List(List())) {
    override def curriedConstructor = (copy[A] _)
  }

  case class ScanOpReset[A](self: Rep[ScanOp[A]])(implicit val typeA: TypeRep[A]) extends FunctionDef[Unit](Some(self), "reset", List(List())) {
    override def curriedConstructor = (copy[A] _)
  }

  case class ScanOp_Field_I_$eq[A](self: Rep[ScanOp[A]], x$1: Rep[Int])(implicit val typeA: TypeRep[A]) extends FieldSetter[Int](self, "i", x$1) {
    override def curriedConstructor = (copy[A] _).curried
  }

  case class ScanOp_Field_I[A](self: Rep[ScanOp[A]])(implicit val typeA: TypeRep[A]) extends FieldGetter[Int](self, "i") {
    override def curriedConstructor = (copy[A] _)
  }

  case class ScanOp_Field_Table[A](self: Rep[ScanOp[A]])(implicit val typeA: TypeRep[A]) extends FieldDef[Array[A]](self, "table") {
    override def curriedConstructor = (copy[A] _)
    override def isPure = true

  }

  case class ScanOp_Field_NullDynamicRecord[A](self: Rep[ScanOp[A]])(implicit val typeA: TypeRep[A]) extends FieldDef[A](self, "NullDynamicRecord") {
    override def curriedConstructor = (copy[A] _)
    override def isPure = true

  }

  // method definitions
  def scanOpNew[A](table: Rep[Array[A]])(implicit typeA: TypeRep[A]): Rep[ScanOp[A]] = ScanOpNew[A](table)
  def scanOpForeach[A](self: Rep[ScanOp[A]], f: Rep[((A) => Unit)])(implicit typeA: TypeRep[A]): Rep[Unit] = ScanOpForeach[A](self, f)
  def scanOpFindFirst[A](self: Rep[ScanOp[A]], cond: Rep[((A) => Boolean)])(implicit typeA: TypeRep[A]): Rep[A] = ScanOpFindFirst[A](self, cond)
  def scanOpNullDynamicRecord[A, D](self: Rep[ScanOp[A]])(implicit typeA: TypeRep[A], typeD: TypeRep[D], di: DummyImplicit): Rep[D] = ScanOpNullDynamicRecord[A, D](self)
  def scanOpOpen[A](self: Rep[ScanOp[A]])(implicit typeA: TypeRep[A]): Rep[Unit] = ScanOpOpen[A](self)
  def scanOpNext[A](self: Rep[ScanOp[A]])(implicit typeA: TypeRep[A]): Rep[A] = ScanOpNext[A](self)
  def scanOpClose[A](self: Rep[ScanOp[A]])(implicit typeA: TypeRep[A]): Rep[Unit] = ScanOpClose[A](self)
  def scanOpReset[A](self: Rep[ScanOp[A]])(implicit typeA: TypeRep[A]): Rep[Unit] = ScanOpReset[A](self)
  def scanOp_Field_I_$eq[A](self: Rep[ScanOp[A]], x$1: Rep[Int])(implicit typeA: TypeRep[A]): Rep[Unit] = ScanOp_Field_I_$eq[A](self, x$1)
  def scanOp_Field_I[A](self: Rep[ScanOp[A]])(implicit typeA: TypeRep[A]): Rep[Int] = ScanOp_Field_I[A](self)
  def scanOp_Field_Table[A](self: Rep[ScanOp[A]])(implicit typeA: TypeRep[A]): Rep[Array[A]] = ScanOp_Field_Table[A](self)
  def scanOp_Field_NullDynamicRecord[A](self: Rep[ScanOp[A]])(implicit typeA: TypeRep[A]): Rep[A] = ScanOp_Field_NullDynamicRecord[A](self)
  type ScanOp[A] = ch.epfl.data.legobase.queryengine.volcano.ScanOp[A]
  case class ScanOpType[A](typeA: TypeRep[A]) extends TypeRep[ScanOp[A]] {
    def rebuild(newArguments: TypeRep[_]*): TypeRep[_] = ScanOpType(newArguments(0).asInstanceOf[TypeRep[_]])
    private implicit val tagA = typeA.typeTag
    val name = s"ScanOp[${typeA.name}]"
    val typeArguments = List(typeA)

    val typeTag = scala.reflect.runtime.universe.typeTag[ScanOp[A]]
  }
  implicit def typeScanOp[A: TypeRep] = ScanOpType(implicitly[TypeRep[A]])
}
trait ScanOpImplicits { this: ScanOpComponent =>
  // Add implicit conversions here!
}
trait ScanOpImplementations { self: DeepDSL =>
  override def scanOpOpen[A](self: Rep[ScanOp[A]])(implicit typeA: TypeRep[A]): Rep[Unit] = {
    unit(())
  }
  override def scanOpNext[A](self: Rep[ScanOp[A]])(implicit typeA: TypeRep[A]): Rep[A] = {
    __ifThenElse(self.i.$less(self.table.length), {
      val v: this.Rep[A] = self.table.apply(self.i);
      self.i_$eq(self.i.$plus(unit(1)));
      v
    }, self.NullDynamicRecord)
  }
  override def scanOpClose[A](self: Rep[ScanOp[A]])(implicit typeA: TypeRep[A]): Rep[Unit] = {
    unit(())
  }
  override def scanOpReset[A](self: Rep[ScanOp[A]])(implicit typeA: TypeRep[A]): Rep[Unit] = {
    self.i_$eq(unit(0))
  }
}
trait ScanOpComponent extends ScanOpOps with ScanOpImplicits { self: OperatorsComponent => }
trait SelectOpOps extends Base { this: OperatorsComponent =>
  implicit class SelectOpRep[A](self: Rep[SelectOp[A]])(implicit typeA: TypeRep[A]) {
    def foreach(f: Rep[(A => Unit)]): Rep[Unit] = selectOpForeach[A](self, f)(typeA)
    def findFirst(cond: Rep[(A => Boolean)]): Rep[A] = selectOpFindFirst[A](self, cond)(typeA)
    def NullDynamicRecord[D](implicit typeD: TypeRep[D], di: DummyImplicit): Rep[D] = selectOpNullDynamicRecord[A, D](self)(typeA, typeD, di)
    def open(): Rep[Unit] = selectOpOpen[A](self)(typeA)
    def next(): Rep[A] = selectOpNext[A](self)(typeA)
    def close(): Rep[Unit] = selectOpClose[A](self)(typeA)
    def reset(): Rep[Unit] = selectOpReset[A](self)(typeA)
    def selectPred: Rep[(A => Boolean)] = selectOp_Field_SelectPred[A](self)(typeA)
    def parent: Rep[Operator[A]] = selectOp_Field_Parent[A](self)(typeA)
    def NullDynamicRecord: Rep[A] = selectOp_Field_NullDynamicRecord[A](self)(typeA)
  }
  object SelectOp {

  }
  // constructors
  def __newSelectOp[A](parent: Rep[Operator[A]])(selectPred: Rep[(A => Boolean)])(implicit typeA: TypeRep[A]): Rep[SelectOp[A]] = selectOpNew[A](parent, selectPred)(typeA)
  // case classes
  case class SelectOpNew[A](parent: Rep[Operator[A]], selectPred: Rep[((A) => Boolean)])(implicit val typeA: TypeRep[A]) extends ConstructorDef[SelectOp[A]](List(typeA), "SelectOp", List(List(parent), List(selectPred))) {
    override def curriedConstructor = (copy[A] _).curried
  }

  case class SelectOpForeach[A](self: Rep[SelectOp[A]], f: Rep[((A) => Unit)])(implicit val typeA: TypeRep[A]) extends FunctionDef[Unit](Some(self), "foreach", List(List(f))) {
    override def curriedConstructor = (copy[A] _).curried
  }

  case class SelectOpFindFirst[A](self: Rep[SelectOp[A]], cond: Rep[((A) => Boolean)])(implicit val typeA: TypeRep[A]) extends FunctionDef[A](Some(self), "findFirst", List(List(cond))) {
    override def curriedConstructor = (copy[A] _).curried
  }

  case class SelectOpNullDynamicRecord[A, D](self: Rep[SelectOp[A]])(implicit val typeA: TypeRep[A], val typeD: TypeRep[D], val di: DummyImplicit) extends FunctionDef[D](Some(self), "NullDynamicRecord", List()) {
    override def curriedConstructor = (copy[A, D] _)
  }

  case class SelectOpOpen[A](self: Rep[SelectOp[A]])(implicit val typeA: TypeRep[A]) extends FunctionDef[Unit](Some(self), "open", List(List())) {
    override def curriedConstructor = (copy[A] _)
  }

  case class SelectOpNext[A](self: Rep[SelectOp[A]])(implicit val typeA: TypeRep[A]) extends FunctionDef[A](Some(self), "next", List(List())) {
    override def curriedConstructor = (copy[A] _)
  }

  case class SelectOpClose[A](self: Rep[SelectOp[A]])(implicit val typeA: TypeRep[A]) extends FunctionDef[Unit](Some(self), "close", List(List())) {
    override def curriedConstructor = (copy[A] _)
  }

  case class SelectOpReset[A](self: Rep[SelectOp[A]])(implicit val typeA: TypeRep[A]) extends FunctionDef[Unit](Some(self), "reset", List(List())) {
    override def curriedConstructor = (copy[A] _)
  }

  case class SelectOp_Field_SelectPred[A](self: Rep[SelectOp[A]])(implicit val typeA: TypeRep[A]) extends FieldDef[(A => Boolean)](self, "selectPred") {
    override def curriedConstructor = (copy[A] _)
    override def isPure = true

  }

  case class SelectOp_Field_Parent[A](self: Rep[SelectOp[A]])(implicit val typeA: TypeRep[A]) extends FieldDef[Operator[A]](self, "parent") {
    override def curriedConstructor = (copy[A] _)
    override def isPure = true

  }

  case class SelectOp_Field_NullDynamicRecord[A](self: Rep[SelectOp[A]])(implicit val typeA: TypeRep[A]) extends FieldDef[A](self, "NullDynamicRecord") {
    override def curriedConstructor = (copy[A] _)
    override def isPure = true

  }

  // method definitions
  def selectOpNew[A](parent: Rep[Operator[A]], selectPred: Rep[((A) => Boolean)])(implicit typeA: TypeRep[A]): Rep[SelectOp[A]] = SelectOpNew[A](parent, selectPred)
  def selectOpForeach[A](self: Rep[SelectOp[A]], f: Rep[((A) => Unit)])(implicit typeA: TypeRep[A]): Rep[Unit] = SelectOpForeach[A](self, f)
  def selectOpFindFirst[A](self: Rep[SelectOp[A]], cond: Rep[((A) => Boolean)])(implicit typeA: TypeRep[A]): Rep[A] = SelectOpFindFirst[A](self, cond)
  def selectOpNullDynamicRecord[A, D](self: Rep[SelectOp[A]])(implicit typeA: TypeRep[A], typeD: TypeRep[D], di: DummyImplicit): Rep[D] = SelectOpNullDynamicRecord[A, D](self)
  def selectOpOpen[A](self: Rep[SelectOp[A]])(implicit typeA: TypeRep[A]): Rep[Unit] = SelectOpOpen[A](self)
  def selectOpNext[A](self: Rep[SelectOp[A]])(implicit typeA: TypeRep[A]): Rep[A] = SelectOpNext[A](self)
  def selectOpClose[A](self: Rep[SelectOp[A]])(implicit typeA: TypeRep[A]): Rep[Unit] = SelectOpClose[A](self)
  def selectOpReset[A](self: Rep[SelectOp[A]])(implicit typeA: TypeRep[A]): Rep[Unit] = SelectOpReset[A](self)
  def selectOp_Field_SelectPred[A](self: Rep[SelectOp[A]])(implicit typeA: TypeRep[A]): Rep[(A => Boolean)] = SelectOp_Field_SelectPred[A](self)
  def selectOp_Field_Parent[A](self: Rep[SelectOp[A]])(implicit typeA: TypeRep[A]): Rep[Operator[A]] = SelectOp_Field_Parent[A](self)
  def selectOp_Field_NullDynamicRecord[A](self: Rep[SelectOp[A]])(implicit typeA: TypeRep[A]): Rep[A] = SelectOp_Field_NullDynamicRecord[A](self)
  type SelectOp[A] = ch.epfl.data.legobase.queryengine.volcano.SelectOp[A]
  case class SelectOpType[A](typeA: TypeRep[A]) extends TypeRep[SelectOp[A]] {
    def rebuild(newArguments: TypeRep[_]*): TypeRep[_] = SelectOpType(newArguments(0).asInstanceOf[TypeRep[_]])
    private implicit val tagA = typeA.typeTag
    val name = s"SelectOp[${typeA.name}]"
    val typeArguments = List(typeA)

    val typeTag = scala.reflect.runtime.universe.typeTag[SelectOp[A]]
  }
  implicit def typeSelectOp[A: TypeRep] = SelectOpType(implicitly[TypeRep[A]])
}
trait SelectOpImplicits { this: SelectOpComponent =>
  // Add implicit conversions here!
}
trait SelectOpImplementations { self: DeepDSL =>
  override def selectOpOpen[A](self: Rep[SelectOp[A]])(implicit typeA: TypeRep[A]): Rep[Unit] = {
    self.parent.open()
  }
  override def selectOpNext[A](self: Rep[SelectOp[A]])(implicit typeA: TypeRep[A]): Rep[A] = {
    self.parent.findFirst(self.selectPred)
  }
  override def selectOpClose[A](self: Rep[SelectOp[A]])(implicit typeA: TypeRep[A]): Rep[Unit] = {
    unit(())
  }
  override def selectOpReset[A](self: Rep[SelectOp[A]])(implicit typeA: TypeRep[A]): Rep[Unit] = {
    self.parent.reset()
  }
}
trait SelectOpComponent extends SelectOpOps with SelectOpImplicits { self: OperatorsComponent => }
trait AggOpOps extends Base { this: OperatorsComponent =>
  implicit class AggOpRep[A, B](self: Rep[AggOp[A, B]])(implicit typeA: TypeRep[A], typeB: TypeRep[B]) {
    def foreach(f: Rep[(AGGRecord[B] => Unit)]): Rep[Unit] = aggOpForeach[A, B](self, f)(typeA, typeB)
    def findFirst(cond: Rep[(AGGRecord[B] => Boolean)]): Rep[AGGRecord[B]] = aggOpFindFirst[A, B](self, cond)(typeA, typeB)
    def NullDynamicRecord[D](implicit typeD: TypeRep[D], di: DummyImplicit): Rep[D] = aggOpNullDynamicRecord[A, B, D](self)(typeA, typeB, typeD, di)
    def open(): Rep[Unit] = aggOpOpen[A, B](self)(typeA, typeB)
    def next(): Rep[AGGRecord[B]] = aggOpNext[A, B](self)(typeA, typeB)
    def close(): Rep[Unit] = aggOpClose[A, B](self)(typeA, typeB)
    def reset(): Rep[Unit] = aggOpReset[A, B](self)(typeA, typeB)
    def keySet_=(x$1: Rep[Set[B]]): Rep[Unit] = aggOp_Field_KeySet_$eq[A, B](self, x$1)(typeA, typeB)
    def keySet: Rep[Set[B]] = aggOp_Field_KeySet[A, B](self)(typeA, typeB)
    def hm: Rep[HashMap[B, Array[Double]]] = aggOp_Field_Hm[A, B](self)(typeA, typeB)
    def aggFuncs: Rep[Seq[((A, Double) => Double)]] = aggOp_Field_AggFuncs[A, B](self)(typeA, typeB)
    def grp: Rep[(A => B)] = aggOp_Field_Grp[A, B](self)(typeA, typeB)
    def numAggs: Rep[Int] = aggOp_Field_NumAggs[A, B](self)(typeA, typeB)
    def parent: Rep[Operator[A]] = aggOp_Field_Parent[A, B](self)(typeA, typeB)
    def NullDynamicRecord: Rep[AGGRecord[B]] = aggOp_Field_NullDynamicRecord[A, B](self)(typeA, typeB)
  }
  object AggOp {

  }
  // constructors
  def __newAggOp[A, B](parent: Rep[Operator[A]], numAggs: Rep[Int])(grp: Rep[(A => B)])(aggFuncs: Rep[((A, Double) => Double)]*)(implicit typeA: TypeRep[A], typeB: TypeRep[B]): Rep[AggOp[A, B]] = aggOpNew[A, B](parent, numAggs, grp, aggFuncs: _*)(typeA, typeB)
  // case classes
  case class AggOpNew[A, B](parent: Rep[Operator[A]], numAggs: Rep[Int], grp: Rep[((A) => B)], aggFuncsOutput: Rep[Seq[((A, Double) => Double)]])(implicit val typeA: TypeRep[A], val typeB: TypeRep[B]) extends ConstructorDef[AggOp[A, B]](List(typeA, typeB), "AggOp", List(List(parent, numAggs), List(grp), List(__varArg(aggFuncsOutput)))) {
    override def curriedConstructor = (copy[A, B] _).curried
  }

  case class AggOpForeach[A, B](self: Rep[AggOp[A, B]], f: Rep[((AGGRecord[B]) => Unit)])(implicit val typeA: TypeRep[A], val typeB: TypeRep[B]) extends FunctionDef[Unit](Some(self), "foreach", List(List(f))) {
    override def curriedConstructor = (copy[A, B] _).curried
  }

  case class AggOpFindFirst[A, B](self: Rep[AggOp[A, B]], cond: Rep[((AGGRecord[B]) => Boolean)])(implicit val typeA: TypeRep[A], val typeB: TypeRep[B]) extends FunctionDef[AGGRecord[B]](Some(self), "findFirst", List(List(cond))) {
    override def curriedConstructor = (copy[A, B] _).curried
  }

  case class AggOpNullDynamicRecord[A, B, D](self: Rep[AggOp[A, B]])(implicit val typeA: TypeRep[A], val typeB: TypeRep[B], val typeD: TypeRep[D], val di: DummyImplicit) extends FunctionDef[D](Some(self), "NullDynamicRecord", List()) {
    override def curriedConstructor = (copy[A, B, D] _)
  }

  case class AggOpOpen[A, B](self: Rep[AggOp[A, B]])(implicit val typeA: TypeRep[A], val typeB: TypeRep[B]) extends FunctionDef[Unit](Some(self), "open", List(List())) {
    override def curriedConstructor = (copy[A, B] _)
  }

  case class AggOpNext[A, B](self: Rep[AggOp[A, B]])(implicit val typeA: TypeRep[A], val typeB: TypeRep[B]) extends FunctionDef[AGGRecord[B]](Some(self), "next", List(List())) {
    override def curriedConstructor = (copy[A, B] _)
  }

  case class AggOpClose[A, B](self: Rep[AggOp[A, B]])(implicit val typeA: TypeRep[A], val typeB: TypeRep[B]) extends FunctionDef[Unit](Some(self), "close", List(List())) {
    override def curriedConstructor = (copy[A, B] _)
  }

  case class AggOpReset[A, B](self: Rep[AggOp[A, B]])(implicit val typeA: TypeRep[A], val typeB: TypeRep[B]) extends FunctionDef[Unit](Some(self), "reset", List(List())) {
    override def curriedConstructor = (copy[A, B] _)
  }

  case class AggOp_Field_KeySet_$eq[A, B](self: Rep[AggOp[A, B]], x$1: Rep[Set[B]])(implicit val typeA: TypeRep[A], val typeB: TypeRep[B]) extends FieldSetter[Set[B]](self, "keySet", x$1) {
    override def curriedConstructor = (copy[A, B] _).curried
  }

  case class AggOp_Field_KeySet[A, B](self: Rep[AggOp[A, B]])(implicit val typeA: TypeRep[A], val typeB: TypeRep[B]) extends FieldGetter[Set[B]](self, "keySet") {
    override def curriedConstructor = (copy[A, B] _)
  }

  case class AggOp_Field_Hm[A, B](self: Rep[AggOp[A, B]])(implicit val typeA: TypeRep[A], val typeB: TypeRep[B]) extends FieldDef[HashMap[B, Array[Double]]](self, "hm") {
    override def curriedConstructor = (copy[A, B] _)
    override def isPure = true

  }

  case class AggOp_Field_AggFuncs[A, B](self: Rep[AggOp[A, B]])(implicit val typeA: TypeRep[A], val typeB: TypeRep[B]) extends FieldDef[Seq[((A, Double) => Double)]](self, "aggFuncs") {
    override def curriedConstructor = (copy[A, B] _)
    override def isPure = true

  }

  case class AggOp_Field_Grp[A, B](self: Rep[AggOp[A, B]])(implicit val typeA: TypeRep[A], val typeB: TypeRep[B]) extends FieldDef[(A => B)](self, "grp") {
    override def curriedConstructor = (copy[A, B] _)
    override def isPure = true

  }

  case class AggOp_Field_NumAggs[A, B](self: Rep[AggOp[A, B]])(implicit val typeA: TypeRep[A], val typeB: TypeRep[B]) extends FieldDef[Int](self, "numAggs") {
    override def curriedConstructor = (copy[A, B] _)
    override def isPure = true

  }

  case class AggOp_Field_Parent[A, B](self: Rep[AggOp[A, B]])(implicit val typeA: TypeRep[A], val typeB: TypeRep[B]) extends FieldDef[Operator[A]](self, "parent") {
    override def curriedConstructor = (copy[A, B] _)
    override def isPure = true

  }

  case class AggOp_Field_NullDynamicRecord[A, B](self: Rep[AggOp[A, B]])(implicit val typeA: TypeRep[A], val typeB: TypeRep[B]) extends FieldDef[AGGRecord[B]](self, "NullDynamicRecord") {
    override def curriedConstructor = (copy[A, B] _)
    override def isPure = true

  }

  // method definitions
  def aggOpNew[A, B](parent: Rep[Operator[A]], numAggs: Rep[Int], grp: Rep[((A) => B)], aggFuncs: Rep[((A, Double) => Double)]*)(implicit typeA: TypeRep[A], typeB: TypeRep[B]): Rep[AggOp[A, B]] = {
    val aggFuncsOutput = __liftSeq(aggFuncs.toSeq)
    AggOpNew[A, B](parent, numAggs, grp, aggFuncsOutput)
  }
  def aggOpForeach[A, B](self: Rep[AggOp[A, B]], f: Rep[((AGGRecord[B]) => Unit)])(implicit typeA: TypeRep[A], typeB: TypeRep[B]): Rep[Unit] = AggOpForeach[A, B](self, f)
  def aggOpFindFirst[A, B](self: Rep[AggOp[A, B]], cond: Rep[((AGGRecord[B]) => Boolean)])(implicit typeA: TypeRep[A], typeB: TypeRep[B]): Rep[AGGRecord[B]] = AggOpFindFirst[A, B](self, cond)
  def aggOpNullDynamicRecord[A, B, D](self: Rep[AggOp[A, B]])(implicit typeA: TypeRep[A], typeB: TypeRep[B], typeD: TypeRep[D], di: DummyImplicit): Rep[D] = AggOpNullDynamicRecord[A, B, D](self)
  def aggOpOpen[A, B](self: Rep[AggOp[A, B]])(implicit typeA: TypeRep[A], typeB: TypeRep[B]): Rep[Unit] = AggOpOpen[A, B](self)
  def aggOpNext[A, B](self: Rep[AggOp[A, B]])(implicit typeA: TypeRep[A], typeB: TypeRep[B]): Rep[AGGRecord[B]] = AggOpNext[A, B](self)
  def aggOpClose[A, B](self: Rep[AggOp[A, B]])(implicit typeA: TypeRep[A], typeB: TypeRep[B]): Rep[Unit] = AggOpClose[A, B](self)
  def aggOpReset[A, B](self: Rep[AggOp[A, B]])(implicit typeA: TypeRep[A], typeB: TypeRep[B]): Rep[Unit] = AggOpReset[A, B](self)
  def aggOp_Field_KeySet_$eq[A, B](self: Rep[AggOp[A, B]], x$1: Rep[Set[B]])(implicit typeA: TypeRep[A], typeB: TypeRep[B]): Rep[Unit] = AggOp_Field_KeySet_$eq[A, B](self, x$1)
  def aggOp_Field_KeySet[A, B](self: Rep[AggOp[A, B]])(implicit typeA: TypeRep[A], typeB: TypeRep[B]): Rep[Set[B]] = AggOp_Field_KeySet[A, B](self)
  def aggOp_Field_Hm[A, B](self: Rep[AggOp[A, B]])(implicit typeA: TypeRep[A], typeB: TypeRep[B]): Rep[HashMap[B, Array[Double]]] = AggOp_Field_Hm[A, B](self)
  def aggOp_Field_AggFuncs[A, B](self: Rep[AggOp[A, B]])(implicit typeA: TypeRep[A], typeB: TypeRep[B]): Rep[Seq[((A, Double) => Double)]] = AggOp_Field_AggFuncs[A, B](self)
  def aggOp_Field_Grp[A, B](self: Rep[AggOp[A, B]])(implicit typeA: TypeRep[A], typeB: TypeRep[B]): Rep[(A => B)] = AggOp_Field_Grp[A, B](self)
  def aggOp_Field_NumAggs[A, B](self: Rep[AggOp[A, B]])(implicit typeA: TypeRep[A], typeB: TypeRep[B]): Rep[Int] = AggOp_Field_NumAggs[A, B](self)
  def aggOp_Field_Parent[A, B](self: Rep[AggOp[A, B]])(implicit typeA: TypeRep[A], typeB: TypeRep[B]): Rep[Operator[A]] = AggOp_Field_Parent[A, B](self)
  def aggOp_Field_NullDynamicRecord[A, B](self: Rep[AggOp[A, B]])(implicit typeA: TypeRep[A], typeB: TypeRep[B]): Rep[AGGRecord[B]] = AggOp_Field_NullDynamicRecord[A, B](self)
  type AggOp[A, B] = ch.epfl.data.legobase.queryengine.volcano.AggOp[A, B]
  case class AggOpType[A, B](typeA: TypeRep[A], typeB: TypeRep[B]) extends TypeRep[AggOp[A, B]] {
    def rebuild(newArguments: TypeRep[_]*): TypeRep[_] = AggOpType(newArguments(0).asInstanceOf[TypeRep[_]], newArguments(1).asInstanceOf[TypeRep[_]])
    private implicit val tagA = typeA.typeTag
    private implicit val tagB = typeB.typeTag
    val name = s"AggOp[${typeA.name}, ${typeB.name}]"
    val typeArguments = List(typeA, typeB)

    val typeTag = scala.reflect.runtime.universe.typeTag[AggOp[A, B]]
  }
  implicit def typeAggOp[A: TypeRep, B: TypeRep] = AggOpType(implicitly[TypeRep[A]], implicitly[TypeRep[B]])
}
trait AggOpImplicits { this: AggOpComponent =>
  // Add implicit conversions here!
}
trait AggOpImplementations { self: DeepDSL =>
  override def aggOpOpen[A, B](self: Rep[AggOp[A, B]])(implicit typeA: TypeRep[A], typeB: TypeRep[B]): Rep[Unit] = {
    {
      self.parent.open();
      self.parent.foreach(__lambda(((t: this.Rep[A]) => {
        val key: this.Rep[B] = __app(self.grp).apply(t);
        val aggs: this.Rep[Array[Double]] = self.hm.getOrElseUpdate(key, __newArray[Double](self.numAggs));
        var i: this.Var[Int] = __newVar(unit(0));
        self.aggFuncs.foreach[Unit](__lambda(((aggFun: this.Rep[(A, Double) => Double]) => {
          aggs.update(readVar(i), __app(aggFun).apply(t, aggs.apply(i)));
          __assign(i, readVar(i).$plus(unit(1)))
        })))
      })));
      self.keySet_$eq(Set.apply[B](self.hm.keySet.toSeq))
    }
  }
  override def aggOpNext[A, B](self: Rep[AggOp[A, B]])(implicit typeA: TypeRep[A], typeB: TypeRep[B]): Rep[AGGRecord[B]] = {
    __ifThenElse(infix_$bang$eq(self.hm.size, unit(0)), {
      val key: this.Rep[B] = self.keySet.head;
      self.keySet.remove(key);
      val elem: this.Rep[Option[Array[Double]]] = self.hm.remove(key);
      GenericEngine.newAGGRecord[B](key, elem.get)
    }, self.NullDynamicRecord)
  }
  override def aggOpClose[A, B](self: Rep[AggOp[A, B]])(implicit typeA: TypeRep[A], typeB: TypeRep[B]): Rep[Unit] = {
    unit(())
  }
  override def aggOpReset[A, B](self: Rep[AggOp[A, B]])(implicit typeA: TypeRep[A], typeB: TypeRep[B]): Rep[Unit] = {
    {
      self.parent.reset();
      self.hm.clear();
      self.open()
    }
  }
}
trait AggOpComponent extends AggOpOps with AggOpImplicits { self: OperatorsComponent => }
trait SortOpOps extends Base { this: OperatorsComponent =>
  implicit class SortOpRep[A](self: Rep[SortOp[A]])(implicit typeA: TypeRep[A]) {
    def foreach(f: Rep[(A => Unit)]): Rep[Unit] = sortOpForeach[A](self, f)(typeA)
    def findFirst(cond: Rep[(A => Boolean)]): Rep[A] = sortOpFindFirst[A](self, cond)(typeA)
    def NullDynamicRecord[D](implicit typeD: TypeRep[D], di: DummyImplicit): Rep[D] = sortOpNullDynamicRecord[A, D](self)(typeA, typeD, di)
    def open(): Rep[Unit] = sortOpOpen[A](self)(typeA)
    def next(): Rep[A] = sortOpNext[A](self)(typeA)
    def close(): Rep[Unit] = sortOpClose[A](self)(typeA)
    def reset(): Rep[Unit] = sortOpReset[A](self)(typeA)
    def sortedTree: Rep[TreeSet[A]] = sortOp_Field_SortedTree[A](self)(typeA)
    def orderingFunc: Rep[((A, A) => Int)] = sortOp_Field_OrderingFunc[A](self)(typeA)
    def parent: Rep[Operator[A]] = sortOp_Field_Parent[A](self)(typeA)
    def NullDynamicRecord: Rep[A] = sortOp_Field_NullDynamicRecord[A](self)(typeA)
  }
  object SortOp {

  }
  // constructors
  def __newSortOp[A](parent: Rep[Operator[A]])(orderingFunc: Rep[((A, A) => Int)])(implicit typeA: TypeRep[A]): Rep[SortOp[A]] = sortOpNew[A](parent, orderingFunc)(typeA)
  // case classes
  case class SortOpNew[A](parent: Rep[Operator[A]], orderingFunc: Rep[((A, A) => Int)])(implicit val typeA: TypeRep[A]) extends ConstructorDef[SortOp[A]](List(typeA), "SortOp", List(List(parent), List(orderingFunc))) {
    override def curriedConstructor = (copy[A] _).curried
  }

  case class SortOpForeach[A](self: Rep[SortOp[A]], f: Rep[((A) => Unit)])(implicit val typeA: TypeRep[A]) extends FunctionDef[Unit](Some(self), "foreach", List(List(f))) {
    override def curriedConstructor = (copy[A] _).curried
  }

  case class SortOpFindFirst[A](self: Rep[SortOp[A]], cond: Rep[((A) => Boolean)])(implicit val typeA: TypeRep[A]) extends FunctionDef[A](Some(self), "findFirst", List(List(cond))) {
    override def curriedConstructor = (copy[A] _).curried
  }

  case class SortOpNullDynamicRecord[A, D](self: Rep[SortOp[A]])(implicit val typeA: TypeRep[A], val typeD: TypeRep[D], val di: DummyImplicit) extends FunctionDef[D](Some(self), "NullDynamicRecord", List()) {
    override def curriedConstructor = (copy[A, D] _)
  }

  case class SortOpOpen[A](self: Rep[SortOp[A]])(implicit val typeA: TypeRep[A]) extends FunctionDef[Unit](Some(self), "open", List(List())) {
    override def curriedConstructor = (copy[A] _)
  }

  case class SortOpNext[A](self: Rep[SortOp[A]])(implicit val typeA: TypeRep[A]) extends FunctionDef[A](Some(self), "next", List(List())) {
    override def curriedConstructor = (copy[A] _)
  }

  case class SortOpClose[A](self: Rep[SortOp[A]])(implicit val typeA: TypeRep[A]) extends FunctionDef[Unit](Some(self), "close", List(List())) {
    override def curriedConstructor = (copy[A] _)
  }

  case class SortOpReset[A](self: Rep[SortOp[A]])(implicit val typeA: TypeRep[A]) extends FunctionDef[Unit](Some(self), "reset", List(List())) {
    override def curriedConstructor = (copy[A] _)
  }

  case class SortOp_Field_SortedTree[A](self: Rep[SortOp[A]])(implicit val typeA: TypeRep[A]) extends FieldDef[TreeSet[A]](self, "sortedTree") {
    override def curriedConstructor = (copy[A] _)
    override def isPure = true

  }

  case class SortOp_Field_OrderingFunc[A](self: Rep[SortOp[A]])(implicit val typeA: TypeRep[A]) extends FieldDef[((A, A) => Int)](self, "orderingFunc") {
    override def curriedConstructor = (copy[A] _)
    override def isPure = true

  }

  case class SortOp_Field_Parent[A](self: Rep[SortOp[A]])(implicit val typeA: TypeRep[A]) extends FieldDef[Operator[A]](self, "parent") {
    override def curriedConstructor = (copy[A] _)
    override def isPure = true

  }

  case class SortOp_Field_NullDynamicRecord[A](self: Rep[SortOp[A]])(implicit val typeA: TypeRep[A]) extends FieldDef[A](self, "NullDynamicRecord") {
    override def curriedConstructor = (copy[A] _)
    override def isPure = true

  }

  // method definitions
  def sortOpNew[A](parent: Rep[Operator[A]], orderingFunc: Rep[((A, A) => Int)])(implicit typeA: TypeRep[A]): Rep[SortOp[A]] = SortOpNew[A](parent, orderingFunc)
  def sortOpForeach[A](self: Rep[SortOp[A]], f: Rep[((A) => Unit)])(implicit typeA: TypeRep[A]): Rep[Unit] = SortOpForeach[A](self, f)
  def sortOpFindFirst[A](self: Rep[SortOp[A]], cond: Rep[((A) => Boolean)])(implicit typeA: TypeRep[A]): Rep[A] = SortOpFindFirst[A](self, cond)
  def sortOpNullDynamicRecord[A, D](self: Rep[SortOp[A]])(implicit typeA: TypeRep[A], typeD: TypeRep[D], di: DummyImplicit): Rep[D] = SortOpNullDynamicRecord[A, D](self)
  def sortOpOpen[A](self: Rep[SortOp[A]])(implicit typeA: TypeRep[A]): Rep[Unit] = SortOpOpen[A](self)
  def sortOpNext[A](self: Rep[SortOp[A]])(implicit typeA: TypeRep[A]): Rep[A] = SortOpNext[A](self)
  def sortOpClose[A](self: Rep[SortOp[A]])(implicit typeA: TypeRep[A]): Rep[Unit] = SortOpClose[A](self)
  def sortOpReset[A](self: Rep[SortOp[A]])(implicit typeA: TypeRep[A]): Rep[Unit] = SortOpReset[A](self)
  def sortOp_Field_SortedTree[A](self: Rep[SortOp[A]])(implicit typeA: TypeRep[A]): Rep[TreeSet[A]] = SortOp_Field_SortedTree[A](self)
  def sortOp_Field_OrderingFunc[A](self: Rep[SortOp[A]])(implicit typeA: TypeRep[A]): Rep[((A, A) => Int)] = SortOp_Field_OrderingFunc[A](self)
  def sortOp_Field_Parent[A](self: Rep[SortOp[A]])(implicit typeA: TypeRep[A]): Rep[Operator[A]] = SortOp_Field_Parent[A](self)
  def sortOp_Field_NullDynamicRecord[A](self: Rep[SortOp[A]])(implicit typeA: TypeRep[A]): Rep[A] = SortOp_Field_NullDynamicRecord[A](self)
  type SortOp[A] = ch.epfl.data.legobase.queryengine.volcano.SortOp[A]
  case class SortOpType[A](typeA: TypeRep[A]) extends TypeRep[SortOp[A]] {
    def rebuild(newArguments: TypeRep[_]*): TypeRep[_] = SortOpType(newArguments(0).asInstanceOf[TypeRep[_]])
    private implicit val tagA = typeA.typeTag
    val name = s"SortOp[${typeA.name}]"
    val typeArguments = List(typeA)

    val typeTag = scala.reflect.runtime.universe.typeTag[SortOp[A]]
  }
  implicit def typeSortOp[A: TypeRep] = SortOpType(implicitly[TypeRep[A]])
}
trait SortOpImplicits { this: SortOpComponent =>
  // Add implicit conversions here!
}
trait SortOpImplementations { self: DeepDSL =>
  override def sortOpOpen[A](self: Rep[SortOp[A]])(implicit typeA: TypeRep[A]): Rep[Unit] = {
    {
      self.parent.open();
      self.parent.foreach(__lambda(((t: this.Rep[A]) => {
        self.sortedTree.$plus$eq(t);
        unit(())
      })))
    }
  }
  override def sortOpNext[A](self: Rep[SortOp[A]])(implicit typeA: TypeRep[A]): Rep[A] = {
    __ifThenElse(infix_$bang$eq(self.sortedTree.size, unit(0)), {
      val elem: this.Rep[A] = self.sortedTree.head;
      self.sortedTree.$minus$eq(elem);
      elem
    }, self.NullDynamicRecord)
  }
  override def sortOpClose[A](self: Rep[SortOp[A]])(implicit typeA: TypeRep[A]): Rep[Unit] = {
    unit(())
  }
  override def sortOpReset[A](self: Rep[SortOp[A]])(implicit typeA: TypeRep[A]): Rep[Unit] = {
    {
      self.parent.reset();
      self.open()
    }
  }
}
trait SortOpComponent extends SortOpOps with SortOpImplicits { self: OperatorsComponent => }
trait MapOpOps extends Base { this: OperatorsComponent =>
  implicit class MapOpRep[A](self: Rep[MapOp[A]])(implicit typeA: TypeRep[A]) {
    def foreach(f: Rep[(A => Unit)]): Rep[Unit] = mapOpForeach[A](self, f)(typeA)
    def findFirst(cond: Rep[(A => Boolean)]): Rep[A] = mapOpFindFirst[A](self, cond)(typeA)
    def NullDynamicRecord[D](implicit typeD: TypeRep[D], di: DummyImplicit): Rep[D] = mapOpNullDynamicRecord[A, D](self)(typeA, typeD, di)
    def open(): Rep[Unit] = mapOpOpen[A](self)(typeA)
    def next(): Rep[A] = mapOpNext[A](self)(typeA)
    def close(): Rep[Unit] = mapOpClose[A](self)(typeA)
    def reset(): Rep[Unit] = mapOpReset[A](self)(typeA)
    def aggFuncs: Rep[Seq[(A => Unit)]] = mapOp_Field_AggFuncs[A](self)(typeA)
    def parent: Rep[Operator[A]] = mapOp_Field_Parent[A](self)(typeA)
    def NullDynamicRecord: Rep[A] = mapOp_Field_NullDynamicRecord[A](self)(typeA)
  }
  object MapOp {

  }
  // constructors
  def __newMapOp[A](parent: Rep[Operator[A]])(aggFuncs: Rep[(A => Unit)]*)(implicit typeA: TypeRep[A]): Rep[MapOp[A]] = mapOpNew[A](parent, aggFuncs: _*)(typeA)
  // case classes
  case class MapOpNew[A](parent: Rep[Operator[A]], aggFuncsOutput: Rep[Seq[((A) => Unit)]])(implicit val typeA: TypeRep[A]) extends ConstructorDef[MapOp[A]](List(typeA), "MapOp", List(List(parent), List(__varArg(aggFuncsOutput)))) {
    override def curriedConstructor = (copy[A] _).curried
  }

  case class MapOpForeach[A](self: Rep[MapOp[A]], f: Rep[((A) => Unit)])(implicit val typeA: TypeRep[A]) extends FunctionDef[Unit](Some(self), "foreach", List(List(f))) {
    override def curriedConstructor = (copy[A] _).curried
  }

  case class MapOpFindFirst[A](self: Rep[MapOp[A]], cond: Rep[((A) => Boolean)])(implicit val typeA: TypeRep[A]) extends FunctionDef[A](Some(self), "findFirst", List(List(cond))) {
    override def curriedConstructor = (copy[A] _).curried
  }

  case class MapOpNullDynamicRecord[A, D](self: Rep[MapOp[A]])(implicit val typeA: TypeRep[A], val typeD: TypeRep[D], val di: DummyImplicit) extends FunctionDef[D](Some(self), "NullDynamicRecord", List()) {
    override def curriedConstructor = (copy[A, D] _)
  }

  case class MapOpOpen[A](self: Rep[MapOp[A]])(implicit val typeA: TypeRep[A]) extends FunctionDef[Unit](Some(self), "open", List(List())) {
    override def curriedConstructor = (copy[A] _)
  }

  case class MapOpNext[A](self: Rep[MapOp[A]])(implicit val typeA: TypeRep[A]) extends FunctionDef[A](Some(self), "next", List(List())) {
    override def curriedConstructor = (copy[A] _)
  }

  case class MapOpClose[A](self: Rep[MapOp[A]])(implicit val typeA: TypeRep[A]) extends FunctionDef[Unit](Some(self), "close", List(List())) {
    override def curriedConstructor = (copy[A] _)
  }

  case class MapOpReset[A](self: Rep[MapOp[A]])(implicit val typeA: TypeRep[A]) extends FunctionDef[Unit](Some(self), "reset", List(List())) {
    override def curriedConstructor = (copy[A] _)
  }

  case class MapOp_Field_AggFuncs[A](self: Rep[MapOp[A]])(implicit val typeA: TypeRep[A]) extends FieldDef[Seq[(A => Unit)]](self, "aggFuncs") {
    override def curriedConstructor = (copy[A] _)
    override def isPure = true

  }

  case class MapOp_Field_Parent[A](self: Rep[MapOp[A]])(implicit val typeA: TypeRep[A]) extends FieldDef[Operator[A]](self, "parent") {
    override def curriedConstructor = (copy[A] _)
    override def isPure = true

  }

  case class MapOp_Field_NullDynamicRecord[A](self: Rep[MapOp[A]])(implicit val typeA: TypeRep[A]) extends FieldDef[A](self, "NullDynamicRecord") {
    override def curriedConstructor = (copy[A] _)
    override def isPure = true

  }

  // method definitions
  def mapOpNew[A](parent: Rep[Operator[A]], aggFuncs: Rep[((A) => Unit)]*)(implicit typeA: TypeRep[A]): Rep[MapOp[A]] = {
    val aggFuncsOutput = __liftSeq(aggFuncs.toSeq)
    MapOpNew[A](parent, aggFuncsOutput)
  }
  def mapOpForeach[A](self: Rep[MapOp[A]], f: Rep[((A) => Unit)])(implicit typeA: TypeRep[A]): Rep[Unit] = MapOpForeach[A](self, f)
  def mapOpFindFirst[A](self: Rep[MapOp[A]], cond: Rep[((A) => Boolean)])(implicit typeA: TypeRep[A]): Rep[A] = MapOpFindFirst[A](self, cond)
  def mapOpNullDynamicRecord[A, D](self: Rep[MapOp[A]])(implicit typeA: TypeRep[A], typeD: TypeRep[D], di: DummyImplicit): Rep[D] = MapOpNullDynamicRecord[A, D](self)
  def mapOpOpen[A](self: Rep[MapOp[A]])(implicit typeA: TypeRep[A]): Rep[Unit] = MapOpOpen[A](self)
  def mapOpNext[A](self: Rep[MapOp[A]])(implicit typeA: TypeRep[A]): Rep[A] = MapOpNext[A](self)
  def mapOpClose[A](self: Rep[MapOp[A]])(implicit typeA: TypeRep[A]): Rep[Unit] = MapOpClose[A](self)
  def mapOpReset[A](self: Rep[MapOp[A]])(implicit typeA: TypeRep[A]): Rep[Unit] = MapOpReset[A](self)
  def mapOp_Field_AggFuncs[A](self: Rep[MapOp[A]])(implicit typeA: TypeRep[A]): Rep[Seq[(A => Unit)]] = MapOp_Field_AggFuncs[A](self)
  def mapOp_Field_Parent[A](self: Rep[MapOp[A]])(implicit typeA: TypeRep[A]): Rep[Operator[A]] = MapOp_Field_Parent[A](self)
  def mapOp_Field_NullDynamicRecord[A](self: Rep[MapOp[A]])(implicit typeA: TypeRep[A]): Rep[A] = MapOp_Field_NullDynamicRecord[A](self)
  type MapOp[A] = ch.epfl.data.legobase.queryengine.volcano.MapOp[A]
  case class MapOpType[A](typeA: TypeRep[A]) extends TypeRep[MapOp[A]] {
    def rebuild(newArguments: TypeRep[_]*): TypeRep[_] = MapOpType(newArguments(0).asInstanceOf[TypeRep[_]])
    private implicit val tagA = typeA.typeTag
    val name = s"MapOp[${typeA.name}]"
    val typeArguments = List(typeA)

    val typeTag = scala.reflect.runtime.universe.typeTag[MapOp[A]]
  }
  implicit def typeMapOp[A: TypeRep] = MapOpType(implicitly[TypeRep[A]])
}
trait MapOpImplicits { this: MapOpComponent =>
  // Add implicit conversions here!
}
trait MapOpImplementations { self: DeepDSL =>
  override def mapOpOpen[A](self: Rep[MapOp[A]])(implicit typeA: TypeRep[A]): Rep[Unit] = {
    self.parent.open()
  }
  override def mapOpNext[A](self: Rep[MapOp[A]])(implicit typeA: TypeRep[A]): Rep[A] = {
    {
      val t: this.Rep[A] = self.parent.next();
      __ifThenElse(infix_$bang$eq(t, self.NullDynamicRecord), {
        self.aggFuncs.foreach[Unit](__lambda(((agg: this.Rep[A => Unit]) => __app(agg).apply(t))));
        t
      }, self.NullDynamicRecord)
    }
  }
  override def mapOpClose[A](self: Rep[MapOp[A]])(implicit typeA: TypeRep[A]): Rep[Unit] = {
    unit(())
  }
  override def mapOpReset[A](self: Rep[MapOp[A]])(implicit typeA: TypeRep[A]): Rep[Unit] = {
    self.parent.reset()
  }
}
trait MapOpComponent extends MapOpOps with MapOpImplicits { self: OperatorsComponent => }
trait PrintOpOps extends Base { this: OperatorsComponent =>
  implicit class PrintOpRep[A](self: Rep[PrintOp[A]])(implicit typeA: TypeRep[A]) {
    def foreach(f: Rep[(A => Unit)]): Rep[Unit] = printOpForeach[A](self, f)(typeA)
    def findFirst(cond: Rep[(A => Boolean)]): Rep[A] = printOpFindFirst[A](self, cond)(typeA)
    def NullDynamicRecord[D](implicit typeD: TypeRep[D], di: DummyImplicit): Rep[D] = printOpNullDynamicRecord[A, D](self)(typeA, typeD, di)
    def open(): Rep[Unit] = printOpOpen[A](self)(typeA)
    def next(): Rep[A] = printOpNext[A](self)(typeA)
    def close(): Rep[Unit] = printOpClose[A](self)(typeA)
    def reset(): Rep[Unit] = printOpReset[A](self)(typeA)
    def numRows_=(x$1: Rep[Int]): Rep[Unit] = printOp_Field_NumRows_$eq[A](self, x$1)(typeA)
    def numRows: Rep[Int] = printOp_Field_NumRows[A](self)(typeA)
    def limit: Rep[(() => Boolean)] = printOp_Field_Limit[A](self)(typeA)
    def printFunc: Rep[(A => Unit)] = printOp_Field_PrintFunc[A](self)(typeA)
    def parent_=(x$1: Rep[Operator[A]]): Rep[Unit] = printOp_Field_Parent_$eq[A](self, x$1)(typeA)
    def parent: Rep[Operator[A]] = printOp_Field_Parent[A](self)(typeA)
    def NullDynamicRecord: Rep[A] = printOp_Field_NullDynamicRecord[A](self)(typeA)
  }
  object PrintOp {

  }
  // constructors
  def __newPrintOp[A](parent: Rep[Operator[A]])(printFunc: Rep[(A => Unit)], limit: Rep[(() => Boolean)])(implicit typeA: TypeRep[A]): Rep[PrintOp[A]] = printOpNew[A](parent, printFunc, limit)(typeA)
  // case classes
  case class PrintOpNew[A](parent: Rep[Operator[A]], printFunc: Rep[((A) => Unit)], limit: Rep[(() => Boolean)])(implicit val typeA: TypeRep[A]) extends ConstructorDef[PrintOp[A]](List(typeA), "PrintOp", List(List(parent), List(printFunc, limit))) {
    override def curriedConstructor = (copy[A] _).curried
  }

  case class PrintOpForeach[A](self: Rep[PrintOp[A]], f: Rep[((A) => Unit)])(implicit val typeA: TypeRep[A]) extends FunctionDef[Unit](Some(self), "foreach", List(List(f))) {
    override def curriedConstructor = (copy[A] _).curried
  }

  case class PrintOpFindFirst[A](self: Rep[PrintOp[A]], cond: Rep[((A) => Boolean)])(implicit val typeA: TypeRep[A]) extends FunctionDef[A](Some(self), "findFirst", List(List(cond))) {
    override def curriedConstructor = (copy[A] _).curried
  }

  case class PrintOpNullDynamicRecord[A, D](self: Rep[PrintOp[A]])(implicit val typeA: TypeRep[A], val typeD: TypeRep[D], val di: DummyImplicit) extends FunctionDef[D](Some(self), "NullDynamicRecord", List()) {
    override def curriedConstructor = (copy[A, D] _)
  }

  case class PrintOpOpen[A](self: Rep[PrintOp[A]])(implicit val typeA: TypeRep[A]) extends FunctionDef[Unit](Some(self), "open", List(List())) {
    override def curriedConstructor = (copy[A] _)
  }

  case class PrintOpNext[A](self: Rep[PrintOp[A]])(implicit val typeA: TypeRep[A]) extends FunctionDef[A](Some(self), "next", List(List())) {
    override def curriedConstructor = (copy[A] _)
  }

  case class PrintOpClose[A](self: Rep[PrintOp[A]])(implicit val typeA: TypeRep[A]) extends FunctionDef[Unit](Some(self), "close", List(List())) {
    override def curriedConstructor = (copy[A] _)
  }

  case class PrintOpReset[A](self: Rep[PrintOp[A]])(implicit val typeA: TypeRep[A]) extends FunctionDef[Unit](Some(self), "reset", List(List())) {
    override def curriedConstructor = (copy[A] _)
  }

  case class PrintOp_Field_NumRows_$eq[A](self: Rep[PrintOp[A]], x$1: Rep[Int])(implicit val typeA: TypeRep[A]) extends FieldSetter[Int](self, "numRows", x$1) {
    override def curriedConstructor = (copy[A] _).curried
  }

  case class PrintOp_Field_NumRows[A](self: Rep[PrintOp[A]])(implicit val typeA: TypeRep[A]) extends FieldGetter[Int](self, "numRows") {
    override def curriedConstructor = (copy[A] _)
  }

  case class PrintOp_Field_Limit[A](self: Rep[PrintOp[A]])(implicit val typeA: TypeRep[A]) extends FieldDef[(() => Boolean)](self, "limit") {
    override def curriedConstructor = (copy[A] _)
    override def isPure = true

  }

  case class PrintOp_Field_PrintFunc[A](self: Rep[PrintOp[A]])(implicit val typeA: TypeRep[A]) extends FieldDef[(A => Unit)](self, "printFunc") {
    override def curriedConstructor = (copy[A] _)
    override def isPure = true

  }

  case class PrintOp_Field_Parent_$eq[A](self: Rep[PrintOp[A]], x$1: Rep[Operator[A]])(implicit val typeA: TypeRep[A]) extends FieldSetter[Operator[A]](self, "parent", x$1) {
    override def curriedConstructor = (copy[A] _).curried
  }

  case class PrintOp_Field_Parent[A](self: Rep[PrintOp[A]])(implicit val typeA: TypeRep[A]) extends FieldGetter[Operator[A]](self, "parent") {
    override def curriedConstructor = (copy[A] _)
  }

  case class PrintOp_Field_NullDynamicRecord[A](self: Rep[PrintOp[A]])(implicit val typeA: TypeRep[A]) extends FieldDef[A](self, "NullDynamicRecord") {
    override def curriedConstructor = (copy[A] _)
    override def isPure = true

  }

  // method definitions
  def printOpNew[A](parent: Rep[Operator[A]], printFunc: Rep[((A) => Unit)], limit: Rep[(() => Boolean)])(implicit typeA: TypeRep[A]): Rep[PrintOp[A]] = PrintOpNew[A](parent, printFunc, limit)
  def printOpForeach[A](self: Rep[PrintOp[A]], f: Rep[((A) => Unit)])(implicit typeA: TypeRep[A]): Rep[Unit] = PrintOpForeach[A](self, f)
  def printOpFindFirst[A](self: Rep[PrintOp[A]], cond: Rep[((A) => Boolean)])(implicit typeA: TypeRep[A]): Rep[A] = PrintOpFindFirst[A](self, cond)
  def printOpNullDynamicRecord[A, D](self: Rep[PrintOp[A]])(implicit typeA: TypeRep[A], typeD: TypeRep[D], di: DummyImplicit): Rep[D] = PrintOpNullDynamicRecord[A, D](self)
  def printOpOpen[A](self: Rep[PrintOp[A]])(implicit typeA: TypeRep[A]): Rep[Unit] = PrintOpOpen[A](self)
  def printOpNext[A](self: Rep[PrintOp[A]])(implicit typeA: TypeRep[A]): Rep[A] = PrintOpNext[A](self)
  def printOpClose[A](self: Rep[PrintOp[A]])(implicit typeA: TypeRep[A]): Rep[Unit] = PrintOpClose[A](self)
  def printOpReset[A](self: Rep[PrintOp[A]])(implicit typeA: TypeRep[A]): Rep[Unit] = PrintOpReset[A](self)
  def printOp_Field_NumRows_$eq[A](self: Rep[PrintOp[A]], x$1: Rep[Int])(implicit typeA: TypeRep[A]): Rep[Unit] = PrintOp_Field_NumRows_$eq[A](self, x$1)
  def printOp_Field_NumRows[A](self: Rep[PrintOp[A]])(implicit typeA: TypeRep[A]): Rep[Int] = PrintOp_Field_NumRows[A](self)
  def printOp_Field_Limit[A](self: Rep[PrintOp[A]])(implicit typeA: TypeRep[A]): Rep[(() => Boolean)] = PrintOp_Field_Limit[A](self)
  def printOp_Field_PrintFunc[A](self: Rep[PrintOp[A]])(implicit typeA: TypeRep[A]): Rep[(A => Unit)] = PrintOp_Field_PrintFunc[A](self)
  def printOp_Field_Parent_$eq[A](self: Rep[PrintOp[A]], x$1: Rep[Operator[A]])(implicit typeA: TypeRep[A]): Rep[Unit] = PrintOp_Field_Parent_$eq[A](self, x$1)
  def printOp_Field_Parent[A](self: Rep[PrintOp[A]])(implicit typeA: TypeRep[A]): Rep[Operator[A]] = PrintOp_Field_Parent[A](self)
  def printOp_Field_NullDynamicRecord[A](self: Rep[PrintOp[A]])(implicit typeA: TypeRep[A]): Rep[A] = PrintOp_Field_NullDynamicRecord[A](self)
  type PrintOp[A] = ch.epfl.data.legobase.queryengine.volcano.PrintOp[A]
  case class PrintOpType[A](typeA: TypeRep[A]) extends TypeRep[PrintOp[A]] {
    def rebuild(newArguments: TypeRep[_]*): TypeRep[_] = PrintOpType(newArguments(0).asInstanceOf[TypeRep[_]])
    private implicit val tagA = typeA.typeTag
    val name = s"PrintOp[${typeA.name}]"
    val typeArguments = List(typeA)

    val typeTag = scala.reflect.runtime.universe.typeTag[PrintOp[A]]
  }
  implicit def typePrintOp[A: TypeRep] = PrintOpType(implicitly[TypeRep[A]])
}
trait PrintOpImplicits { this: PrintOpComponent =>
  // Add implicit conversions here!
}
trait PrintOpImplementations { self: DeepDSL =>
  override def printOpOpen[A](self: Rep[PrintOp[A]])(implicit typeA: TypeRep[A]): Rep[Unit] = {
    self.parent.open()
  }
  override def printOpNext[A](self: Rep[PrintOp[A]])(implicit typeA: TypeRep[A]): Rep[A] = {
    {
      var exit: this.Var[Boolean] = __newVar(unit(false));
      __whileDo(infix_$eq$eq(readVar(exit), unit(false)), {
        val t: this.Rep[A] = self.parent.next();
        __ifThenElse(infix_$eq$eq(__app(self.limit).apply(), unit(false)).$bar$bar(infix_$eq$eq(t, self.NullDynamicRecord)), __assign(exit, unit(true)), {
          __app(self.printFunc).apply(t);
          self.numRows_$eq(self.numRows.$plus(unit(1)))
        })
      });
      self.NullDynamicRecord
    }
  }
  override def printOpClose[A](self: Rep[PrintOp[A]])(implicit typeA: TypeRep[A]): Rep[Unit] = {
    unit(())
  }
  override def printOpReset[A](self: Rep[PrintOp[A]])(implicit typeA: TypeRep[A]): Rep[Unit] = {
    self.parent.reset()
  }
}
trait PrintOpComponent extends PrintOpOps with PrintOpImplicits { self: OperatorsComponent => }
trait HashJoinOpOps extends Base { this: OperatorsComponent =>
  implicit class HashJoinOpRep[A <: ch.epfl.data.pardis.shallow.AbstractRecord, B <: ch.epfl.data.pardis.shallow.AbstractRecord, C](self: Rep[HashJoinOp[A, B, C]])(implicit typeA: TypeRep[A], typeB: TypeRep[B], typeC: TypeRep[C]) {
    def foreach(f: Rep[(DynamicCompositeRecord[A, B] => Unit)]): Rep[Unit] = hashJoinOpForeach[A, B, C](self, f)(typeA, typeB, typeC)
    def findFirst(cond: Rep[(DynamicCompositeRecord[A, B] => Boolean)]): Rep[DynamicCompositeRecord[A, B]] = hashJoinOpFindFirst[A, B, C](self, cond)(typeA, typeB, typeC)
    def NullDynamicRecord[D](implicit typeD: TypeRep[D], di: DummyImplicit): Rep[D] = hashJoinOpNullDynamicRecord[A, B, C, D](self)(typeA, typeB, typeC, typeD, di)
    def open(): Rep[Unit] = hashJoinOpOpen[A, B, C](self)(typeA, typeB, typeC)
    def next(): Rep[DynamicCompositeRecord[A, B]] = hashJoinOpNext[A, B, C](self)(typeA, typeB, typeC)
    def close(): Rep[Unit] = hashJoinOpClose[A, B, C](self)(typeA, typeB, typeC)
    def reset(): Rep[Unit] = hashJoinOpReset[A, B, C](self)(typeA, typeB, typeC)
    def tmpLine_=(x$1: Rep[B]): Rep[Unit] = hashJoinOp_Field_TmpLine_$eq[A, B, C](self, x$1)(typeA, typeB, typeC)
    def tmpLine: Rep[B] = hashJoinOp_Field_TmpLine[A, B, C](self)(typeA, typeB, typeC)
    def hm: Rep[HashMap[C, ArrayBuffer[A]]] = hashJoinOp_Field_Hm[A, B, C](self)(typeA, typeB, typeC)
    def tmpBuffer_=(x$1: Rep[ArrayBuffer[A]]): Rep[Unit] = hashJoinOp_Field_TmpBuffer_$eq[A, B, C](self, x$1)(typeA, typeB, typeC)
    def tmpBuffer: Rep[ArrayBuffer[A]] = hashJoinOp_Field_TmpBuffer[A, B, C](self)(typeA, typeB, typeC)
    def tmpCount_=(x$1: Rep[Int]): Rep[Unit] = hashJoinOp_Field_TmpCount_$eq[A, B, C](self, x$1)(typeA, typeB, typeC)
    def tmpCount: Rep[Int] = hashJoinOp_Field_TmpCount[A, B, C](self)(typeA, typeB, typeC)
    def rightHash: Rep[(B => C)] = hashJoinOp_Field_RightHash[A, B, C](self)(typeA, typeB, typeC)
    def leftHash: Rep[(A => C)] = hashJoinOp_Field_LeftHash[A, B, C](self)(typeA, typeB, typeC)
    def joinCond: Rep[((A, B) => Boolean)] = hashJoinOp_Field_JoinCond[A, B, C](self)(typeA, typeB, typeC)
    def rightAlias: Rep[String] = hashJoinOp_Field_RightAlias[A, B, C](self)(typeA, typeB, typeC)
    def leftAlias: Rep[String] = hashJoinOp_Field_LeftAlias[A, B, C](self)(typeA, typeB, typeC)
    def rightParent: Rep[Operator[B]] = hashJoinOp_Field_RightParent[A, B, C](self)(typeA, typeB, typeC)
    def leftParent: Rep[Operator[A]] = hashJoinOp_Field_LeftParent[A, B, C](self)(typeA, typeB, typeC)
    def NullDynamicRecord: Rep[DynamicCompositeRecord[A, B]] = hashJoinOp_Field_NullDynamicRecord[A, B, C](self)(typeA, typeB, typeC)
  }
  object HashJoinOp {

  }
  // constructors
  def __newHashJoinOp[A <: ch.epfl.data.pardis.shallow.AbstractRecord, B <: ch.epfl.data.pardis.shallow.AbstractRecord, C](leftParent: Rep[Operator[A]], rightParent: Rep[Operator[B]], leftAlias: Rep[String], rightAlias: Rep[String])(joinCond: Rep[((A, B) => Boolean)])(leftHash: Rep[(A => C)])(rightHash: Rep[(B => C)])(implicit typeA: TypeRep[A], typeB: TypeRep[B], typeC: TypeRep[C]): Rep[HashJoinOp[A, B, C]] = hashJoinOpNew[A, B, C](leftParent, rightParent, leftAlias, rightAlias, joinCond, leftHash, rightHash)(typeA, typeB, typeC)
  // case classes
  case class HashJoinOpNew[A <: ch.epfl.data.pardis.shallow.AbstractRecord, B <: ch.epfl.data.pardis.shallow.AbstractRecord, C](leftParent: Rep[Operator[A]], rightParent: Rep[Operator[B]], leftAlias: Rep[String], rightAlias: Rep[String], joinCond: Rep[((A, B) => Boolean)], leftHash: Rep[((A) => C)], rightHash: Rep[((B) => C)])(implicit val typeA: TypeRep[A], val typeB: TypeRep[B], val typeC: TypeRep[C]) extends ConstructorDef[HashJoinOp[A, B, C]](List(typeA, typeB, typeC), "HashJoinOp", List(List(leftParent, rightParent, leftAlias, rightAlias), List(joinCond), List(leftHash), List(rightHash))) {
    override def curriedConstructor = (copy[A, B, C] _).curried
  }

  case class HashJoinOpForeach[A <: ch.epfl.data.pardis.shallow.AbstractRecord, B <: ch.epfl.data.pardis.shallow.AbstractRecord, C](self: Rep[HashJoinOp[A, B, C]], f: Rep[((DynamicCompositeRecord[A, B]) => Unit)])(implicit val typeA: TypeRep[A], val typeB: TypeRep[B], val typeC: TypeRep[C]) extends FunctionDef[Unit](Some(self), "foreach", List(List(f))) {
    override def curriedConstructor = (copy[A, B, C] _).curried
  }

  case class HashJoinOpFindFirst[A <: ch.epfl.data.pardis.shallow.AbstractRecord, B <: ch.epfl.data.pardis.shallow.AbstractRecord, C](self: Rep[HashJoinOp[A, B, C]], cond: Rep[((DynamicCompositeRecord[A, B]) => Boolean)])(implicit val typeA: TypeRep[A], val typeB: TypeRep[B], val typeC: TypeRep[C]) extends FunctionDef[DynamicCompositeRecord[A, B]](Some(self), "findFirst", List(List(cond))) {
    override def curriedConstructor = (copy[A, B, C] _).curried
  }

  case class HashJoinOpNullDynamicRecord[A <: ch.epfl.data.pardis.shallow.AbstractRecord, B <: ch.epfl.data.pardis.shallow.AbstractRecord, C, D](self: Rep[HashJoinOp[A, B, C]])(implicit val typeA: TypeRep[A], val typeB: TypeRep[B], val typeC: TypeRep[C], val typeD: TypeRep[D], val di: DummyImplicit) extends FunctionDef[D](Some(self), "NullDynamicRecord", List()) {
    override def curriedConstructor = (copy[A, B, C, D] _)
  }

  case class HashJoinOpOpen[A <: ch.epfl.data.pardis.shallow.AbstractRecord, B <: ch.epfl.data.pardis.shallow.AbstractRecord, C](self: Rep[HashJoinOp[A, B, C]])(implicit val typeA: TypeRep[A], val typeB: TypeRep[B], val typeC: TypeRep[C]) extends FunctionDef[Unit](Some(self), "open", List(List())) {
    override def curriedConstructor = (copy[A, B, C] _)
  }

  case class HashJoinOpNext[A <: ch.epfl.data.pardis.shallow.AbstractRecord, B <: ch.epfl.data.pardis.shallow.AbstractRecord, C](self: Rep[HashJoinOp[A, B, C]])(implicit val typeA: TypeRep[A], val typeB: TypeRep[B], val typeC: TypeRep[C]) extends FunctionDef[DynamicCompositeRecord[A, B]](Some(self), "next", List(List())) {
    override def curriedConstructor = (copy[A, B, C] _)
  }

  case class HashJoinOpClose[A <: ch.epfl.data.pardis.shallow.AbstractRecord, B <: ch.epfl.data.pardis.shallow.AbstractRecord, C](self: Rep[HashJoinOp[A, B, C]])(implicit val typeA: TypeRep[A], val typeB: TypeRep[B], val typeC: TypeRep[C]) extends FunctionDef[Unit](Some(self), "close", List(List())) {
    override def curriedConstructor = (copy[A, B, C] _)
  }

  case class HashJoinOpReset[A <: ch.epfl.data.pardis.shallow.AbstractRecord, B <: ch.epfl.data.pardis.shallow.AbstractRecord, C](self: Rep[HashJoinOp[A, B, C]])(implicit val typeA: TypeRep[A], val typeB: TypeRep[B], val typeC: TypeRep[C]) extends FunctionDef[Unit](Some(self), "reset", List(List())) {
    override def curriedConstructor = (copy[A, B, C] _)
  }

  case class HashJoinOp_Field_TmpLine_$eq[A <: ch.epfl.data.pardis.shallow.AbstractRecord, B <: ch.epfl.data.pardis.shallow.AbstractRecord, C](self: Rep[HashJoinOp[A, B, C]], x$1: Rep[B])(implicit val typeA: TypeRep[A], val typeB: TypeRep[B], val typeC: TypeRep[C]) extends FieldSetter[B](self, "tmpLine", x$1) {
    override def curriedConstructor = (copy[A, B, C] _).curried
  }

  case class HashJoinOp_Field_TmpLine[A <: ch.epfl.data.pardis.shallow.AbstractRecord, B <: ch.epfl.data.pardis.shallow.AbstractRecord, C](self: Rep[HashJoinOp[A, B, C]])(implicit val typeA: TypeRep[A], val typeB: TypeRep[B], val typeC: TypeRep[C]) extends FieldGetter[B](self, "tmpLine") {
    override def curriedConstructor = (copy[A, B, C] _)
  }

  case class HashJoinOp_Field_Hm[A <: ch.epfl.data.pardis.shallow.AbstractRecord, B <: ch.epfl.data.pardis.shallow.AbstractRecord, C](self: Rep[HashJoinOp[A, B, C]])(implicit val typeA: TypeRep[A], val typeB: TypeRep[B], val typeC: TypeRep[C]) extends FieldDef[HashMap[C, ArrayBuffer[A]]](self, "hm") {
    override def curriedConstructor = (copy[A, B, C] _)
    override def isPure = true

  }

  case class HashJoinOp_Field_TmpBuffer_$eq[A <: ch.epfl.data.pardis.shallow.AbstractRecord, B <: ch.epfl.data.pardis.shallow.AbstractRecord, C](self: Rep[HashJoinOp[A, B, C]], x$1: Rep[ArrayBuffer[A]])(implicit val typeA: TypeRep[A], val typeB: TypeRep[B], val typeC: TypeRep[C]) extends FieldSetter[ArrayBuffer[A]](self, "tmpBuffer", x$1) {
    override def curriedConstructor = (copy[A, B, C] _).curried
  }

  case class HashJoinOp_Field_TmpBuffer[A <: ch.epfl.data.pardis.shallow.AbstractRecord, B <: ch.epfl.data.pardis.shallow.AbstractRecord, C](self: Rep[HashJoinOp[A, B, C]])(implicit val typeA: TypeRep[A], val typeB: TypeRep[B], val typeC: TypeRep[C]) extends FieldGetter[ArrayBuffer[A]](self, "tmpBuffer") {
    override def curriedConstructor = (copy[A, B, C] _)
  }

  case class HashJoinOp_Field_TmpCount_$eq[A <: ch.epfl.data.pardis.shallow.AbstractRecord, B <: ch.epfl.data.pardis.shallow.AbstractRecord, C](self: Rep[HashJoinOp[A, B, C]], x$1: Rep[Int])(implicit val typeA: TypeRep[A], val typeB: TypeRep[B], val typeC: TypeRep[C]) extends FieldSetter[Int](self, "tmpCount", x$1) {
    override def curriedConstructor = (copy[A, B, C] _).curried
  }

  case class HashJoinOp_Field_TmpCount[A <: ch.epfl.data.pardis.shallow.AbstractRecord, B <: ch.epfl.data.pardis.shallow.AbstractRecord, C](self: Rep[HashJoinOp[A, B, C]])(implicit val typeA: TypeRep[A], val typeB: TypeRep[B], val typeC: TypeRep[C]) extends FieldGetter[Int](self, "tmpCount") {
    override def curriedConstructor = (copy[A, B, C] _)
  }

  case class HashJoinOp_Field_RightHash[A <: ch.epfl.data.pardis.shallow.AbstractRecord, B <: ch.epfl.data.pardis.shallow.AbstractRecord, C](self: Rep[HashJoinOp[A, B, C]])(implicit val typeA: TypeRep[A], val typeB: TypeRep[B], val typeC: TypeRep[C]) extends FieldDef[(B => C)](self, "rightHash") {
    override def curriedConstructor = (copy[A, B, C] _)
    override def isPure = true

  }

  case class HashJoinOp_Field_LeftHash[A <: ch.epfl.data.pardis.shallow.AbstractRecord, B <: ch.epfl.data.pardis.shallow.AbstractRecord, C](self: Rep[HashJoinOp[A, B, C]])(implicit val typeA: TypeRep[A], val typeB: TypeRep[B], val typeC: TypeRep[C]) extends FieldDef[(A => C)](self, "leftHash") {
    override def curriedConstructor = (copy[A, B, C] _)
    override def isPure = true

  }

  case class HashJoinOp_Field_JoinCond[A <: ch.epfl.data.pardis.shallow.AbstractRecord, B <: ch.epfl.data.pardis.shallow.AbstractRecord, C](self: Rep[HashJoinOp[A, B, C]])(implicit val typeA: TypeRep[A], val typeB: TypeRep[B], val typeC: TypeRep[C]) extends FieldDef[((A, B) => Boolean)](self, "joinCond") {
    override def curriedConstructor = (copy[A, B, C] _)
    override def isPure = true

  }

  case class HashJoinOp_Field_RightAlias[A <: ch.epfl.data.pardis.shallow.AbstractRecord, B <: ch.epfl.data.pardis.shallow.AbstractRecord, C](self: Rep[HashJoinOp[A, B, C]])(implicit val typeA: TypeRep[A], val typeB: TypeRep[B], val typeC: TypeRep[C]) extends FieldDef[String](self, "rightAlias") {
    override def curriedConstructor = (copy[A, B, C] _)
    override def isPure = true

  }

  case class HashJoinOp_Field_LeftAlias[A <: ch.epfl.data.pardis.shallow.AbstractRecord, B <: ch.epfl.data.pardis.shallow.AbstractRecord, C](self: Rep[HashJoinOp[A, B, C]])(implicit val typeA: TypeRep[A], val typeB: TypeRep[B], val typeC: TypeRep[C]) extends FieldDef[String](self, "leftAlias") {
    override def curriedConstructor = (copy[A, B, C] _)
    override def isPure = true

  }

  case class HashJoinOp_Field_RightParent[A <: ch.epfl.data.pardis.shallow.AbstractRecord, B <: ch.epfl.data.pardis.shallow.AbstractRecord, C](self: Rep[HashJoinOp[A, B, C]])(implicit val typeA: TypeRep[A], val typeB: TypeRep[B], val typeC: TypeRep[C]) extends FieldDef[Operator[B]](self, "rightParent") {
    override def curriedConstructor = (copy[A, B, C] _)
    override def isPure = true

  }

  case class HashJoinOp_Field_LeftParent[A <: ch.epfl.data.pardis.shallow.AbstractRecord, B <: ch.epfl.data.pardis.shallow.AbstractRecord, C](self: Rep[HashJoinOp[A, B, C]])(implicit val typeA: TypeRep[A], val typeB: TypeRep[B], val typeC: TypeRep[C]) extends FieldDef[Operator[A]](self, "leftParent") {
    override def curriedConstructor = (copy[A, B, C] _)
    override def isPure = true

  }

  case class HashJoinOp_Field_NullDynamicRecord[A <: ch.epfl.data.pardis.shallow.AbstractRecord, B <: ch.epfl.data.pardis.shallow.AbstractRecord, C](self: Rep[HashJoinOp[A, B, C]])(implicit val typeA: TypeRep[A], val typeB: TypeRep[B], val typeC: TypeRep[C]) extends FieldDef[DynamicCompositeRecord[A, B]](self, "NullDynamicRecord") {
    override def curriedConstructor = (copy[A, B, C] _)
    override def isPure = true

  }

  // method definitions
  def hashJoinOpNew[A <: ch.epfl.data.pardis.shallow.AbstractRecord, B <: ch.epfl.data.pardis.shallow.AbstractRecord, C](leftParent: Rep[Operator[A]], rightParent: Rep[Operator[B]], leftAlias: Rep[String], rightAlias: Rep[String], joinCond: Rep[((A, B) => Boolean)], leftHash: Rep[((A) => C)], rightHash: Rep[((B) => C)])(implicit typeA: TypeRep[A], typeB: TypeRep[B], typeC: TypeRep[C]): Rep[HashJoinOp[A, B, C]] = HashJoinOpNew[A, B, C](leftParent, rightParent, leftAlias, rightAlias, joinCond, leftHash, rightHash)
  def hashJoinOpForeach[A <: ch.epfl.data.pardis.shallow.AbstractRecord, B <: ch.epfl.data.pardis.shallow.AbstractRecord, C](self: Rep[HashJoinOp[A, B, C]], f: Rep[((DynamicCompositeRecord[A, B]) => Unit)])(implicit typeA: TypeRep[A], typeB: TypeRep[B], typeC: TypeRep[C]): Rep[Unit] = HashJoinOpForeach[A, B, C](self, f)
  def hashJoinOpFindFirst[A <: ch.epfl.data.pardis.shallow.AbstractRecord, B <: ch.epfl.data.pardis.shallow.AbstractRecord, C](self: Rep[HashJoinOp[A, B, C]], cond: Rep[((DynamicCompositeRecord[A, B]) => Boolean)])(implicit typeA: TypeRep[A], typeB: TypeRep[B], typeC: TypeRep[C]): Rep[DynamicCompositeRecord[A, B]] = HashJoinOpFindFirst[A, B, C](self, cond)
  def hashJoinOpNullDynamicRecord[A <: ch.epfl.data.pardis.shallow.AbstractRecord, B <: ch.epfl.data.pardis.shallow.AbstractRecord, C, D](self: Rep[HashJoinOp[A, B, C]])(implicit typeA: TypeRep[A], typeB: TypeRep[B], typeC: TypeRep[C], typeD: TypeRep[D], di: DummyImplicit): Rep[D] = HashJoinOpNullDynamicRecord[A, B, C, D](self)
  def hashJoinOpOpen[A <: ch.epfl.data.pardis.shallow.AbstractRecord, B <: ch.epfl.data.pardis.shallow.AbstractRecord, C](self: Rep[HashJoinOp[A, B, C]])(implicit typeA: TypeRep[A], typeB: TypeRep[B], typeC: TypeRep[C]): Rep[Unit] = HashJoinOpOpen[A, B, C](self)
  def hashJoinOpNext[A <: ch.epfl.data.pardis.shallow.AbstractRecord, B <: ch.epfl.data.pardis.shallow.AbstractRecord, C](self: Rep[HashJoinOp[A, B, C]])(implicit typeA: TypeRep[A], typeB: TypeRep[B], typeC: TypeRep[C]): Rep[DynamicCompositeRecord[A, B]] = HashJoinOpNext[A, B, C](self)
  def hashJoinOpClose[A <: ch.epfl.data.pardis.shallow.AbstractRecord, B <: ch.epfl.data.pardis.shallow.AbstractRecord, C](self: Rep[HashJoinOp[A, B, C]])(implicit typeA: TypeRep[A], typeB: TypeRep[B], typeC: TypeRep[C]): Rep[Unit] = HashJoinOpClose[A, B, C](self)
  def hashJoinOpReset[A <: ch.epfl.data.pardis.shallow.AbstractRecord, B <: ch.epfl.data.pardis.shallow.AbstractRecord, C](self: Rep[HashJoinOp[A, B, C]])(implicit typeA: TypeRep[A], typeB: TypeRep[B], typeC: TypeRep[C]): Rep[Unit] = HashJoinOpReset[A, B, C](self)
  def hashJoinOp_Field_TmpLine_$eq[A <: ch.epfl.data.pardis.shallow.AbstractRecord, B <: ch.epfl.data.pardis.shallow.AbstractRecord, C](self: Rep[HashJoinOp[A, B, C]], x$1: Rep[B])(implicit typeA: TypeRep[A], typeB: TypeRep[B], typeC: TypeRep[C]): Rep[Unit] = HashJoinOp_Field_TmpLine_$eq[A, B, C](self, x$1)
  def hashJoinOp_Field_TmpLine[A <: ch.epfl.data.pardis.shallow.AbstractRecord, B <: ch.epfl.data.pardis.shallow.AbstractRecord, C](self: Rep[HashJoinOp[A, B, C]])(implicit typeA: TypeRep[A], typeB: TypeRep[B], typeC: TypeRep[C]): Rep[B] = HashJoinOp_Field_TmpLine[A, B, C](self)
  def hashJoinOp_Field_Hm[A <: ch.epfl.data.pardis.shallow.AbstractRecord, B <: ch.epfl.data.pardis.shallow.AbstractRecord, C](self: Rep[HashJoinOp[A, B, C]])(implicit typeA: TypeRep[A], typeB: TypeRep[B], typeC: TypeRep[C]): Rep[HashMap[C, ArrayBuffer[A]]] = HashJoinOp_Field_Hm[A, B, C](self)
  def hashJoinOp_Field_TmpBuffer_$eq[A <: ch.epfl.data.pardis.shallow.AbstractRecord, B <: ch.epfl.data.pardis.shallow.AbstractRecord, C](self: Rep[HashJoinOp[A, B, C]], x$1: Rep[ArrayBuffer[A]])(implicit typeA: TypeRep[A], typeB: TypeRep[B], typeC: TypeRep[C]): Rep[Unit] = HashJoinOp_Field_TmpBuffer_$eq[A, B, C](self, x$1)
  def hashJoinOp_Field_TmpBuffer[A <: ch.epfl.data.pardis.shallow.AbstractRecord, B <: ch.epfl.data.pardis.shallow.AbstractRecord, C](self: Rep[HashJoinOp[A, B, C]])(implicit typeA: TypeRep[A], typeB: TypeRep[B], typeC: TypeRep[C]): Rep[ArrayBuffer[A]] = HashJoinOp_Field_TmpBuffer[A, B, C](self)
  def hashJoinOp_Field_TmpCount_$eq[A <: ch.epfl.data.pardis.shallow.AbstractRecord, B <: ch.epfl.data.pardis.shallow.AbstractRecord, C](self: Rep[HashJoinOp[A, B, C]], x$1: Rep[Int])(implicit typeA: TypeRep[A], typeB: TypeRep[B], typeC: TypeRep[C]): Rep[Unit] = HashJoinOp_Field_TmpCount_$eq[A, B, C](self, x$1)
  def hashJoinOp_Field_TmpCount[A <: ch.epfl.data.pardis.shallow.AbstractRecord, B <: ch.epfl.data.pardis.shallow.AbstractRecord, C](self: Rep[HashJoinOp[A, B, C]])(implicit typeA: TypeRep[A], typeB: TypeRep[B], typeC: TypeRep[C]): Rep[Int] = HashJoinOp_Field_TmpCount[A, B, C](self)
  def hashJoinOp_Field_RightHash[A <: ch.epfl.data.pardis.shallow.AbstractRecord, B <: ch.epfl.data.pardis.shallow.AbstractRecord, C](self: Rep[HashJoinOp[A, B, C]])(implicit typeA: TypeRep[A], typeB: TypeRep[B], typeC: TypeRep[C]): Rep[(B => C)] = HashJoinOp_Field_RightHash[A, B, C](self)
  def hashJoinOp_Field_LeftHash[A <: ch.epfl.data.pardis.shallow.AbstractRecord, B <: ch.epfl.data.pardis.shallow.AbstractRecord, C](self: Rep[HashJoinOp[A, B, C]])(implicit typeA: TypeRep[A], typeB: TypeRep[B], typeC: TypeRep[C]): Rep[(A => C)] = HashJoinOp_Field_LeftHash[A, B, C](self)
  def hashJoinOp_Field_JoinCond[A <: ch.epfl.data.pardis.shallow.AbstractRecord, B <: ch.epfl.data.pardis.shallow.AbstractRecord, C](self: Rep[HashJoinOp[A, B, C]])(implicit typeA: TypeRep[A], typeB: TypeRep[B], typeC: TypeRep[C]): Rep[((A, B) => Boolean)] = HashJoinOp_Field_JoinCond[A, B, C](self)
  def hashJoinOp_Field_RightAlias[A <: ch.epfl.data.pardis.shallow.AbstractRecord, B <: ch.epfl.data.pardis.shallow.AbstractRecord, C](self: Rep[HashJoinOp[A, B, C]])(implicit typeA: TypeRep[A], typeB: TypeRep[B], typeC: TypeRep[C]): Rep[String] = HashJoinOp_Field_RightAlias[A, B, C](self)
  def hashJoinOp_Field_LeftAlias[A <: ch.epfl.data.pardis.shallow.AbstractRecord, B <: ch.epfl.data.pardis.shallow.AbstractRecord, C](self: Rep[HashJoinOp[A, B, C]])(implicit typeA: TypeRep[A], typeB: TypeRep[B], typeC: TypeRep[C]): Rep[String] = HashJoinOp_Field_LeftAlias[A, B, C](self)
  def hashJoinOp_Field_RightParent[A <: ch.epfl.data.pardis.shallow.AbstractRecord, B <: ch.epfl.data.pardis.shallow.AbstractRecord, C](self: Rep[HashJoinOp[A, B, C]])(implicit typeA: TypeRep[A], typeB: TypeRep[B], typeC: TypeRep[C]): Rep[Operator[B]] = HashJoinOp_Field_RightParent[A, B, C](self)
  def hashJoinOp_Field_LeftParent[A <: ch.epfl.data.pardis.shallow.AbstractRecord, B <: ch.epfl.data.pardis.shallow.AbstractRecord, C](self: Rep[HashJoinOp[A, B, C]])(implicit typeA: TypeRep[A], typeB: TypeRep[B], typeC: TypeRep[C]): Rep[Operator[A]] = HashJoinOp_Field_LeftParent[A, B, C](self)
  def hashJoinOp_Field_NullDynamicRecord[A <: ch.epfl.data.pardis.shallow.AbstractRecord, B <: ch.epfl.data.pardis.shallow.AbstractRecord, C](self: Rep[HashJoinOp[A, B, C]])(implicit typeA: TypeRep[A], typeB: TypeRep[B], typeC: TypeRep[C]): Rep[DynamicCompositeRecord[A, B]] = HashJoinOp_Field_NullDynamicRecord[A, B, C](self)
  type HashJoinOp[A <: ch.epfl.data.pardis.shallow.AbstractRecord, B <: ch.epfl.data.pardis.shallow.AbstractRecord, C] = ch.epfl.data.legobase.queryengine.volcano.HashJoinOp[A, B, C]
  case class HashJoinOpType[A <: ch.epfl.data.pardis.shallow.AbstractRecord, B <: ch.epfl.data.pardis.shallow.AbstractRecord, C](typeA: TypeRep[A], typeB: TypeRep[B], typeC: TypeRep[C]) extends TypeRep[HashJoinOp[A, B, C]] {
    def rebuild(newArguments: TypeRep[_]*): TypeRep[_] = HashJoinOpType(newArguments(0).asInstanceOf[TypeRep[_ <: ch.epfl.data.pardis.shallow.AbstractRecord]], newArguments(1).asInstanceOf[TypeRep[_ <: ch.epfl.data.pardis.shallow.AbstractRecord]], newArguments(2).asInstanceOf[TypeRep[_]])
    private implicit val tagA = typeA.typeTag
    private implicit val tagB = typeB.typeTag
    private implicit val tagC = typeC.typeTag
    val name = s"HashJoinOp[${typeA.name}, ${typeB.name}, ${typeC.name}]"
    val typeArguments = List(typeA, typeB, typeC)

    val typeTag = scala.reflect.runtime.universe.typeTag[HashJoinOp[A, B, C]]
  }
  implicit def typeHashJoinOp[A <: ch.epfl.data.pardis.shallow.AbstractRecord: TypeRep, B <: ch.epfl.data.pardis.shallow.AbstractRecord: TypeRep, C: TypeRep] = HashJoinOpType(implicitly[TypeRep[A]], implicitly[TypeRep[B]], implicitly[TypeRep[C]])
}
trait HashJoinOpImplicits { this: HashJoinOpComponent =>
  // Add implicit conversions here!
}
trait HashJoinOpImplementations { self: DeepDSL =>
  override def hashJoinOpOpen[A <: ch.epfl.data.pardis.shallow.AbstractRecord, B <: ch.epfl.data.pardis.shallow.AbstractRecord, C](self: Rep[HashJoinOp[A, B, C]])(implicit typeA: TypeRep[A], typeB: TypeRep[B], typeC: TypeRep[C]): Rep[Unit] = {
    {
      self.leftParent.open();
      self.rightParent.open();
      self.leftParent.foreach(__lambda(((t: this.Rep[A]) => {
        val k: this.Rep[C] = __app(self.leftHash).apply(t);
        val v: this.Rep[scala.collection.mutable.ArrayBuffer[A]] = self.hm.getOrElseUpdate(k, ArrayBuffer.apply[A]());
        v.append(t)
      })))
    }
  }
  override def hashJoinOpNext[A <: ch.epfl.data.pardis.shallow.AbstractRecord, B <: ch.epfl.data.pardis.shallow.AbstractRecord, C](self: Rep[HashJoinOp[A, B, C]])(implicit typeA: TypeRep[A], typeB: TypeRep[B], typeC: TypeRep[C]): Rep[DynamicCompositeRecord[A, B]] = {
    {
      __ifThenElse(infix_$bang$eq(self.tmpCount, unit(-1)), {
        __whileDo(self.tmpCount.$less(self.tmpBuffer.size).$amp$amp(__app(self.joinCond).apply(self.tmpBuffer.apply(self.tmpCount), self.tmpLine).unary_$bang), self.tmpCount_$eq(self.tmpCount.$plus(unit(1))));
        __ifThenElse(infix_$eq$eq(self.tmpCount, self.tmpBuffer.size), self.tmpCount_$eq(unit(-1)), unit(()))
      }, unit(()));
      __ifThenElse(infix_$eq$eq(self.tmpCount, unit(-1)), self.tmpLine_$eq(self.rightParent.findFirst(__lambda(((t: this.Rep[B]) => {
        val k: this.Rep[C] = __app(self.rightHash).apply(t);
        __ifThenElse(self.hm.contains(k), {
          self.tmpBuffer_$eq(self.hm.apply(k));
          self.tmpCount_$eq(self.tmpBuffer.indexWhere(__lambda(((e: this.Rep[A]) => __app(self.joinCond).apply(e, t)))));
          infix_$bang$eq(self.tmpCount, unit(-1))
        }, unit(false))
      })))), unit(()));
      __ifThenElse(infix_$bang$eq(self.tmpLine, self.NullDynamicRecord[B]).$amp$amp(infix_$bang$eq(self.tmpCount, unit(-1))), {
        val res: this.Rep[ch.epfl.data.pardis.shallow.DynamicCompositeRecord[A, B]] = RecordOps[A](self.tmpBuffer.apply(self.tmpCount)).concatenateDynamic[B](self.tmpLine, self.leftAlias, self.rightAlias);
        self.tmpCount_$eq(self.tmpCount.$plus(unit(1)));
        res
      }, self.NullDynamicRecord[ch.epfl.data.pardis.shallow.DynamicCompositeRecord[A, B]])
    }
  }
  override def hashJoinOpClose[A <: ch.epfl.data.pardis.shallow.AbstractRecord, B <: ch.epfl.data.pardis.shallow.AbstractRecord, C](self: Rep[HashJoinOp[A, B, C]])(implicit typeA: TypeRep[A], typeB: TypeRep[B], typeC: TypeRep[C]): Rep[Unit] = {
    unit(())
  }
  override def hashJoinOpReset[A <: ch.epfl.data.pardis.shallow.AbstractRecord, B <: ch.epfl.data.pardis.shallow.AbstractRecord, C](self: Rep[HashJoinOp[A, B, C]])(implicit typeA: TypeRep[A], typeB: TypeRep[B], typeC: TypeRep[C]): Rep[Unit] = {
    {
      self.rightParent.reset();
      self.leftParent.reset();
      self.hm.clear();
      self.tmpLine_$eq(self.NullDynamicRecord[B]);
      self.tmpCount_$eq(unit(0));
      self.tmpBuffer.clear()
    }
  }
}
trait HashJoinOpComponent extends HashJoinOpOps with HashJoinOpImplicits { self: OperatorsComponent => }
trait WindowOpOps extends Base { this: OperatorsComponent =>
  implicit class WindowOpRep[A, B, C](self: Rep[WindowOp[A, B, C]])(implicit typeA: TypeRep[A], typeB: TypeRep[B], typeC: TypeRep[C]) {
    def foreach(f: Rep[(WindowRecord[B, C] => Unit)]): Rep[Unit] = windowOpForeach[A, B, C](self, f)(typeA, typeB, typeC)
    def findFirst(cond: Rep[(WindowRecord[B, C] => Boolean)]): Rep[WindowRecord[B, C]] = windowOpFindFirst[A, B, C](self, cond)(typeA, typeB, typeC)
    def NullDynamicRecord[D](implicit typeD: TypeRep[D], di: DummyImplicit): Rep[D] = windowOpNullDynamicRecord[A, B, C, D](self)(typeA, typeB, typeC, typeD, di)
    def open(): Rep[Unit] = windowOpOpen[A, B, C](self)(typeA, typeB, typeC)
    def next(): Rep[WindowRecord[B, C]] = windowOpNext[A, B, C](self)(typeA, typeB, typeC)
    def close(): Rep[Unit] = windowOpClose[A, B, C](self)(typeA, typeB, typeC)
    def reset(): Rep[Unit] = windowOpReset[A, B, C](self)(typeA, typeB, typeC)
    def keySet_=(x$1: Rep[Set[B]]): Rep[Unit] = windowOp_Field_KeySet_$eq[A, B, C](self, x$1)(typeA, typeB, typeC)
    def keySet: Rep[Set[B]] = windowOp_Field_KeySet[A, B, C](self)(typeA, typeB, typeC)
    def hm: Rep[HashMap[B, ArrayBuffer[A]]] = windowOp_Field_Hm[A, B, C](self)(typeA, typeB, typeC)
    def wndf: Rep[(ArrayBuffer[A] => C)] = windowOp_Field_Wndf[A, B, C](self)(typeA, typeB, typeC)
    def grp: Rep[(A => B)] = windowOp_Field_Grp[A, B, C](self)(typeA, typeB, typeC)
    def parent: Rep[Operator[A]] = windowOp_Field_Parent[A, B, C](self)(typeA, typeB, typeC)
    def NullDynamicRecord: Rep[WindowRecord[B, C]] = windowOp_Field_NullDynamicRecord[A, B, C](self)(typeA, typeB, typeC)
  }
  object WindowOp {

  }
  // constructors
  def __newWindowOp[A, B, C](parent: Rep[Operator[A]])(grp: Rep[(A => B)])(wndf: Rep[(ArrayBuffer[A] => C)])(implicit typeA: TypeRep[A], typeB: TypeRep[B], typeC: TypeRep[C]): Rep[WindowOp[A, B, C]] = windowOpNew[A, B, C](parent, grp, wndf)(typeA, typeB, typeC)
  // case classes
  case class WindowOpNew[A, B, C](parent: Rep[Operator[A]], grp: Rep[((A) => B)], wndf: Rep[((ArrayBuffer[A]) => C)])(implicit val typeA: TypeRep[A], val typeB: TypeRep[B], val typeC: TypeRep[C]) extends ConstructorDef[WindowOp[A, B, C]](List(typeA, typeB, typeC), "WindowOp", List(List(parent), List(grp), List(wndf))) {
    override def curriedConstructor = (copy[A, B, C] _).curried
  }

  case class WindowOpForeach[A, B, C](self: Rep[WindowOp[A, B, C]], f: Rep[((WindowRecord[B, C]) => Unit)])(implicit val typeA: TypeRep[A], val typeB: TypeRep[B], val typeC: TypeRep[C]) extends FunctionDef[Unit](Some(self), "foreach", List(List(f))) {
    override def curriedConstructor = (copy[A, B, C] _).curried
  }

  case class WindowOpFindFirst[A, B, C](self: Rep[WindowOp[A, B, C]], cond: Rep[((WindowRecord[B, C]) => Boolean)])(implicit val typeA: TypeRep[A], val typeB: TypeRep[B], val typeC: TypeRep[C]) extends FunctionDef[WindowRecord[B, C]](Some(self), "findFirst", List(List(cond))) {
    override def curriedConstructor = (copy[A, B, C] _).curried
  }

  case class WindowOpNullDynamicRecord[A, B, C, D](self: Rep[WindowOp[A, B, C]])(implicit val typeA: TypeRep[A], val typeB: TypeRep[B], val typeC: TypeRep[C], val typeD: TypeRep[D], val di: DummyImplicit) extends FunctionDef[D](Some(self), "NullDynamicRecord", List()) {
    override def curriedConstructor = (copy[A, B, C, D] _)
  }

  case class WindowOpOpen[A, B, C](self: Rep[WindowOp[A, B, C]])(implicit val typeA: TypeRep[A], val typeB: TypeRep[B], val typeC: TypeRep[C]) extends FunctionDef[Unit](Some(self), "open", List(List())) {
    override def curriedConstructor = (copy[A, B, C] _)
  }

  case class WindowOpNext[A, B, C](self: Rep[WindowOp[A, B, C]])(implicit val typeA: TypeRep[A], val typeB: TypeRep[B], val typeC: TypeRep[C]) extends FunctionDef[WindowRecord[B, C]](Some(self), "next", List(List())) {
    override def curriedConstructor = (copy[A, B, C] _)
  }

  case class WindowOpClose[A, B, C](self: Rep[WindowOp[A, B, C]])(implicit val typeA: TypeRep[A], val typeB: TypeRep[B], val typeC: TypeRep[C]) extends FunctionDef[Unit](Some(self), "close", List(List())) {
    override def curriedConstructor = (copy[A, B, C] _)
  }

  case class WindowOpReset[A, B, C](self: Rep[WindowOp[A, B, C]])(implicit val typeA: TypeRep[A], val typeB: TypeRep[B], val typeC: TypeRep[C]) extends FunctionDef[Unit](Some(self), "reset", List(List())) {
    override def curriedConstructor = (copy[A, B, C] _)
  }

  case class WindowOp_Field_KeySet_$eq[A, B, C](self: Rep[WindowOp[A, B, C]], x$1: Rep[Set[B]])(implicit val typeA: TypeRep[A], val typeB: TypeRep[B], val typeC: TypeRep[C]) extends FieldSetter[Set[B]](self, "keySet", x$1) {
    override def curriedConstructor = (copy[A, B, C] _).curried
  }

  case class WindowOp_Field_KeySet[A, B, C](self: Rep[WindowOp[A, B, C]])(implicit val typeA: TypeRep[A], val typeB: TypeRep[B], val typeC: TypeRep[C]) extends FieldGetter[Set[B]](self, "keySet") {
    override def curriedConstructor = (copy[A, B, C] _)
  }

  case class WindowOp_Field_Hm[A, B, C](self: Rep[WindowOp[A, B, C]])(implicit val typeA: TypeRep[A], val typeB: TypeRep[B], val typeC: TypeRep[C]) extends FieldDef[HashMap[B, ArrayBuffer[A]]](self, "hm") {
    override def curriedConstructor = (copy[A, B, C] _)
    override def isPure = true

  }

  case class WindowOp_Field_Wndf[A, B, C](self: Rep[WindowOp[A, B, C]])(implicit val typeA: TypeRep[A], val typeB: TypeRep[B], val typeC: TypeRep[C]) extends FieldDef[(ArrayBuffer[A] => C)](self, "wndf") {
    override def curriedConstructor = (copy[A, B, C] _)
    override def isPure = true

  }

  case class WindowOp_Field_Grp[A, B, C](self: Rep[WindowOp[A, B, C]])(implicit val typeA: TypeRep[A], val typeB: TypeRep[B], val typeC: TypeRep[C]) extends FieldDef[(A => B)](self, "grp") {
    override def curriedConstructor = (copy[A, B, C] _)
    override def isPure = true

  }

  case class WindowOp_Field_Parent[A, B, C](self: Rep[WindowOp[A, B, C]])(implicit val typeA: TypeRep[A], val typeB: TypeRep[B], val typeC: TypeRep[C]) extends FieldDef[Operator[A]](self, "parent") {
    override def curriedConstructor = (copy[A, B, C] _)
    override def isPure = true

  }

  case class WindowOp_Field_NullDynamicRecord[A, B, C](self: Rep[WindowOp[A, B, C]])(implicit val typeA: TypeRep[A], val typeB: TypeRep[B], val typeC: TypeRep[C]) extends FieldDef[WindowRecord[B, C]](self, "NullDynamicRecord") {
    override def curriedConstructor = (copy[A, B, C] _)
    override def isPure = true

  }

  // method definitions
  def windowOpNew[A, B, C](parent: Rep[Operator[A]], grp: Rep[((A) => B)], wndf: Rep[((ArrayBuffer[A]) => C)])(implicit typeA: TypeRep[A], typeB: TypeRep[B], typeC: TypeRep[C]): Rep[WindowOp[A, B, C]] = WindowOpNew[A, B, C](parent, grp, wndf)
  def windowOpForeach[A, B, C](self: Rep[WindowOp[A, B, C]], f: Rep[((WindowRecord[B, C]) => Unit)])(implicit typeA: TypeRep[A], typeB: TypeRep[B], typeC: TypeRep[C]): Rep[Unit] = WindowOpForeach[A, B, C](self, f)
  def windowOpFindFirst[A, B, C](self: Rep[WindowOp[A, B, C]], cond: Rep[((WindowRecord[B, C]) => Boolean)])(implicit typeA: TypeRep[A], typeB: TypeRep[B], typeC: TypeRep[C]): Rep[WindowRecord[B, C]] = WindowOpFindFirst[A, B, C](self, cond)
  def windowOpNullDynamicRecord[A, B, C, D](self: Rep[WindowOp[A, B, C]])(implicit typeA: TypeRep[A], typeB: TypeRep[B], typeC: TypeRep[C], typeD: TypeRep[D], di: DummyImplicit): Rep[D] = WindowOpNullDynamicRecord[A, B, C, D](self)
  def windowOpOpen[A, B, C](self: Rep[WindowOp[A, B, C]])(implicit typeA: TypeRep[A], typeB: TypeRep[B], typeC: TypeRep[C]): Rep[Unit] = WindowOpOpen[A, B, C](self)
  def windowOpNext[A, B, C](self: Rep[WindowOp[A, B, C]])(implicit typeA: TypeRep[A], typeB: TypeRep[B], typeC: TypeRep[C]): Rep[WindowRecord[B, C]] = WindowOpNext[A, B, C](self)
  def windowOpClose[A, B, C](self: Rep[WindowOp[A, B, C]])(implicit typeA: TypeRep[A], typeB: TypeRep[B], typeC: TypeRep[C]): Rep[Unit] = WindowOpClose[A, B, C](self)
  def windowOpReset[A, B, C](self: Rep[WindowOp[A, B, C]])(implicit typeA: TypeRep[A], typeB: TypeRep[B], typeC: TypeRep[C]): Rep[Unit] = WindowOpReset[A, B, C](self)
  def windowOp_Field_KeySet_$eq[A, B, C](self: Rep[WindowOp[A, B, C]], x$1: Rep[Set[B]])(implicit typeA: TypeRep[A], typeB: TypeRep[B], typeC: TypeRep[C]): Rep[Unit] = WindowOp_Field_KeySet_$eq[A, B, C](self, x$1)
  def windowOp_Field_KeySet[A, B, C](self: Rep[WindowOp[A, B, C]])(implicit typeA: TypeRep[A], typeB: TypeRep[B], typeC: TypeRep[C]): Rep[Set[B]] = WindowOp_Field_KeySet[A, B, C](self)
  def windowOp_Field_Hm[A, B, C](self: Rep[WindowOp[A, B, C]])(implicit typeA: TypeRep[A], typeB: TypeRep[B], typeC: TypeRep[C]): Rep[HashMap[B, ArrayBuffer[A]]] = WindowOp_Field_Hm[A, B, C](self)
  def windowOp_Field_Wndf[A, B, C](self: Rep[WindowOp[A, B, C]])(implicit typeA: TypeRep[A], typeB: TypeRep[B], typeC: TypeRep[C]): Rep[(ArrayBuffer[A] => C)] = WindowOp_Field_Wndf[A, B, C](self)
  def windowOp_Field_Grp[A, B, C](self: Rep[WindowOp[A, B, C]])(implicit typeA: TypeRep[A], typeB: TypeRep[B], typeC: TypeRep[C]): Rep[(A => B)] = WindowOp_Field_Grp[A, B, C](self)
  def windowOp_Field_Parent[A, B, C](self: Rep[WindowOp[A, B, C]])(implicit typeA: TypeRep[A], typeB: TypeRep[B], typeC: TypeRep[C]): Rep[Operator[A]] = WindowOp_Field_Parent[A, B, C](self)
  def windowOp_Field_NullDynamicRecord[A, B, C](self: Rep[WindowOp[A, B, C]])(implicit typeA: TypeRep[A], typeB: TypeRep[B], typeC: TypeRep[C]): Rep[WindowRecord[B, C]] = WindowOp_Field_NullDynamicRecord[A, B, C](self)
  type WindowOp[A, B, C] = ch.epfl.data.legobase.queryengine.volcano.WindowOp[A, B, C]
  case class WindowOpType[A, B, C](typeA: TypeRep[A], typeB: TypeRep[B], typeC: TypeRep[C]) extends TypeRep[WindowOp[A, B, C]] {
    def rebuild(newArguments: TypeRep[_]*): TypeRep[_] = WindowOpType(newArguments(0).asInstanceOf[TypeRep[_]], newArguments(1).asInstanceOf[TypeRep[_]], newArguments(2).asInstanceOf[TypeRep[_]])
    private implicit val tagA = typeA.typeTag
    private implicit val tagB = typeB.typeTag
    private implicit val tagC = typeC.typeTag
    val name = s"WindowOp[${typeA.name}, ${typeB.name}, ${typeC.name}]"
    val typeArguments = List(typeA, typeB, typeC)

    val typeTag = scala.reflect.runtime.universe.typeTag[WindowOp[A, B, C]]
  }
  implicit def typeWindowOp[A: TypeRep, B: TypeRep, C: TypeRep] = WindowOpType(implicitly[TypeRep[A]], implicitly[TypeRep[B]], implicitly[TypeRep[C]])
}
trait WindowOpImplicits { this: WindowOpComponent =>
  // Add implicit conversions here!
}
trait WindowOpImplementations { self: DeepDSL =>
  override def windowOpOpen[A, B, C](self: Rep[WindowOp[A, B, C]])(implicit typeA: TypeRep[A], typeB: TypeRep[B], typeC: TypeRep[C]): Rep[Unit] = {
    {
      self.parent.open();
      self.parent.foreach(__lambda(((t: this.Rep[A]) => {
        val key: this.Rep[B] = __app(self.grp).apply(t);
        val v: this.Rep[scala.collection.mutable.ArrayBuffer[A]] = self.hm.getOrElseUpdate(key, ArrayBuffer.apply[A]());
        v.append(t)
      })));
      self.keySet_$eq(Set.apply[B](self.hm.keySet.toSeq))
    }
  }
  override def windowOpNext[A, B, C](self: Rep[WindowOp[A, B, C]])(implicit typeA: TypeRep[A], typeB: TypeRep[B], typeC: TypeRep[C]): Rep[WindowRecord[B, C]] = {
    __ifThenElse(infix_$bang$eq(self.hm.size, unit(0)), {
      val key: this.Rep[B] = self.keySet.head;
      self.keySet.remove(key);
      val elem: this.Rep[scala.collection.mutable.ArrayBuffer[A]] = self.hm.remove(key).get;
      GenericEngine.newWindowRecord[B, C](key, __app(self.wndf).apply(elem))
    }, self.NullDynamicRecord)
  }
  override def windowOpClose[A, B, C](self: Rep[WindowOp[A, B, C]])(implicit typeA: TypeRep[A], typeB: TypeRep[B], typeC: TypeRep[C]): Rep[Unit] = {
    unit(())
  }
  override def windowOpReset[A, B, C](self: Rep[WindowOp[A, B, C]])(implicit typeA: TypeRep[A], typeB: TypeRep[B], typeC: TypeRep[C]): Rep[Unit] = {
    {
      self.parent.reset();
      self.hm.clear();
      self.open()
    }
  }
}
trait WindowOpComponent extends WindowOpOps with WindowOpImplicits { self: OperatorsComponent => }
trait LeftHashSemiJoinOpOps extends Base { this: OperatorsComponent =>
  implicit class LeftHashSemiJoinOpRep[A, B, C](self: Rep[LeftHashSemiJoinOp[A, B, C]])(implicit typeA: TypeRep[A], typeB: TypeRep[B], typeC: TypeRep[C]) {
    def foreach(f: Rep[(A => Unit)]): Rep[Unit] = leftHashSemiJoinOpForeach[A, B, C](self, f)(typeA, typeB, typeC)
    def findFirst(cond: Rep[(A => Boolean)]): Rep[A] = leftHashSemiJoinOpFindFirst[A, B, C](self, cond)(typeA, typeB, typeC)
    def NullDynamicRecord[D](implicit typeD: TypeRep[D], di: DummyImplicit): Rep[D] = leftHashSemiJoinOpNullDynamicRecord[A, B, C, D](self)(typeA, typeB, typeC, typeD, di)
    def open(): Rep[Unit] = leftHashSemiJoinOpOpen[A, B, C](self)(typeA, typeB, typeC)
    def next(): Rep[A] = leftHashSemiJoinOpNext[A, B, C](self)(typeA, typeB, typeC)
    def close(): Rep[Unit] = leftHashSemiJoinOpClose[A, B, C](self)(typeA, typeB, typeC)
    def reset(): Rep[Unit] = leftHashSemiJoinOpReset[A, B, C](self)(typeA, typeB, typeC)
    def hm: Rep[HashMap[C, ArrayBuffer[B]]] = leftHashSemiJoinOp_Field_Hm[A, B, C](self)(typeA, typeB, typeC)
    def rightHash: Rep[(B => C)] = leftHashSemiJoinOp_Field_RightHash[A, B, C](self)(typeA, typeB, typeC)
    def leftHash: Rep[(A => C)] = leftHashSemiJoinOp_Field_LeftHash[A, B, C](self)(typeA, typeB, typeC)
    def joinCond: Rep[((A, B) => Boolean)] = leftHashSemiJoinOp_Field_JoinCond[A, B, C](self)(typeA, typeB, typeC)
    def rightParent: Rep[Operator[B]] = leftHashSemiJoinOp_Field_RightParent[A, B, C](self)(typeA, typeB, typeC)
    def leftParent: Rep[Operator[A]] = leftHashSemiJoinOp_Field_LeftParent[A, B, C](self)(typeA, typeB, typeC)
    def NullDynamicRecord: Rep[A] = leftHashSemiJoinOp_Field_NullDynamicRecord[A, B, C](self)(typeA, typeB, typeC)
  }
  object LeftHashSemiJoinOp {

  }
  // constructors
  def __newLeftHashSemiJoinOp[A, B, C](leftParent: Rep[Operator[A]], rightParent: Rep[Operator[B]])(joinCond: Rep[((A, B) => Boolean)])(leftHash: Rep[(A => C)])(rightHash: Rep[(B => C)])(implicit typeA: TypeRep[A], typeB: TypeRep[B], typeC: TypeRep[C]): Rep[LeftHashSemiJoinOp[A, B, C]] = leftHashSemiJoinOpNew[A, B, C](leftParent, rightParent, joinCond, leftHash, rightHash)(typeA, typeB, typeC)
  // case classes
  case class LeftHashSemiJoinOpNew[A, B, C](leftParent: Rep[Operator[A]], rightParent: Rep[Operator[B]], joinCond: Rep[((A, B) => Boolean)], leftHash: Rep[((A) => C)], rightHash: Rep[((B) => C)])(implicit val typeA: TypeRep[A], val typeB: TypeRep[B], val typeC: TypeRep[C]) extends ConstructorDef[LeftHashSemiJoinOp[A, B, C]](List(typeA, typeB, typeC), "LeftHashSemiJoinOp", List(List(leftParent, rightParent), List(joinCond), List(leftHash), List(rightHash))) {
    override def curriedConstructor = (copy[A, B, C] _).curried
  }

  case class LeftHashSemiJoinOpForeach[A, B, C](self: Rep[LeftHashSemiJoinOp[A, B, C]], f: Rep[((A) => Unit)])(implicit val typeA: TypeRep[A], val typeB: TypeRep[B], val typeC: TypeRep[C]) extends FunctionDef[Unit](Some(self), "foreach", List(List(f))) {
    override def curriedConstructor = (copy[A, B, C] _).curried
  }

  case class LeftHashSemiJoinOpFindFirst[A, B, C](self: Rep[LeftHashSemiJoinOp[A, B, C]], cond: Rep[((A) => Boolean)])(implicit val typeA: TypeRep[A], val typeB: TypeRep[B], val typeC: TypeRep[C]) extends FunctionDef[A](Some(self), "findFirst", List(List(cond))) {
    override def curriedConstructor = (copy[A, B, C] _).curried
  }

  case class LeftHashSemiJoinOpNullDynamicRecord[A, B, C, D](self: Rep[LeftHashSemiJoinOp[A, B, C]])(implicit val typeA: TypeRep[A], val typeB: TypeRep[B], val typeC: TypeRep[C], val typeD: TypeRep[D], val di: DummyImplicit) extends FunctionDef[D](Some(self), "NullDynamicRecord", List()) {
    override def curriedConstructor = (copy[A, B, C, D] _)
  }

  case class LeftHashSemiJoinOpOpen[A, B, C](self: Rep[LeftHashSemiJoinOp[A, B, C]])(implicit val typeA: TypeRep[A], val typeB: TypeRep[B], val typeC: TypeRep[C]) extends FunctionDef[Unit](Some(self), "open", List(List())) {
    override def curriedConstructor = (copy[A, B, C] _)
  }

  case class LeftHashSemiJoinOpNext[A, B, C](self: Rep[LeftHashSemiJoinOp[A, B, C]])(implicit val typeA: TypeRep[A], val typeB: TypeRep[B], val typeC: TypeRep[C]) extends FunctionDef[A](Some(self), "next", List(List())) {
    override def curriedConstructor = (copy[A, B, C] _)
  }

  case class LeftHashSemiJoinOpClose[A, B, C](self: Rep[LeftHashSemiJoinOp[A, B, C]])(implicit val typeA: TypeRep[A], val typeB: TypeRep[B], val typeC: TypeRep[C]) extends FunctionDef[Unit](Some(self), "close", List(List())) {
    override def curriedConstructor = (copy[A, B, C] _)
  }

  case class LeftHashSemiJoinOpReset[A, B, C](self: Rep[LeftHashSemiJoinOp[A, B, C]])(implicit val typeA: TypeRep[A], val typeB: TypeRep[B], val typeC: TypeRep[C]) extends FunctionDef[Unit](Some(self), "reset", List(List())) {
    override def curriedConstructor = (copy[A, B, C] _)
  }

  case class LeftHashSemiJoinOp_Field_Hm[A, B, C](self: Rep[LeftHashSemiJoinOp[A, B, C]])(implicit val typeA: TypeRep[A], val typeB: TypeRep[B], val typeC: TypeRep[C]) extends FieldDef[HashMap[C, ArrayBuffer[B]]](self, "hm") {
    override def curriedConstructor = (copy[A, B, C] _)
    override def isPure = true

  }

  case class LeftHashSemiJoinOp_Field_RightHash[A, B, C](self: Rep[LeftHashSemiJoinOp[A, B, C]])(implicit val typeA: TypeRep[A], val typeB: TypeRep[B], val typeC: TypeRep[C]) extends FieldDef[(B => C)](self, "rightHash") {
    override def curriedConstructor = (copy[A, B, C] _)
    override def isPure = true

  }

  case class LeftHashSemiJoinOp_Field_LeftHash[A, B, C](self: Rep[LeftHashSemiJoinOp[A, B, C]])(implicit val typeA: TypeRep[A], val typeB: TypeRep[B], val typeC: TypeRep[C]) extends FieldDef[(A => C)](self, "leftHash") {
    override def curriedConstructor = (copy[A, B, C] _)
    override def isPure = true

  }

  case class LeftHashSemiJoinOp_Field_JoinCond[A, B, C](self: Rep[LeftHashSemiJoinOp[A, B, C]])(implicit val typeA: TypeRep[A], val typeB: TypeRep[B], val typeC: TypeRep[C]) extends FieldDef[((A, B) => Boolean)](self, "joinCond") {
    override def curriedConstructor = (copy[A, B, C] _)
    override def isPure = true

  }

  case class LeftHashSemiJoinOp_Field_RightParent[A, B, C](self: Rep[LeftHashSemiJoinOp[A, B, C]])(implicit val typeA: TypeRep[A], val typeB: TypeRep[B], val typeC: TypeRep[C]) extends FieldDef[Operator[B]](self, "rightParent") {
    override def curriedConstructor = (copy[A, B, C] _)
    override def isPure = true

  }

  case class LeftHashSemiJoinOp_Field_LeftParent[A, B, C](self: Rep[LeftHashSemiJoinOp[A, B, C]])(implicit val typeA: TypeRep[A], val typeB: TypeRep[B], val typeC: TypeRep[C]) extends FieldDef[Operator[A]](self, "leftParent") {
    override def curriedConstructor = (copy[A, B, C] _)
    override def isPure = true

  }

  case class LeftHashSemiJoinOp_Field_NullDynamicRecord[A, B, C](self: Rep[LeftHashSemiJoinOp[A, B, C]])(implicit val typeA: TypeRep[A], val typeB: TypeRep[B], val typeC: TypeRep[C]) extends FieldDef[A](self, "NullDynamicRecord") {
    override def curriedConstructor = (copy[A, B, C] _)
    override def isPure = true

  }

  // method definitions
  def leftHashSemiJoinOpNew[A, B, C](leftParent: Rep[Operator[A]], rightParent: Rep[Operator[B]], joinCond: Rep[((A, B) => Boolean)], leftHash: Rep[((A) => C)], rightHash: Rep[((B) => C)])(implicit typeA: TypeRep[A], typeB: TypeRep[B], typeC: TypeRep[C]): Rep[LeftHashSemiJoinOp[A, B, C]] = LeftHashSemiJoinOpNew[A, B, C](leftParent, rightParent, joinCond, leftHash, rightHash)
  def leftHashSemiJoinOpForeach[A, B, C](self: Rep[LeftHashSemiJoinOp[A, B, C]], f: Rep[((A) => Unit)])(implicit typeA: TypeRep[A], typeB: TypeRep[B], typeC: TypeRep[C]): Rep[Unit] = LeftHashSemiJoinOpForeach[A, B, C](self, f)
  def leftHashSemiJoinOpFindFirst[A, B, C](self: Rep[LeftHashSemiJoinOp[A, B, C]], cond: Rep[((A) => Boolean)])(implicit typeA: TypeRep[A], typeB: TypeRep[B], typeC: TypeRep[C]): Rep[A] = LeftHashSemiJoinOpFindFirst[A, B, C](self, cond)
  def leftHashSemiJoinOpNullDynamicRecord[A, B, C, D](self: Rep[LeftHashSemiJoinOp[A, B, C]])(implicit typeA: TypeRep[A], typeB: TypeRep[B], typeC: TypeRep[C], typeD: TypeRep[D], di: DummyImplicit): Rep[D] = LeftHashSemiJoinOpNullDynamicRecord[A, B, C, D](self)
  def leftHashSemiJoinOpOpen[A, B, C](self: Rep[LeftHashSemiJoinOp[A, B, C]])(implicit typeA: TypeRep[A], typeB: TypeRep[B], typeC: TypeRep[C]): Rep[Unit] = LeftHashSemiJoinOpOpen[A, B, C](self)
  def leftHashSemiJoinOpNext[A, B, C](self: Rep[LeftHashSemiJoinOp[A, B, C]])(implicit typeA: TypeRep[A], typeB: TypeRep[B], typeC: TypeRep[C]): Rep[A] = LeftHashSemiJoinOpNext[A, B, C](self)
  def leftHashSemiJoinOpClose[A, B, C](self: Rep[LeftHashSemiJoinOp[A, B, C]])(implicit typeA: TypeRep[A], typeB: TypeRep[B], typeC: TypeRep[C]): Rep[Unit] = LeftHashSemiJoinOpClose[A, B, C](self)
  def leftHashSemiJoinOpReset[A, B, C](self: Rep[LeftHashSemiJoinOp[A, B, C]])(implicit typeA: TypeRep[A], typeB: TypeRep[B], typeC: TypeRep[C]): Rep[Unit] = LeftHashSemiJoinOpReset[A, B, C](self)
  def leftHashSemiJoinOp_Field_Hm[A, B, C](self: Rep[LeftHashSemiJoinOp[A, B, C]])(implicit typeA: TypeRep[A], typeB: TypeRep[B], typeC: TypeRep[C]): Rep[HashMap[C, ArrayBuffer[B]]] = LeftHashSemiJoinOp_Field_Hm[A, B, C](self)
  def leftHashSemiJoinOp_Field_RightHash[A, B, C](self: Rep[LeftHashSemiJoinOp[A, B, C]])(implicit typeA: TypeRep[A], typeB: TypeRep[B], typeC: TypeRep[C]): Rep[(B => C)] = LeftHashSemiJoinOp_Field_RightHash[A, B, C](self)
  def leftHashSemiJoinOp_Field_LeftHash[A, B, C](self: Rep[LeftHashSemiJoinOp[A, B, C]])(implicit typeA: TypeRep[A], typeB: TypeRep[B], typeC: TypeRep[C]): Rep[(A => C)] = LeftHashSemiJoinOp_Field_LeftHash[A, B, C](self)
  def leftHashSemiJoinOp_Field_JoinCond[A, B, C](self: Rep[LeftHashSemiJoinOp[A, B, C]])(implicit typeA: TypeRep[A], typeB: TypeRep[B], typeC: TypeRep[C]): Rep[((A, B) => Boolean)] = LeftHashSemiJoinOp_Field_JoinCond[A, B, C](self)
  def leftHashSemiJoinOp_Field_RightParent[A, B, C](self: Rep[LeftHashSemiJoinOp[A, B, C]])(implicit typeA: TypeRep[A], typeB: TypeRep[B], typeC: TypeRep[C]): Rep[Operator[B]] = LeftHashSemiJoinOp_Field_RightParent[A, B, C](self)
  def leftHashSemiJoinOp_Field_LeftParent[A, B, C](self: Rep[LeftHashSemiJoinOp[A, B, C]])(implicit typeA: TypeRep[A], typeB: TypeRep[B], typeC: TypeRep[C]): Rep[Operator[A]] = LeftHashSemiJoinOp_Field_LeftParent[A, B, C](self)
  def leftHashSemiJoinOp_Field_NullDynamicRecord[A, B, C](self: Rep[LeftHashSemiJoinOp[A, B, C]])(implicit typeA: TypeRep[A], typeB: TypeRep[B], typeC: TypeRep[C]): Rep[A] = LeftHashSemiJoinOp_Field_NullDynamicRecord[A, B, C](self)
  type LeftHashSemiJoinOp[A, B, C] = ch.epfl.data.legobase.queryengine.volcano.LeftHashSemiJoinOp[A, B, C]
  case class LeftHashSemiJoinOpType[A, B, C](typeA: TypeRep[A], typeB: TypeRep[B], typeC: TypeRep[C]) extends TypeRep[LeftHashSemiJoinOp[A, B, C]] {
    def rebuild(newArguments: TypeRep[_]*): TypeRep[_] = LeftHashSemiJoinOpType(newArguments(0).asInstanceOf[TypeRep[_]], newArguments(1).asInstanceOf[TypeRep[_]], newArguments(2).asInstanceOf[TypeRep[_]])
    private implicit val tagA = typeA.typeTag
    private implicit val tagB = typeB.typeTag
    private implicit val tagC = typeC.typeTag
    val name = s"LeftHashSemiJoinOp[${typeA.name}, ${typeB.name}, ${typeC.name}]"
    val typeArguments = List(typeA, typeB, typeC)

    val typeTag = scala.reflect.runtime.universe.typeTag[LeftHashSemiJoinOp[A, B, C]]
  }
  implicit def typeLeftHashSemiJoinOp[A: TypeRep, B: TypeRep, C: TypeRep] = LeftHashSemiJoinOpType(implicitly[TypeRep[A]], implicitly[TypeRep[B]], implicitly[TypeRep[C]])
}
trait LeftHashSemiJoinOpImplicits { this: LeftHashSemiJoinOpComponent =>
  // Add implicit conversions here!
}
trait LeftHashSemiJoinOpImplementations { self: DeepDSL =>
  override def leftHashSemiJoinOpOpen[A, B, C](self: Rep[LeftHashSemiJoinOp[A, B, C]])(implicit typeA: TypeRep[A], typeB: TypeRep[B], typeC: TypeRep[C]): Rep[Unit] = {
    {
      self.leftParent.open();
      self.rightParent.open();
      self.rightParent.foreach(__lambda(((t: this.Rep[B]) => {
        val k: this.Rep[C] = __app(self.rightHash).apply(t);
        val v: this.Rep[scala.collection.mutable.ArrayBuffer[B]] = self.hm.getOrElseUpdate(k, ArrayBuffer.apply[B]());
        v.append(t)
      })))
    }
  }
  override def leftHashSemiJoinOpNext[A, B, C](self: Rep[LeftHashSemiJoinOp[A, B, C]])(implicit typeA: TypeRep[A], typeB: TypeRep[B], typeC: TypeRep[C]): Rep[A] = {
    self.leftParent.findFirst(__lambda(((t: this.Rep[A]) => {
      val k: this.Rep[C] = __app(self.leftHash).apply(t);
      __ifThenElse(self.hm.contains(k), {
        val tmpBuffer: this.Rep[scala.collection.mutable.ArrayBuffer[B]] = self.hm.apply(k);
        infix_$bang$eq(tmpBuffer.indexWhere(__lambda(((e: this.Rep[B]) => __app(self.joinCond).apply(t, e)))), unit(-1))
      }, unit(false))
    })))
  }
  override def leftHashSemiJoinOpClose[A, B, C](self: Rep[LeftHashSemiJoinOp[A, B, C]])(implicit typeA: TypeRep[A], typeB: TypeRep[B], typeC: TypeRep[C]): Rep[Unit] = {
    unit(())
  }
  override def leftHashSemiJoinOpReset[A, B, C](self: Rep[LeftHashSemiJoinOp[A, B, C]])(implicit typeA: TypeRep[A], typeB: TypeRep[B], typeC: TypeRep[C]): Rep[Unit] = {
    {
      self.rightParent.reset();
      self.leftParent.reset();
      self.hm.clear()
    }
  }
}
trait LeftHashSemiJoinOpComponent extends LeftHashSemiJoinOpOps with LeftHashSemiJoinOpImplicits { self: OperatorsComponent => }
trait NestedLoopsJoinOpOps extends Base { this: OperatorsComponent =>
  implicit class NestedLoopsJoinOpRep[A <: ch.epfl.data.pardis.shallow.AbstractRecord, B <: ch.epfl.data.pardis.shallow.AbstractRecord](self: Rep[NestedLoopsJoinOp[A, B]])(implicit typeA: TypeRep[A], typeB: TypeRep[B]) {
    def foreach(f: Rep[(DynamicCompositeRecord[A, B] => Unit)]): Rep[Unit] = nestedLoopsJoinOpForeach[A, B](self, f)(typeA, typeB)
    def findFirst(cond: Rep[(DynamicCompositeRecord[A, B] => Boolean)]): Rep[DynamicCompositeRecord[A, B]] = nestedLoopsJoinOpFindFirst[A, B](self, cond)(typeA, typeB)
    def NullDynamicRecord[D](implicit typeD: TypeRep[D], di: DummyImplicit): Rep[D] = nestedLoopsJoinOpNullDynamicRecord[A, B, D](self)(typeA, typeB, typeD, di)
    def open(): Rep[Unit] = nestedLoopsJoinOpOpen[A, B](self)(typeA, typeB)
    def next(): Rep[DynamicCompositeRecord[A, B]] = nestedLoopsJoinOpNext[A, B](self)(typeA, typeB)
    def close(): Rep[Unit] = nestedLoopsJoinOpClose[A, B](self)(typeA, typeB)
    def reset(): Rep[Unit] = nestedLoopsJoinOpReset[A, B](self)(typeA, typeB)
    def cnt_=(x$1: Rep[Int]): Rep[Unit] = nestedLoopsJoinOp_Field_Cnt_$eq[A, B](self, x$1)(typeA, typeB)
    def cnt: Rep[Int] = nestedLoopsJoinOp_Field_Cnt[A, B](self)(typeA, typeB)
    def rightTuple_=(x$1: Rep[B]): Rep[Unit] = nestedLoopsJoinOp_Field_RightTuple_$eq[A, B](self, x$1)(typeA, typeB)
    def rightTuple: Rep[B] = nestedLoopsJoinOp_Field_RightTuple[A, B](self)(typeA, typeB)
    def leftTuple_=(x$1: Rep[A]): Rep[Unit] = nestedLoopsJoinOp_Field_LeftTuple_$eq[A, B](self, x$1)(typeA, typeB)
    def leftTuple: Rep[A] = nestedLoopsJoinOp_Field_LeftTuple[A, B](self)(typeA, typeB)
    def joinCond: Rep[((A, B) => Boolean)] = nestedLoopsJoinOp_Field_JoinCond[A, B](self)(typeA, typeB)
    def rightAlias: Rep[String] = nestedLoopsJoinOp_Field_RightAlias[A, B](self)(typeA, typeB)
    def leftAlias: Rep[String] = nestedLoopsJoinOp_Field_LeftAlias[A, B](self)(typeA, typeB)
    def rightParent: Rep[Operator[B]] = nestedLoopsJoinOp_Field_RightParent[A, B](self)(typeA, typeB)
    def leftParent: Rep[Operator[A]] = nestedLoopsJoinOp_Field_LeftParent[A, B](self)(typeA, typeB)
    def NullDynamicRecord: Rep[DynamicCompositeRecord[A, B]] = nestedLoopsJoinOp_Field_NullDynamicRecord[A, B](self)(typeA, typeB)
  }
  object NestedLoopsJoinOp {

  }
  // constructors
  def __newNestedLoopsJoinOp[A <: ch.epfl.data.pardis.shallow.AbstractRecord, B <: ch.epfl.data.pardis.shallow.AbstractRecord](leftParent: Rep[Operator[A]], rightParent: Rep[Operator[B]], leftAlias: Rep[String], rightAlias: Rep[String])(joinCond: Rep[((A, B) => Boolean)])(implicit typeA: TypeRep[A], typeB: TypeRep[B]): Rep[NestedLoopsJoinOp[A, B]] = nestedLoopsJoinOpNew[A, B](leftParent, rightParent, leftAlias, rightAlias, joinCond)(typeA, typeB)
  // case classes
  case class NestedLoopsJoinOpNew[A <: ch.epfl.data.pardis.shallow.AbstractRecord, B <: ch.epfl.data.pardis.shallow.AbstractRecord](leftParent: Rep[Operator[A]], rightParent: Rep[Operator[B]], leftAlias: Rep[String], rightAlias: Rep[String], joinCond: Rep[((A, B) => Boolean)])(implicit val typeA: TypeRep[A], val typeB: TypeRep[B]) extends ConstructorDef[NestedLoopsJoinOp[A, B]](List(typeA, typeB), "NestedLoopsJoinOp", List(List(leftParent, rightParent, leftAlias, rightAlias), List(joinCond))) {
    override def curriedConstructor = (copy[A, B] _).curried
  }

  case class NestedLoopsJoinOpForeach[A <: ch.epfl.data.pardis.shallow.AbstractRecord, B <: ch.epfl.data.pardis.shallow.AbstractRecord](self: Rep[NestedLoopsJoinOp[A, B]], f: Rep[((DynamicCompositeRecord[A, B]) => Unit)])(implicit val typeA: TypeRep[A], val typeB: TypeRep[B]) extends FunctionDef[Unit](Some(self), "foreach", List(List(f))) {
    override def curriedConstructor = (copy[A, B] _).curried
  }

  case class NestedLoopsJoinOpFindFirst[A <: ch.epfl.data.pardis.shallow.AbstractRecord, B <: ch.epfl.data.pardis.shallow.AbstractRecord](self: Rep[NestedLoopsJoinOp[A, B]], cond: Rep[((DynamicCompositeRecord[A, B]) => Boolean)])(implicit val typeA: TypeRep[A], val typeB: TypeRep[B]) extends FunctionDef[DynamicCompositeRecord[A, B]](Some(self), "findFirst", List(List(cond))) {
    override def curriedConstructor = (copy[A, B] _).curried
  }

  case class NestedLoopsJoinOpNullDynamicRecord[A <: ch.epfl.data.pardis.shallow.AbstractRecord, B <: ch.epfl.data.pardis.shallow.AbstractRecord, D](self: Rep[NestedLoopsJoinOp[A, B]])(implicit val typeA: TypeRep[A], val typeB: TypeRep[B], val typeD: TypeRep[D], val di: DummyImplicit) extends FunctionDef[D](Some(self), "NullDynamicRecord", List()) {
    override def curriedConstructor = (copy[A, B, D] _)
  }

  case class NestedLoopsJoinOpOpen[A <: ch.epfl.data.pardis.shallow.AbstractRecord, B <: ch.epfl.data.pardis.shallow.AbstractRecord](self: Rep[NestedLoopsJoinOp[A, B]])(implicit val typeA: TypeRep[A], val typeB: TypeRep[B]) extends FunctionDef[Unit](Some(self), "open", List(List())) {
    override def curriedConstructor = (copy[A, B] _)
  }

  case class NestedLoopsJoinOpNext[A <: ch.epfl.data.pardis.shallow.AbstractRecord, B <: ch.epfl.data.pardis.shallow.AbstractRecord](self: Rep[NestedLoopsJoinOp[A, B]])(implicit val typeA: TypeRep[A], val typeB: TypeRep[B]) extends FunctionDef[DynamicCompositeRecord[A, B]](Some(self), "next", List(List())) {
    override def curriedConstructor = (copy[A, B] _)
  }

  case class NestedLoopsJoinOpClose[A <: ch.epfl.data.pardis.shallow.AbstractRecord, B <: ch.epfl.data.pardis.shallow.AbstractRecord](self: Rep[NestedLoopsJoinOp[A, B]])(implicit val typeA: TypeRep[A], val typeB: TypeRep[B]) extends FunctionDef[Unit](Some(self), "close", List(List())) {
    override def curriedConstructor = (copy[A, B] _)
  }

  case class NestedLoopsJoinOpReset[A <: ch.epfl.data.pardis.shallow.AbstractRecord, B <: ch.epfl.data.pardis.shallow.AbstractRecord](self: Rep[NestedLoopsJoinOp[A, B]])(implicit val typeA: TypeRep[A], val typeB: TypeRep[B]) extends FunctionDef[Unit](Some(self), "reset", List(List())) {
    override def curriedConstructor = (copy[A, B] _)
  }

  case class NestedLoopsJoinOp_Field_Cnt_$eq[A <: ch.epfl.data.pardis.shallow.AbstractRecord, B <: ch.epfl.data.pardis.shallow.AbstractRecord](self: Rep[NestedLoopsJoinOp[A, B]], x$1: Rep[Int])(implicit val typeA: TypeRep[A], val typeB: TypeRep[B]) extends FieldSetter[Int](self, "cnt", x$1) {
    override def curriedConstructor = (copy[A, B] _).curried
  }

  case class NestedLoopsJoinOp_Field_Cnt[A <: ch.epfl.data.pardis.shallow.AbstractRecord, B <: ch.epfl.data.pardis.shallow.AbstractRecord](self: Rep[NestedLoopsJoinOp[A, B]])(implicit val typeA: TypeRep[A], val typeB: TypeRep[B]) extends FieldGetter[Int](self, "cnt") {
    override def curriedConstructor = (copy[A, B] _)
  }

  case class NestedLoopsJoinOp_Field_RightTuple_$eq[A <: ch.epfl.data.pardis.shallow.AbstractRecord, B <: ch.epfl.data.pardis.shallow.AbstractRecord](self: Rep[NestedLoopsJoinOp[A, B]], x$1: Rep[B])(implicit val typeA: TypeRep[A], val typeB: TypeRep[B]) extends FieldSetter[B](self, "rightTuple", x$1) {
    override def curriedConstructor = (copy[A, B] _).curried
  }

  case class NestedLoopsJoinOp_Field_RightTuple[A <: ch.epfl.data.pardis.shallow.AbstractRecord, B <: ch.epfl.data.pardis.shallow.AbstractRecord](self: Rep[NestedLoopsJoinOp[A, B]])(implicit val typeA: TypeRep[A], val typeB: TypeRep[B]) extends FieldGetter[B](self, "rightTuple") {
    override def curriedConstructor = (copy[A, B] _)
  }

  case class NestedLoopsJoinOp_Field_LeftTuple_$eq[A <: ch.epfl.data.pardis.shallow.AbstractRecord, B <: ch.epfl.data.pardis.shallow.AbstractRecord](self: Rep[NestedLoopsJoinOp[A, B]], x$1: Rep[A])(implicit val typeA: TypeRep[A], val typeB: TypeRep[B]) extends FieldSetter[A](self, "leftTuple", x$1) {
    override def curriedConstructor = (copy[A, B] _).curried
  }

  case class NestedLoopsJoinOp_Field_LeftTuple[A <: ch.epfl.data.pardis.shallow.AbstractRecord, B <: ch.epfl.data.pardis.shallow.AbstractRecord](self: Rep[NestedLoopsJoinOp[A, B]])(implicit val typeA: TypeRep[A], val typeB: TypeRep[B]) extends FieldGetter[A](self, "leftTuple") {
    override def curriedConstructor = (copy[A, B] _)
  }

  case class NestedLoopsJoinOp_Field_JoinCond[A <: ch.epfl.data.pardis.shallow.AbstractRecord, B <: ch.epfl.data.pardis.shallow.AbstractRecord](self: Rep[NestedLoopsJoinOp[A, B]])(implicit val typeA: TypeRep[A], val typeB: TypeRep[B]) extends FieldDef[((A, B) => Boolean)](self, "joinCond") {
    override def curriedConstructor = (copy[A, B] _)
    override def isPure = true

  }

  case class NestedLoopsJoinOp_Field_RightAlias[A <: ch.epfl.data.pardis.shallow.AbstractRecord, B <: ch.epfl.data.pardis.shallow.AbstractRecord](self: Rep[NestedLoopsJoinOp[A, B]])(implicit val typeA: TypeRep[A], val typeB: TypeRep[B]) extends FieldDef[String](self, "rightAlias") {
    override def curriedConstructor = (copy[A, B] _)
    override def isPure = true

  }

  case class NestedLoopsJoinOp_Field_LeftAlias[A <: ch.epfl.data.pardis.shallow.AbstractRecord, B <: ch.epfl.data.pardis.shallow.AbstractRecord](self: Rep[NestedLoopsJoinOp[A, B]])(implicit val typeA: TypeRep[A], val typeB: TypeRep[B]) extends FieldDef[String](self, "leftAlias") {
    override def curriedConstructor = (copy[A, B] _)
    override def isPure = true

  }

  case class NestedLoopsJoinOp_Field_RightParent[A <: ch.epfl.data.pardis.shallow.AbstractRecord, B <: ch.epfl.data.pardis.shallow.AbstractRecord](self: Rep[NestedLoopsJoinOp[A, B]])(implicit val typeA: TypeRep[A], val typeB: TypeRep[B]) extends FieldDef[Operator[B]](self, "rightParent") {
    override def curriedConstructor = (copy[A, B] _)
    override def isPure = true

  }

  case class NestedLoopsJoinOp_Field_LeftParent[A <: ch.epfl.data.pardis.shallow.AbstractRecord, B <: ch.epfl.data.pardis.shallow.AbstractRecord](self: Rep[NestedLoopsJoinOp[A, B]])(implicit val typeA: TypeRep[A], val typeB: TypeRep[B]) extends FieldDef[Operator[A]](self, "leftParent") {
    override def curriedConstructor = (copy[A, B] _)
    override def isPure = true

  }

  case class NestedLoopsJoinOp_Field_NullDynamicRecord[A <: ch.epfl.data.pardis.shallow.AbstractRecord, B <: ch.epfl.data.pardis.shallow.AbstractRecord](self: Rep[NestedLoopsJoinOp[A, B]])(implicit val typeA: TypeRep[A], val typeB: TypeRep[B]) extends FieldDef[DynamicCompositeRecord[A, B]](self, "NullDynamicRecord") {
    override def curriedConstructor = (copy[A, B] _)
    override def isPure = true

  }

  // method definitions
  def nestedLoopsJoinOpNew[A <: ch.epfl.data.pardis.shallow.AbstractRecord, B <: ch.epfl.data.pardis.shallow.AbstractRecord](leftParent: Rep[Operator[A]], rightParent: Rep[Operator[B]], leftAlias: Rep[String], rightAlias: Rep[String], joinCond: Rep[((A, B) => Boolean)])(implicit typeA: TypeRep[A], typeB: TypeRep[B]): Rep[NestedLoopsJoinOp[A, B]] = NestedLoopsJoinOpNew[A, B](leftParent, rightParent, leftAlias, rightAlias, joinCond)
  def nestedLoopsJoinOpForeach[A <: ch.epfl.data.pardis.shallow.AbstractRecord, B <: ch.epfl.data.pardis.shallow.AbstractRecord](self: Rep[NestedLoopsJoinOp[A, B]], f: Rep[((DynamicCompositeRecord[A, B]) => Unit)])(implicit typeA: TypeRep[A], typeB: TypeRep[B]): Rep[Unit] = NestedLoopsJoinOpForeach[A, B](self, f)
  def nestedLoopsJoinOpFindFirst[A <: ch.epfl.data.pardis.shallow.AbstractRecord, B <: ch.epfl.data.pardis.shallow.AbstractRecord](self: Rep[NestedLoopsJoinOp[A, B]], cond: Rep[((DynamicCompositeRecord[A, B]) => Boolean)])(implicit typeA: TypeRep[A], typeB: TypeRep[B]): Rep[DynamicCompositeRecord[A, B]] = NestedLoopsJoinOpFindFirst[A, B](self, cond)
  def nestedLoopsJoinOpNullDynamicRecord[A <: ch.epfl.data.pardis.shallow.AbstractRecord, B <: ch.epfl.data.pardis.shallow.AbstractRecord, D](self: Rep[NestedLoopsJoinOp[A, B]])(implicit typeA: TypeRep[A], typeB: TypeRep[B], typeD: TypeRep[D], di: DummyImplicit): Rep[D] = NestedLoopsJoinOpNullDynamicRecord[A, B, D](self)
  def nestedLoopsJoinOpOpen[A <: ch.epfl.data.pardis.shallow.AbstractRecord, B <: ch.epfl.data.pardis.shallow.AbstractRecord](self: Rep[NestedLoopsJoinOp[A, B]])(implicit typeA: TypeRep[A], typeB: TypeRep[B]): Rep[Unit] = NestedLoopsJoinOpOpen[A, B](self)
  def nestedLoopsJoinOpNext[A <: ch.epfl.data.pardis.shallow.AbstractRecord, B <: ch.epfl.data.pardis.shallow.AbstractRecord](self: Rep[NestedLoopsJoinOp[A, B]])(implicit typeA: TypeRep[A], typeB: TypeRep[B]): Rep[DynamicCompositeRecord[A, B]] = NestedLoopsJoinOpNext[A, B](self)
  def nestedLoopsJoinOpClose[A <: ch.epfl.data.pardis.shallow.AbstractRecord, B <: ch.epfl.data.pardis.shallow.AbstractRecord](self: Rep[NestedLoopsJoinOp[A, B]])(implicit typeA: TypeRep[A], typeB: TypeRep[B]): Rep[Unit] = NestedLoopsJoinOpClose[A, B](self)
  def nestedLoopsJoinOpReset[A <: ch.epfl.data.pardis.shallow.AbstractRecord, B <: ch.epfl.data.pardis.shallow.AbstractRecord](self: Rep[NestedLoopsJoinOp[A, B]])(implicit typeA: TypeRep[A], typeB: TypeRep[B]): Rep[Unit] = NestedLoopsJoinOpReset[A, B](self)
  def nestedLoopsJoinOp_Field_Cnt_$eq[A <: ch.epfl.data.pardis.shallow.AbstractRecord, B <: ch.epfl.data.pardis.shallow.AbstractRecord](self: Rep[NestedLoopsJoinOp[A, B]], x$1: Rep[Int])(implicit typeA: TypeRep[A], typeB: TypeRep[B]): Rep[Unit] = NestedLoopsJoinOp_Field_Cnt_$eq[A, B](self, x$1)
  def nestedLoopsJoinOp_Field_Cnt[A <: ch.epfl.data.pardis.shallow.AbstractRecord, B <: ch.epfl.data.pardis.shallow.AbstractRecord](self: Rep[NestedLoopsJoinOp[A, B]])(implicit typeA: TypeRep[A], typeB: TypeRep[B]): Rep[Int] = NestedLoopsJoinOp_Field_Cnt[A, B](self)
  def nestedLoopsJoinOp_Field_RightTuple_$eq[A <: ch.epfl.data.pardis.shallow.AbstractRecord, B <: ch.epfl.data.pardis.shallow.AbstractRecord](self: Rep[NestedLoopsJoinOp[A, B]], x$1: Rep[B])(implicit typeA: TypeRep[A], typeB: TypeRep[B]): Rep[Unit] = NestedLoopsJoinOp_Field_RightTuple_$eq[A, B](self, x$1)
  def nestedLoopsJoinOp_Field_RightTuple[A <: ch.epfl.data.pardis.shallow.AbstractRecord, B <: ch.epfl.data.pardis.shallow.AbstractRecord](self: Rep[NestedLoopsJoinOp[A, B]])(implicit typeA: TypeRep[A], typeB: TypeRep[B]): Rep[B] = NestedLoopsJoinOp_Field_RightTuple[A, B](self)
  def nestedLoopsJoinOp_Field_LeftTuple_$eq[A <: ch.epfl.data.pardis.shallow.AbstractRecord, B <: ch.epfl.data.pardis.shallow.AbstractRecord](self: Rep[NestedLoopsJoinOp[A, B]], x$1: Rep[A])(implicit typeA: TypeRep[A], typeB: TypeRep[B]): Rep[Unit] = NestedLoopsJoinOp_Field_LeftTuple_$eq[A, B](self, x$1)
  def nestedLoopsJoinOp_Field_LeftTuple[A <: ch.epfl.data.pardis.shallow.AbstractRecord, B <: ch.epfl.data.pardis.shallow.AbstractRecord](self: Rep[NestedLoopsJoinOp[A, B]])(implicit typeA: TypeRep[A], typeB: TypeRep[B]): Rep[A] = NestedLoopsJoinOp_Field_LeftTuple[A, B](self)
  def nestedLoopsJoinOp_Field_JoinCond[A <: ch.epfl.data.pardis.shallow.AbstractRecord, B <: ch.epfl.data.pardis.shallow.AbstractRecord](self: Rep[NestedLoopsJoinOp[A, B]])(implicit typeA: TypeRep[A], typeB: TypeRep[B]): Rep[((A, B) => Boolean)] = NestedLoopsJoinOp_Field_JoinCond[A, B](self)
  def nestedLoopsJoinOp_Field_RightAlias[A <: ch.epfl.data.pardis.shallow.AbstractRecord, B <: ch.epfl.data.pardis.shallow.AbstractRecord](self: Rep[NestedLoopsJoinOp[A, B]])(implicit typeA: TypeRep[A], typeB: TypeRep[B]): Rep[String] = NestedLoopsJoinOp_Field_RightAlias[A, B](self)
  def nestedLoopsJoinOp_Field_LeftAlias[A <: ch.epfl.data.pardis.shallow.AbstractRecord, B <: ch.epfl.data.pardis.shallow.AbstractRecord](self: Rep[NestedLoopsJoinOp[A, B]])(implicit typeA: TypeRep[A], typeB: TypeRep[B]): Rep[String] = NestedLoopsJoinOp_Field_LeftAlias[A, B](self)
  def nestedLoopsJoinOp_Field_RightParent[A <: ch.epfl.data.pardis.shallow.AbstractRecord, B <: ch.epfl.data.pardis.shallow.AbstractRecord](self: Rep[NestedLoopsJoinOp[A, B]])(implicit typeA: TypeRep[A], typeB: TypeRep[B]): Rep[Operator[B]] = NestedLoopsJoinOp_Field_RightParent[A, B](self)
  def nestedLoopsJoinOp_Field_LeftParent[A <: ch.epfl.data.pardis.shallow.AbstractRecord, B <: ch.epfl.data.pardis.shallow.AbstractRecord](self: Rep[NestedLoopsJoinOp[A, B]])(implicit typeA: TypeRep[A], typeB: TypeRep[B]): Rep[Operator[A]] = NestedLoopsJoinOp_Field_LeftParent[A, B](self)
  def nestedLoopsJoinOp_Field_NullDynamicRecord[A <: ch.epfl.data.pardis.shallow.AbstractRecord, B <: ch.epfl.data.pardis.shallow.AbstractRecord](self: Rep[NestedLoopsJoinOp[A, B]])(implicit typeA: TypeRep[A], typeB: TypeRep[B]): Rep[DynamicCompositeRecord[A, B]] = NestedLoopsJoinOp_Field_NullDynamicRecord[A, B](self)
  type NestedLoopsJoinOp[A <: ch.epfl.data.pardis.shallow.AbstractRecord, B <: ch.epfl.data.pardis.shallow.AbstractRecord] = ch.epfl.data.legobase.queryengine.volcano.NestedLoopsJoinOp[A, B]
  case class NestedLoopsJoinOpType[A <: ch.epfl.data.pardis.shallow.AbstractRecord, B <: ch.epfl.data.pardis.shallow.AbstractRecord](typeA: TypeRep[A], typeB: TypeRep[B]) extends TypeRep[NestedLoopsJoinOp[A, B]] {
    def rebuild(newArguments: TypeRep[_]*): TypeRep[_] = NestedLoopsJoinOpType(newArguments(0).asInstanceOf[TypeRep[_ <: ch.epfl.data.pardis.shallow.AbstractRecord]], newArguments(1).asInstanceOf[TypeRep[_ <: ch.epfl.data.pardis.shallow.AbstractRecord]])
    private implicit val tagA = typeA.typeTag
    private implicit val tagB = typeB.typeTag
    val name = s"NestedLoopsJoinOp[${typeA.name}, ${typeB.name}]"
    val typeArguments = List(typeA, typeB)

    val typeTag = scala.reflect.runtime.universe.typeTag[NestedLoopsJoinOp[A, B]]
  }
  implicit def typeNestedLoopsJoinOp[A <: ch.epfl.data.pardis.shallow.AbstractRecord: TypeRep, B <: ch.epfl.data.pardis.shallow.AbstractRecord: TypeRep] = NestedLoopsJoinOpType(implicitly[TypeRep[A]], implicitly[TypeRep[B]])
}
trait NestedLoopsJoinOpImplicits { this: NestedLoopsJoinOpComponent =>
  // Add implicit conversions here!
}
trait NestedLoopsJoinOpImplementations { self: DeepDSL =>
  override def nestedLoopsJoinOpOpen[A <: ch.epfl.data.pardis.shallow.AbstractRecord, B <: ch.epfl.data.pardis.shallow.AbstractRecord](self: Rep[NestedLoopsJoinOp[A, B]])(implicit typeA: TypeRep[A], typeB: TypeRep[B]): Rep[Unit] = {
    {
      self.rightParent.open();
      self.leftParent.open();
      self.leftTuple_$eq(self.leftParent.next())
    }
  }
  override def nestedLoopsJoinOpNext[A <: ch.epfl.data.pardis.shallow.AbstractRecord, B <: ch.epfl.data.pardis.shallow.AbstractRecord](self: Rep[NestedLoopsJoinOp[A, B]])(implicit typeA: TypeRep[A], typeB: TypeRep[B]): Rep[DynamicCompositeRecord[A, B]] = {
    {
      var exit: this.Var[Boolean] = __newVar(unit(false));
      __whileDo(infix_$eq$eq(readVar(exit), unit(false)).$amp$amp(infix_$bang$eq(self.leftTuple, self.NullDynamicRecord[A])), {
        self.rightTuple_$eq(self.rightParent.findFirst(__lambda(((t: this.Rep[B]) => __app(self.joinCond).apply(self.leftTuple, t)))));
        __ifThenElse(infix_$eq$eq(self.rightTuple, self.NullDynamicRecord[B]), {
          self.rightParent.reset();
          self.leftTuple_$eq(self.leftParent.next())
        }, __assign(exit, unit(true)))
      });
      __ifThenElse(infix_$bang$eq(self.leftTuple, self.NullDynamicRecord[A]), RecordOps[A](self.leftTuple).concatenateDynamic[B](self.rightTuple, self.leftAlias, self.rightAlias), self.NullDynamicRecord)
    }
  }
  override def nestedLoopsJoinOpClose[A <: ch.epfl.data.pardis.shallow.AbstractRecord, B <: ch.epfl.data.pardis.shallow.AbstractRecord](self: Rep[NestedLoopsJoinOp[A, B]])(implicit typeA: TypeRep[A], typeB: TypeRep[B]): Rep[Unit] = {
    unit(())
  }
  override def nestedLoopsJoinOpReset[A <: ch.epfl.data.pardis.shallow.AbstractRecord, B <: ch.epfl.data.pardis.shallow.AbstractRecord](self: Rep[NestedLoopsJoinOp[A, B]])(implicit typeA: TypeRep[A], typeB: TypeRep[B]): Rep[Unit] = {
    {
      self.rightParent.reset();
      self.leftParent.reset();
      self.leftTuple_$eq(self.NullDynamicRecord[A])
    }
  }
}
trait NestedLoopsJoinOpComponent extends NestedLoopsJoinOpOps with NestedLoopsJoinOpImplicits { self: OperatorsComponent => }
trait OperatorsComponent extends OperatorComponent with ScanOpComponent with SelectOpComponent with AggOpComponent with SortOpComponent with MapOpComponent with PrintOpComponent with HashJoinOpComponent with WindowOpComponent with LeftHashSemiJoinOpComponent with NestedLoopsJoinOpComponent with AGGRecordComponent with WindowRecordComponent with CharacterComponent with DoubleComponent with IntComponent with LongComponent with ArrayComponent with LINEITEMRecordComponent with K2DBScannerComponent with IntegerComponent with BooleanComponent with HashMapComponent with SetComponent with TreeSetComponent with DefaultEntryComponent with ArrayBufferComponent with ManualLiftedLegoBase { self: DeepDSL => }