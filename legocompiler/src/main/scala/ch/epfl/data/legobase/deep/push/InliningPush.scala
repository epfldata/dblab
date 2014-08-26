package ch.epfl.data
package legobase
package deep
package push

import scala.language.implicitConversions
import pardis.ir._
import pardis.ir.pardisTypeImplicits._

trait InliningPush extends DeepDSL with pardis.ir.InlineFunctions with QueriesImplementations with OperatorImplementations with ScanOpImplementations with SelectOpImplementations with AggOpImplementations with SortOpImplementations with MapOpImplementations with PrintOpImplementations with WindowOpImplementations with HashJoinOpImplementations with LeftHashSemiJoinOpImplementations with NestedLoopsJoinOpImplementations with SubquerySingleResultImplementations with ViewOpImplementations with HashJoinAntiImplementations with LeftOuterJoinOpImplementations { this: InliningLegoBase =>
  override def findSymbol[T: TypeRep](d: Def[T]): Option[Sym[T]] =
    scopeDefs.find(x => x.rhs == d && x.rhs.tp == d.tp).map(x => x.sym.asInstanceOf[Sym[T]])
  override def infix_asInstanceOf[T: TypeRep](exp: Rep[Any]): Rep[T] = {
    // System.out.println(s"asInstanceOf for $exp from ${exp.tp} to ${typeRep[T]}")
    val res = exp match {
      // case _ if exp.tp.isRecord      => exp.asInstanceOf[Rep[T]]
      case Def(PardisCast(exp2))     => infix_asInstanceOf[T](exp2)
      case _ if exp.tp == typeRep[T] => exp.asInstanceOf[Rep[T]]
      case _                         => super.infix_asInstanceOf[T](exp)
    }
    // System.out.println(s"res $res")
    res
  }

  override def __ifThenElse[T: TypeRep](cond: Rep[Boolean], thenp: => Rep[T], elsep: => Rep[T]): Rep[T] = cond match {
    case Constant(true)  => thenp
    case Constant(false) => elsep
    case _               => super.__ifThenElse(cond, thenp, elsep)
  }

  override def infix_==[A: TypeRep, B: TypeRep](a: Rep[A], b: Rep[B]): Rep[Boolean] = (a, b) match {
    case (Constant(v1), Constant(v2)) => unit(v1 == v2)
    case _                            => super.infix_==(a, b)
  }

  override def operatorOpen[A](self: Rep[Operator[A]])(implicit typeA: TypeRep[A]): Rep[Unit] = self match {
    case Def(x: PrintOpNew[_]) => self.asInstanceOf[Rep[PrintOp[A]]].open
    case Def(x: SortOpNew[_])  => self.asInstanceOf[Rep[SortOp[A]]].open
    case Def(x: MapOpNew[_])   => self.asInstanceOf[Rep[MapOp[A]]].open
    case Def(x: AggOpNew[_, _]) => {
      type X = Any
      type Y = Any
      aggOpOpen(self.asInstanceOf[Rep[AggOp[X, Y]]])(x.typeA.asInstanceOf[TypeRep[X]], x.typeB.asInstanceOf[TypeRep[Y]])
    }
    case Def(x: ScanOpNew[_]) => {
      type X = pardis.shallow.Record
      scanOpOpen(self.asInstanceOf[Rep[ScanOp[X]]])(x.typeA.asInstanceOf[TypeRep[X]])
    }
    case Def(x: SelectOpNew[_]) => self.asInstanceOf[Rep[SelectOp[A]]].open
    case Def(x: WindowOpNew[_, _, _]) => {
      type X = Any
      type Y = Any
      type Z = Any
      windowOpOpen(self.asInstanceOf[Rep[WindowOp[X, Y, Z]]])(x.typeA.asInstanceOf[TypeRep[X]], x.typeB.asInstanceOf[TypeRep[Y]], x.typeC.asInstanceOf[TypeRep[Z]])
    }
    case Def(x: HashJoinOpNew1[_, _, _]) => {
      type X = pardis.shallow.Record
      type Y = pardis.shallow.Record
      type Z = Any
      hashJoinOpOpen(self.asInstanceOf[Rep[HashJoinOp[X, Y, Z]]])(x.typeA.asInstanceOf[TypeRep[X]], x.typeB.asInstanceOf[TypeRep[Y]], x.typeC.asInstanceOf[TypeRep[Z]])
    }
    case Def(x: LeftHashSemiJoinOpNew[_, _, _]) => {
      type X = Any
      type Y = Any
      type Z = Any
      leftHashSemiJoinOpOpen(self.asInstanceOf[Rep[LeftHashSemiJoinOp[X, Y, Z]]])(x.typeA.asInstanceOf[TypeRep[X]], x.typeB.asInstanceOf[TypeRep[Y]], x.typeC.asInstanceOf[TypeRep[Z]])
    }
    case Def(x: NestedLoopsJoinOpNew[_, _]) => {
      type X = pardis.shallow.Record
      type Y = pardis.shallow.Record
      nestedLoopsJoinOpOpen(self.asInstanceOf[Rep[NestedLoopsJoinOp[X, Y]]])(x.typeA.asInstanceOf[TypeRep[X]], x.typeB.asInstanceOf[TypeRep[Y]])
    }
    case Def(x: ViewOpNew[_]) => {
      type X = Any
      viewOpOpen(self.asInstanceOf[Rep[ViewOp[X]]])(x.typeA.asInstanceOf[TypeRep[X]])
    }
    case Def(x: HashJoinAntiNew[_, _, _]) => {
      type X = Any
      type Y = Any
      type Z = Any
      hashJoinAntiOpen(self.asInstanceOf[Rep[HashJoinAnti[X, Y, Z]]])(x.typeA.asInstanceOf[TypeRep[X]], x.typeB.asInstanceOf[TypeRep[Y]], x.typeC.asInstanceOf[TypeRep[Z]])
    }
    case Def(x: LeftOuterJoinOpNew[_, _, _]) => {
      type X = pardis.shallow.Record
      type Y = pardis.shallow.Record
      type Z = Any
      leftOuterJoinOpOpen(self.asInstanceOf[Rep[LeftOuterJoinOp[X, Y, Z]]])(x.typeA.asInstanceOf[TypeRep[X]], x.typeB.asInstanceOf[TypeRep[Y]], x.typeC.asInstanceOf[TypeRep[Z]], x.evidence$1.asInstanceOf[Manifest[Y]])
    }
    case _ => super.operatorOpen(self)
  }

  // is needed only for query 7
  override def operatorReset[A](self: Rep[Operator[A]])(implicit typeA: TypeRep[A]): Rep[Unit] = self match {
    case Def(x: PrintOpNew[_]) => self.asInstanceOf[Rep[PrintOp[A]]].reset
    case Def(x: SortOpNew[_])  => self.asInstanceOf[Rep[SortOp[A]]].reset
    case Def(x: MapOpNew[_])   => self.asInstanceOf[Rep[MapOp[A]]].reset
    case Def(x: AggOpNew[_, _]) => {
      type X = Any
      type Y = Any
      aggOpReset(self.asInstanceOf[Rep[AggOp[X, Y]]])(x.typeA.asInstanceOf[TypeRep[X]], x.typeB.asInstanceOf[TypeRep[Y]])
    }
    case Def(x: ScanOpNew[_])   => self.asInstanceOf[Rep[ScanOp[A]]].reset
    case Def(x: SelectOpNew[_]) => self.asInstanceOf[Rep[SelectOp[A]]].reset
    case Def(x: WindowOpNew[_, _, _]) => {
      type X = Any
      type Y = Any
      type Z = Any
      windowOpReset(self.asInstanceOf[Rep[WindowOp[X, Y, Z]]])(x.typeA.asInstanceOf[TypeRep[X]], x.typeB.asInstanceOf[TypeRep[Y]], x.typeC.asInstanceOf[TypeRep[Z]])
    }
    case Def(x: HashJoinOpNew1[_, _, _]) => {
      type X = pardis.shallow.Record
      type Y = pardis.shallow.Record
      type Z = Any
      hashJoinOpReset(self.asInstanceOf[Rep[HashJoinOp[X, Y, Z]]])(x.typeA.asInstanceOf[TypeRep[X]], x.typeB.asInstanceOf[TypeRep[Y]], x.typeC.asInstanceOf[TypeRep[Z]])
    }
    case Def(x: LeftHashSemiJoinOpNew[_, _, _]) => {
      type X = Any
      type Y = Any
      type Z = Any
      leftHashSemiJoinOpReset(self.asInstanceOf[Rep[LeftHashSemiJoinOp[X, Y, Z]]])(x.typeA.asInstanceOf[TypeRep[X]], x.typeB.asInstanceOf[TypeRep[Y]], x.typeC.asInstanceOf[TypeRep[Z]])
    }
    case Def(x: NestedLoopsJoinOpNew[_, _]) => {
      type X = pardis.shallow.Record
      type Y = pardis.shallow.Record
      nestedLoopsJoinOpReset(self.asInstanceOf[Rep[NestedLoopsJoinOp[X, Y]]])(x.typeA.asInstanceOf[TypeRep[X]], x.typeB.asInstanceOf[TypeRep[Y]])
    }
    case _ => super.operatorReset(self)
  }

  override def operatorNext[A](self: Rep[Operator[A]])(implicit typeA: TypeRep[A]) = self match {
    case Def(x: PrintOpNew[_]) => self.asInstanceOf[Rep[PrintOp[A]]].next
    case Def(x: SortOpNew[_])  => self.asInstanceOf[Rep[SortOp[A]]].next
    case Def(x: MapOpNew[_])   => self.asInstanceOf[Rep[MapOp[A]]].next
    case Def(x: AggOpNew[_, _]) => {
      type X = Any
      type Y = Any
      aggOpNext(self.asInstanceOf[Rep[AggOp[X, Y]]])(x.typeA.asInstanceOf[TypeRep[X]], x.typeB.asInstanceOf[TypeRep[Y]])
    }
    case Def(x: ScanOpNew[_]) => {
      type X = Any
      scanOpNext(self.asInstanceOf[Rep[ScanOp[X]]])(x.typeA.asInstanceOf[TypeRep[X]])
    }
    case Def(x: SelectOpNew[_]) => self.asInstanceOf[Rep[SelectOp[A]]].next
    case Def(x: WindowOpNew[_, _, _]) => {
      type X = Any
      type Y = Any
      type Z = Any
      windowOpNext(self.asInstanceOf[Rep[WindowOp[X, Y, Z]]])(x.typeA.asInstanceOf[TypeRep[X]], x.typeB.asInstanceOf[TypeRep[Y]], x.typeC.asInstanceOf[TypeRep[Z]])
    }
    case Def(x: HashJoinOpNew1[_, _, _]) => {
      type X = pardis.shallow.Record
      type Y = pardis.shallow.Record
      type Z = Any
      hashJoinOpNext(self.asInstanceOf[Rep[HashJoinOp[X, Y, Z]]])(x.typeA.asInstanceOf[TypeRep[X]], x.typeB.asInstanceOf[TypeRep[Y]], x.typeC.asInstanceOf[TypeRep[Z]])
    }
    case Def(x: LeftHashSemiJoinOpNew[_, _, _]) => {
      type X = Any
      type Y = Any
      type Z = Any
      leftHashSemiJoinOpNext(self.asInstanceOf[Rep[LeftHashSemiJoinOp[X, Y, Z]]])(x.typeA.asInstanceOf[TypeRep[X]], x.typeB.asInstanceOf[TypeRep[Y]], x.typeC.asInstanceOf[TypeRep[Z]])
    }
    case Def(x: NestedLoopsJoinOpNew[_, _]) => {
      type X = pardis.shallow.Record
      type Y = pardis.shallow.Record
      nestedLoopsJoinOpNext(self.asInstanceOf[Rep[NestedLoopsJoinOp[X, Y]]])(x.typeA.asInstanceOf[TypeRep[X]], x.typeB.asInstanceOf[TypeRep[Y]])
    }
    case Def(x: ViewOpNew[_]) => {
      type X = Any
      viewOpNext(self.asInstanceOf[Rep[ViewOp[X]]])(x.typeA.asInstanceOf[TypeRep[X]])
    }
    case Def(x: HashJoinAntiNew[_, _, _]) => {
      type X = Any
      type Y = Any
      type Z = Any
      hashJoinAntiNext(self.asInstanceOf[Rep[HashJoinAnti[X, Y, Z]]])(x.typeA.asInstanceOf[TypeRep[X]], x.typeB.asInstanceOf[TypeRep[Y]], x.typeC.asInstanceOf[TypeRep[Z]])
    }
    case Def(x: LeftOuterJoinOpNew[_, _, _]) => {
      type X = pardis.shallow.Record
      type Y = pardis.shallow.Record
      type Z = Any
      leftOuterJoinOpNext(self.asInstanceOf[Rep[LeftOuterJoinOp[X, Y, Z]]])(x.typeA.asInstanceOf[TypeRep[X]], x.typeB.asInstanceOf[TypeRep[Y]], x.typeC.asInstanceOf[TypeRep[Z]], x.evidence$1.asInstanceOf[Manifest[Y]])
    }
    case _ => super.operatorNext(self)
  }

  override def operatorConsume[A](self: Rep[Operator[A]], tuple: Rep[Record])(implicit typeA: TypeRep[A]): Rep[Unit] = self match {
    case Def(x: PrintOpNew[_]) => {
      type X = Any
      printOpConsume(self.asInstanceOf[Rep[PrintOp[X]]], tuple)(x.typeA.asInstanceOf[TypeRep[X]])
    }
    case Def(x: SortOpNew[_]) => {
      type X = Any
      sortOpConsume(self.asInstanceOf[Rep[SortOp[X]]], tuple)(x.typeA.asInstanceOf[TypeRep[X]])
    }
    case Def(x: MapOpNew[_]) => {
      type X = Any
      mapOpConsume(self.asInstanceOf[Rep[MapOp[X]]], tuple)(x.typeA.asInstanceOf[TypeRep[X]])
    }
    case Def(x: AggOpNew[_, _]) => {
      type X = Any
      type Y = Any
      aggOpConsume(self.asInstanceOf[Rep[AggOp[X, Y]]], tuple)(x.typeA.asInstanceOf[TypeRep[X]], x.typeB.asInstanceOf[TypeRep[Y]])
    }
    case Def(x: ScanOpNew[_]) => {
      type X = Any
      scanOpConsume(self.asInstanceOf[Rep[ScanOp[X]]], tuple)(x.typeA.asInstanceOf[TypeRep[X]])
    }
    case Def(x: SelectOpNew[_]) => {
      type X = Any
      selectOpConsume(self.asInstanceOf[Rep[SelectOp[X]]], tuple)(x.typeA.asInstanceOf[TypeRep[X]])
    }
    case Def(x: WindowOpNew[_, _, _]) => {
      type X = Any
      type Y = Any
      type Z = Any
      windowOpConsume(self.asInstanceOf[Rep[WindowOp[X, Y, Z]]], tuple)(x.typeA.asInstanceOf[TypeRep[X]], x.typeB.asInstanceOf[TypeRep[Y]], x.typeC.asInstanceOf[TypeRep[Z]])
    }
    case Def(x: HashJoinOpNew1[_, _, _]) => {
      type X = pardis.shallow.Record
      type Y = pardis.shallow.Record
      type Z = Any
      hashJoinOpConsume(self.asInstanceOf[Rep[HashJoinOp[X, Y, Z]]], tuple)(x.typeA.asInstanceOf[TypeRep[X]], x.typeB.asInstanceOf[TypeRep[Y]], x.typeC.asInstanceOf[TypeRep[Z]])
    }
    case Def(x: LeftHashSemiJoinOpNew[_, _, _]) => {
      type X = Any
      type Y = Any
      type Z = Any
      leftHashSemiJoinOpConsume(self.asInstanceOf[Rep[LeftHashSemiJoinOp[X, Y, Z]]], tuple)(x.typeA.asInstanceOf[TypeRep[X]], x.typeB.asInstanceOf[TypeRep[Y]], x.typeC.asInstanceOf[TypeRep[Z]])
    }
    case Def(x: NestedLoopsJoinOpNew[_, _]) => {
      type X = pardis.shallow.Record
      type Y = pardis.shallow.Record
      nestedLoopsJoinOpConsume(self.asInstanceOf[Rep[NestedLoopsJoinOp[X, Y]]], tuple)(x.typeA.asInstanceOf[TypeRep[X]], x.typeB.asInstanceOf[TypeRep[Y]])
    }
    case Def(x: ViewOpNew[_]) => {
      type X = Any
      viewOpConsume(self.asInstanceOf[Rep[ViewOp[X]]], tuple)(x.typeA.asInstanceOf[TypeRep[X]])
    }
    case Def(x: SubquerySingleResultNew[_]) => {
      type X = Any
      subquerySingleResultConsume(self.asInstanceOf[Rep[SubquerySingleResult[X]]], tuple)(x.typeA.asInstanceOf[TypeRep[X]])
    }
    case Def(x: HashJoinAntiNew[_, _, _]) => {
      type X = Any
      type Y = Any
      type Z = Any
      hashJoinAntiConsume(self.asInstanceOf[Rep[HashJoinAnti[X, Y, Z]]], tuple)(x.typeA.asInstanceOf[TypeRep[X]], x.typeB.asInstanceOf[TypeRep[Y]], x.typeC.asInstanceOf[TypeRep[Z]])
    }
    case Def(x: LeftOuterJoinOpNew[_, _, _]) => {
      type X = pardis.shallow.Record
      type Y = pardis.shallow.Record
      type Z = Any
      leftOuterJoinOpConsume(self.asInstanceOf[Rep[LeftOuterJoinOp[X, Y, Z]]], tuple)(x.typeA.asInstanceOf[TypeRep[X]], x.typeB.asInstanceOf[TypeRep[Y]], x.typeC.asInstanceOf[TypeRep[Z]], x.evidence$1.asInstanceOf[Manifest[Y]])
    }
    case _ => super.operatorConsume(self, tuple)
  }

  val mutableValues = collection.mutable.Map[(Rep[Any], String), Any]()

  override def operator_Field_Child_$eq[A](self: Rep[Operator[A]], x$1: Rep[Operator[Any]])(implicit typeA: TypeRep[A]): Rep[Unit] = {
    mutableValues(self -> "child") = x$1
    unit(())
  }

  override def operator_Field_Child[A](self: Rep[Operator[A]])(implicit typeA: TypeRep[A]): Rep[Operator[Any]] = {
    mutableValues(self -> "child").asInstanceOf[Rep[Operator[Any]]]
  }

  override def scanOp_Field_Child[A](self: Rep[ScanOp[A]])(implicit typeA: TypeRep[A]): Rep[Operator[Any]] = operator_Field_Child(self)
  override def printOp_Field_Child[A](self: Rep[PrintOp[A]])(implicit typeA: TypeRep[A]): Rep[Operator[Any]] = operator_Field_Child(self)
  override def selectOp_Field_Child[A](self: Rep[SelectOp[A]])(implicit typeA: TypeRep[A]): Rep[Operator[Any]] = operator_Field_Child(self)
  override def aggOp_Field_Child[A, B](self: Rep[AggOp[A, B]])(implicit typeA: TypeRep[A], typeB: TypeRep[B]): Rep[Operator[Any]] = operator_Field_Child(self)
  override def mapOp_Field_Child[A](self: Rep[MapOp[A]])(implicit typeA: TypeRep[A]): Rep[Operator[Any]] = operator_Field_Child(self)
  override def sortOp_Field_Child[A](self: Rep[SortOp[A]])(implicit typeA: TypeRep[A]): Rep[Operator[Any]] = operator_Field_Child(self)
  override def hashJoinOp_Field_Child[A <: ch.epfl.data.pardis.shallow.Record, B <: ch.epfl.data.pardis.shallow.Record, C](self: Rep[HashJoinOp[A, B, C]])(implicit typeA: TypeRep[A], typeB: TypeRep[B], typeC: TypeRep[C]): Rep[Operator[Any]] = operator_Field_Child(self)
  override def windowOp_Field_Child[A, B, C](self: Rep[WindowOp[A, B, C]])(implicit typeA: TypeRep[A], typeB: TypeRep[B], typeC: TypeRep[C]): Rep[Operator[Any]] = operator_Field_Child(self)
  override def leftHashSemiJoinOp_Field_Child[A, B, C](self: Rep[LeftHashSemiJoinOp[A, B, C]])(implicit typeA: TypeRep[A], typeB: TypeRep[B], typeC: TypeRep[C]): Rep[Operator[Any]] = operator_Field_Child(self)
  override def nestedLoopsJoinOp_Field_Child[A <: ch.epfl.data.pardis.shallow.Record, B <: ch.epfl.data.pardis.shallow.Record](self: Rep[NestedLoopsJoinOp[A, B]])(implicit typeA: TypeRep[A], typeB: TypeRep[B]): Rep[Operator[Any]] = operator_Field_Child(self)
  override def subquerySingleResult_Field_Child[A](self: Rep[SubquerySingleResult[A]])(implicit typeA: TypeRep[A]): Rep[Operator[Any]] = operator_Field_Child(self)
  override def viewOp_Field_Child[A](self: Rep[ViewOp[A]])(implicit typeA: TypeRep[A]): Rep[Operator[Any]] = operator_Field_Child(self)
  override def leftOuterJoinOp_Field_Child[A <: ch.epfl.data.pardis.shallow.Record, B <: ch.epfl.data.pardis.shallow.Record, C](self: Rep[LeftOuterJoinOp[A, B, C]])(implicit typeA: TypeRep[A], typeB: TypeRep[B], typeC: TypeRep[C]): Rep[Operator[Any]] = operator_Field_Child(self)
  override def hashJoinAnti_Field_Child[A, B, C](self: Rep[HashJoinAnti[A, B, C]])(implicit typeA: TypeRep[A], typeB: TypeRep[B], typeC: TypeRep[C]): Rep[Operator[Any]] = operator_Field_Child(self)

  override def hashJoinOp_Field_Mode_$eq[A <: ch.epfl.data.pardis.shallow.Record, B <: ch.epfl.data.pardis.shallow.Record, C](self: Rep[HashJoinOp[A, B, C]], x$1: Rep[Int])(implicit typeA: TypeRep[A], typeB: TypeRep[B], typeC: TypeRep[C]): Rep[Unit] = {
    mutableValues(self -> "mode") = x$1
    unit(())
  }
  override def hashJoinOp_Field_Mode[A <: ch.epfl.data.pardis.shallow.Record, B <: ch.epfl.data.pardis.shallow.Record, C](self: Rep[HashJoinOp[A, B, C]])(implicit typeA: TypeRep[A], typeB: TypeRep[B], typeC: TypeRep[C]): Rep[Int] = {
    mutableValues.get(self -> "mode").getOrElse(unit(0)).asInstanceOf[Rep[Int]]
  }
  override def leftHashSemiJoinOp_Field_Mode_$eq[A, B, C](self: Rep[LeftHashSemiJoinOp[A, B, C]], x$1: Rep[Int])(implicit typeA: TypeRep[A], typeB: TypeRep[B], typeC: TypeRep[C]): Rep[Unit] = {
    mutableValues(self -> "mode") = x$1
    unit(())
  }
  override def leftHashSemiJoinOp_Field_Mode[A, B, C](self: Rep[LeftHashSemiJoinOp[A, B, C]])(implicit typeA: TypeRep[A], typeB: TypeRep[B], typeC: TypeRep[C]): Rep[Int] = {
    mutableValues.get(self -> "mode").getOrElse(unit(0)).asInstanceOf[Rep[Int]]
  }
  override def nestedLoopsJoinOp_Field_Mode_$eq[A <: ch.epfl.data.pardis.shallow.Record, B <: ch.epfl.data.pardis.shallow.Record](self: Rep[NestedLoopsJoinOp[A, B]], x$1: Rep[Int])(implicit typeA: TypeRep[A], typeB: TypeRep[B]): Rep[Unit] = {
    mutableValues(self -> "mode") = x$1
    unit(())
  }
  override def nestedLoopsJoinOp_Field_Mode[A <: ch.epfl.data.pardis.shallow.Record, B <: ch.epfl.data.pardis.shallow.Record](self: Rep[NestedLoopsJoinOp[A, B]])(implicit typeA: TypeRep[A], typeB: TypeRep[B]): Rep[Int] = {
    mutableValues.get(self -> "mode").getOrElse(unit(0)).asInstanceOf[Rep[Int]]
  }
  override def leftOuterJoinOp_Field_Mode_$eq[A <: ch.epfl.data.pardis.shallow.Record, B <: ch.epfl.data.pardis.shallow.Record, C](self: Rep[LeftOuterJoinOp[A, B, C]], x$1: Rep[Int])(implicit typeA: TypeRep[A], typeB: TypeRep[B], typeC: TypeRep[C]): Rep[Unit] = {
    mutableValues(self -> "mode") = x$1
    unit(())
  }
  override def leftOuterJoinOp_Field_Mode[A <: ch.epfl.data.pardis.shallow.Record, B <: ch.epfl.data.pardis.shallow.Record, C](self: Rep[LeftOuterJoinOp[A, B, C]])(implicit typeA: TypeRep[A], typeB: TypeRep[B], typeC: TypeRep[C]): Rep[Int] = {
    mutableValues.get(self -> "mode").getOrElse(unit(0)).asInstanceOf[Rep[Int]]
  }
  override def hashJoinAnti_Field_Mode_$eq[A, B, C](self: Rep[HashJoinAnti[A, B, C]], x$1: Rep[Int])(implicit typeA: TypeRep[A], typeB: TypeRep[B], typeC: TypeRep[C]): Rep[Unit] = {
    mutableValues(self -> "mode") = x$1
    unit(())
  }
  override def hashJoinAnti_Field_Mode[A, B, C](self: Rep[HashJoinAnti[A, B, C]])(implicit typeA: TypeRep[A], typeB: TypeRep[B], typeC: TypeRep[C]): Rep[Int] = {
    mutableValues.get(self -> "mode").getOrElse(unit(0)).asInstanceOf[Rep[Int]]
  }

  override def printOp_Field_Parent[A](self: Rep[PrintOp[A]])(implicit typeA: TypeRep[A]): Rep[Operator[A]] = {
    self match {
      case Def(x: PrintOpNew[_]) => x.parent
      case _                     => super.printOp_Field_Parent(self)
    }
  }

  override def sortOp_Field_Parent[A](self: Rep[SortOp[A]])(implicit typeA: TypeRep[A]): Rep[Operator[A]] = {
    self match {
      case Def(x: SortOpNew[_]) => x.parent
      case _                    => super.sortOp_Field_Parent(self)
    }
  }

  override def scanOp_Field_Table[A](self: Rep[ScanOp[A]])(implicit typeA: TypeRep[A]): Rep[Array[A]] = {
    self match {
      case Def(x: ScanOpNew[_]) => x.table
      case _                    => super.scanOp_Field_Table(self)
    }
  }

  override def mapOp_Field_Parent[A](self: Rep[MapOp[A]])(implicit typeA: TypeRep[A]): Rep[Operator[A]] = {
    self match {
      case Def(x: MapOpNew[_]) => x.parent
      case _                   => super.mapOp_Field_Parent(self)
    }
  }

  override def selectOp_Field_Parent[A](self: Rep[SelectOp[A]])(implicit typeA: TypeRep[A]): Rep[Operator[A]] = {
    self match {
      case Def(x: SelectOpNew[_]) => x.parent
      case _                      => super.selectOp_Field_Parent(self)
    }
  }

  override def aggOp_Field_Parent[A, B](self: Rep[AggOp[A, B]])(implicit typeA: TypeRep[A], typeB: TypeRep[B]): Rep[Operator[A]] = {
    self match {
      case Def(x: AggOpNew[_, _]) => x.parent
      case _                      => super.aggOp_Field_Parent(self)
    }
  }

  override def aggOp_Field_NumAggs[A, B](self: Rep[AggOp[A, B]])(implicit typeA: TypeRep[A], typeB: TypeRep[B]): Rep[Int] = {
    self match {
      case Def(x: AggOpNew[_, _]) => x.numAggs
      case _                      => super.aggOp_Field_NumAggs(self)
    }
  }

  override def windowOp_Field_Parent[A, B, C](self: Rep[WindowOp[A, B, C]])(implicit typeA: TypeRep[A], typeB: TypeRep[B], typeC: TypeRep[C]): Rep[Operator[A]] = {
    self match {
      case Def(x: WindowOpNew[_, _, _]) => x.parent
      case _                            => super.windowOp_Field_Parent(self)
    }
  }

  override def mapOp_Field_AggFuncs[A](self: Rep[MapOp[A]])(implicit typeA: TypeRep[A]): Rep[Seq[A => Unit]] = self match {
    case Def(MapOpNew(_, funs)) => funs
    case _                      => super.mapOp_Field_AggFuncs(self)
  }

  override def aggOp_Field_Grp[A, B](self: Rep[AggOp[A, B]])(implicit typeA: TypeRep[A], typeB: TypeRep[B]): Rep[A => B] = self match {
    case Def(AggOpNew(_, _, f, _)) => f
    case _                         => ???
  }
  override def aggOp_Field_AggFuncs[A, B](self: Rep[AggOp[A, B]])(implicit typeA: TypeRep[A], typeB: TypeRep[B]): Rep[Seq[((A, Double) => Double)]] = self match {
    case Def(AggOpNew(_, _, _, funs)) => funs
    case _                            => ???
  }

  override def printOp_Field_PrintFunc[A](self: Rep[PrintOp[A]])(implicit typeA: TypeRep[A]): Rep[A => Unit] = self match {
    case Def(PrintOpNew(_, f, _)) => f
    case _                        => ???
  }
  override def printOp_Field_Limit[A](self: Rep[PrintOp[A]])(implicit typeA: TypeRep[A]): Rep[() => Boolean] = self match {
    case Def(PrintOpNew(_, _, limit)) => limit
    case _                            => ???
  }

  override def selectOp_Field_SelectPred[A](self: Rep[SelectOp[A]])(implicit typeA: TypeRep[A]): Rep[A => Boolean] = self match {
    case Def(SelectOpNew(_, f)) => f
    case _                      => ???
  }

  override def windowOp_Field_Wndf[A, B, C](self: Rep[WindowOp[A, B, C]])(implicit typeA: TypeRep[A], typeB: TypeRep[B], typeC: TypeRep[C]): Rep[(ArrayBuffer[A] => C)] = self match {
    case Def(x: WindowOpNew[_, _, _]) => x.wndf
    case _                            => super.windowOp_Field_Wndf(self)
  }
  override def windowOp_Field_Grp[A, B, C](self: Rep[WindowOp[A, B, C]])(implicit typeA: TypeRep[A], typeB: TypeRep[B], typeC: TypeRep[C]): Rep[(A => B)] = self match {
    case Def(x: WindowOpNew[_, _, _]) => x.grp
    case _                            => super.windowOp_Field_Grp(self)
  }

  override def hashJoinOp_Field_RightParent[A <: ch.epfl.data.pardis.shallow.Record, B <: ch.epfl.data.pardis.shallow.Record, C](self: Rep[HashJoinOp[A, B, C]])(implicit typeA: TypeRep[A], typeB: TypeRep[B], typeC: TypeRep[C]): Rep[Operator[B]] = {
    self match {
      case Def(x: HashJoinOpNew1[_, _, _]) => x.rightParent
      case _                               => super.hashJoinOp_Field_RightParent(self)
    }
  }
  override def hashJoinOp_Field_LeftParent[A <: ch.epfl.data.pardis.shallow.Record, B <: ch.epfl.data.pardis.shallow.Record, C](self: Rep[HashJoinOp[A, B, C]])(implicit typeA: TypeRep[A], typeB: TypeRep[B], typeC: TypeRep[C]): Rep[Operator[A]] = {
    self match {
      case Def(x: HashJoinOpNew1[_, _, _]) => x.leftParent
      case _                               => super.hashJoinOp_Field_LeftParent(self)
    }
  }

  override def hashJoinOp_Field_JoinCond[A <: ch.epfl.data.pardis.shallow.Record, B <: ch.epfl.data.pardis.shallow.Record, C](self: Rep[HashJoinOp[A, B, C]])(implicit typeA: TypeRep[A], typeB: TypeRep[B], typeC: TypeRep[C]): Rep[((A, B) => Boolean)] = self match {
    case Def(x: HashJoinOpNew1[_, _, _]) => x.joinCond
    case _                               => super.hashJoinOp_Field_JoinCond(self)
  }

  override def hashJoinOp_Field_LeftHash[A <: ch.epfl.data.pardis.shallow.Record, B <: ch.epfl.data.pardis.shallow.Record, C](self: Rep[HashJoinOp[A, B, C]])(implicit typeA: TypeRep[A], typeB: TypeRep[B], typeC: TypeRep[C]): Rep[(A => C)] = self match {
    case Def(x: HashJoinOpNew1[_, _, _]) => x.leftHash
    case _                               => super.hashJoinOp_Field_LeftHash(self)
  }

  override def hashJoinOp_Field_RightHash[A <: ch.epfl.data.pardis.shallow.Record, B <: ch.epfl.data.pardis.shallow.Record, C](self: Rep[HashJoinOp[A, B, C]])(implicit typeA: TypeRep[A], typeB: TypeRep[B], typeC: TypeRep[C]): Rep[(B => C)] = self match {
    case Def(x: HashJoinOpNew1[_, _, _]) => x.rightHash
    case _                               => super.hashJoinOp_Field_RightHash(self)
  }

  override def hashJoinOp_Field_RightAlias[A <: ch.epfl.data.pardis.shallow.Record, B <: ch.epfl.data.pardis.shallow.Record, C](self: Rep[HashJoinOp[A, B, C]])(implicit typeA: TypeRep[A], typeB: TypeRep[B], typeC: TypeRep[C]): Rep[String] = self match {
    case Def(x: HashJoinOpNew1[_, _, _]) => x.rightAlias
    case _                               => super.hashJoinOp_Field_RightAlias(self)
  }

  override def hashJoinOp_Field_LeftAlias[A <: ch.epfl.data.pardis.shallow.Record, B <: ch.epfl.data.pardis.shallow.Record, C](self: Rep[HashJoinOp[A, B, C]])(implicit typeA: TypeRep[A], typeB: TypeRep[B], typeC: TypeRep[C]): Rep[String] = self match {
    case Def(x: HashJoinOpNew1[_, _, _]) => x.leftAlias
    case _                               => super.hashJoinOp_Field_LeftAlias(self)
  }

  override def leftHashSemiJoinOp_Field_JoinCond[A, B, C](self: Rep[LeftHashSemiJoinOp[A, B, C]])(implicit typeA: TypeRep[A], typeB: TypeRep[B], typeC: TypeRep[C]): Rep[((A, B) => Boolean)] = self match {
    case Def(x: LeftHashSemiJoinOpNew[_, _, _]) => x.joinCond
    case _                                      => super.leftHashSemiJoinOp_Field_JoinCond(self)
  }

  override def leftHashSemiJoinOp_Field_RightHash[A, B, C](self: Rep[LeftHashSemiJoinOp[A, B, C]])(implicit typeA: TypeRep[A], typeB: TypeRep[B], typeC: TypeRep[C]): Rep[(B => C)] = self match {
    case Def(x: LeftHashSemiJoinOpNew[_, _, _]) => x.rightHash
    case _                                      => super.leftHashSemiJoinOp_Field_RightHash(self)
  }
  override def leftHashSemiJoinOp_Field_LeftHash[A, B, C](self: Rep[LeftHashSemiJoinOp[A, B, C]])(implicit typeA: TypeRep[A], typeB: TypeRep[B], typeC: TypeRep[C]): Rep[(A => C)] = self match {
    case Def(x: LeftHashSemiJoinOpNew[_, _, _]) => x.leftHash
    case _                                      => super.leftHashSemiJoinOp_Field_LeftHash(self)
  }
  override def leftHashSemiJoinOp_Field_RightParent[A, B, C](self: Rep[LeftHashSemiJoinOp[A, B, C]])(implicit typeA: TypeRep[A], typeB: TypeRep[B], typeC: TypeRep[C]): Rep[Operator[B]] = self match {
    case Def(x: LeftHashSemiJoinOpNew[_, _, _]) => x.rightParent
    case _                                      => super.leftHashSemiJoinOp_Field_RightParent(self)
  }
  override def leftHashSemiJoinOp_Field_LeftParent[A, B, C](self: Rep[LeftHashSemiJoinOp[A, B, C]])(implicit typeA: TypeRep[A], typeB: TypeRep[B], typeC: TypeRep[C]): Rep[Operator[A]] = self match {
    case Def(x: LeftHashSemiJoinOpNew[_, _, _]) => x.leftParent
    case _                                      => super.leftHashSemiJoinOp_Field_LeftParent(self)
  }

  override def nestedLoopsJoinOp_Field_RightParent[A <: ch.epfl.data.pardis.shallow.Record, B <: ch.epfl.data.pardis.shallow.Record](self: Rep[NestedLoopsJoinOp[A, B]])(implicit typeA: TypeRep[A], typeB: TypeRep[B]): Rep[Operator[B]] = self match {
    case Def(x: NestedLoopsJoinOpNew[_, _]) => x.rightParent
    case _                                  => super.nestedLoopsJoinOp_Field_RightParent(self)
  }
  override def nestedLoopsJoinOp_Field_LeftParent[A <: ch.epfl.data.pardis.shallow.Record, B <: ch.epfl.data.pardis.shallow.Record](self: Rep[NestedLoopsJoinOp[A, B]])(implicit typeA: TypeRep[A], typeB: TypeRep[B]): Rep[Operator[A]] = self match {
    case Def(x: NestedLoopsJoinOpNew[_, _]) => x.leftParent
    case _                                  => super.nestedLoopsJoinOp_Field_LeftParent(self)
  }
  override def nestedLoopsJoinOp_Field_JoinCond[A <: ch.epfl.data.pardis.shallow.Record, B <: ch.epfl.data.pardis.shallow.Record](self: Rep[NestedLoopsJoinOp[A, B]])(implicit typeA: TypeRep[A], typeB: TypeRep[B]): Rep[((A, B) => Boolean)] = self match {
    case Def(x: NestedLoopsJoinOpNew[_, _]) => x.joinCond
    case _                                  => super.nestedLoopsJoinOp_Field_JoinCond(self)
  }
  override def nestedLoopsJoinOp_Field_RightAlias[A <: ch.epfl.data.pardis.shallow.Record, B <: ch.epfl.data.pardis.shallow.Record](self: Rep[NestedLoopsJoinOp[A, B]])(implicit typeA: TypeRep[A], typeB: TypeRep[B]): Rep[String] = self match {
    case Def(x: NestedLoopsJoinOpNew[_, _]) => x.rightAlias
    case _                                  => super.nestedLoopsJoinOp_Field_RightAlias(self)
  }
  override def nestedLoopsJoinOp_Field_LeftAlias[A <: ch.epfl.data.pardis.shallow.Record, B <: ch.epfl.data.pardis.shallow.Record](self: Rep[NestedLoopsJoinOp[A, B]])(implicit typeA: TypeRep[A], typeB: TypeRep[B]): Rep[String] = self match {
    case Def(x: NestedLoopsJoinOpNew[_, _]) => x.leftAlias
    case _                                  => super.nestedLoopsJoinOp_Field_LeftAlias(self)
  }

  override def subquerySingleResult_Field_Parent[A](self: Rep[SubquerySingleResult[A]])(implicit typeA: TypeRep[A]): Rep[Operator[A]] = {
    self match {
      case Def(x: SubquerySingleResultNew[_]) => x.parent
      case _                                  => super.subquerySingleResult_Field_Parent(self)
    }
  }

  override def viewOp_Field_Parent[A](self: Rep[ViewOp[A]])(implicit typeA: TypeRep[A]): Rep[Operator[A]] = {
    self match {
      case Def(x: ViewOpNew[_]) => x.parent
      case _                    => super.viewOp_Field_Parent(self)
    }
  }

  override def hashJoinAnti_Field_RightParent[A, B, C](self: Rep[HashJoinAnti[A, B, C]])(implicit typeA: TypeRep[A], typeB: TypeRep[B], typeC: TypeRep[C]): Rep[Operator[B]] = {
    self match {
      case Def(x: HashJoinAntiNew[_, _, _]) => x.rightParent
      case _                                => super.hashJoinAnti_Field_RightParent(self)
    }
  }

  override def hashJoinAnti_Field_LeftParent[A, B, C](self: Rep[HashJoinAnti[A, B, C]])(implicit typeA: TypeRep[A], typeB: TypeRep[B], typeC: TypeRep[C]): Rep[Operator[A]] = {
    self match {
      case Def(x: HashJoinAntiNew[_, _, _]) => x.leftParent
      case _                                => super.hashJoinAnti_Field_LeftParent(self)
    }
  }

  override def hashJoinAnti_Field_JoinCond[A, B, C](self: Rep[HashJoinAnti[A, B, C]])(implicit typeA: TypeRep[A], typeB: TypeRep[B], typeC: TypeRep[C]): Rep[((A, B) => Boolean)] = self match {
    case Def(x: HashJoinAntiNew[_, _, _]) => x.joinCond
    case _                                => super.hashJoinAnti_Field_JoinCond(self)
  }

  override def hashJoinAnti_Field_LeftHash[A, B, C](self: Rep[HashJoinAnti[A, B, C]])(implicit typeA: TypeRep[A], typeB: TypeRep[B], typeC: TypeRep[C]): Rep[(A => C)] = self match {
    case Def(x: HashJoinAntiNew[_, _, _]) => x.leftHash
    case _                                => super.hashJoinAnti_Field_LeftHash(self)
  }

  override def hashJoinAnti_Field_RightHash[A, B, C](self: Rep[HashJoinAnti[A, B, C]])(implicit typeA: TypeRep[A], typeB: TypeRep[B], typeC: TypeRep[C]): Rep[(B => C)] = self match {
    case Def(x: HashJoinAntiNew[_, _, _]) => x.rightHash
    case _                                => super.hashJoinAnti_Field_RightHash(self)
  }

  override def leftOuterJoinOp_Field_RightHash[A <: ch.epfl.data.pardis.shallow.Record, B <: ch.epfl.data.pardis.shallow.Record, C](self: Rep[LeftOuterJoinOp[A, B, C]])(implicit typeA: TypeRep[A], typeB: TypeRep[B], typeC: TypeRep[C]): Rep[(B => C)] = self match {
    case Def(x: LeftOuterJoinOpNew[_, _, _]) => x.rightHash
    case _                                   => super.leftOuterJoinOp_Field_RightHash(self)
  }
  override def leftOuterJoinOp_Field_LeftHash[A <: ch.epfl.data.pardis.shallow.Record, B <: ch.epfl.data.pardis.shallow.Record, C](self: Rep[LeftOuterJoinOp[A, B, C]])(implicit typeA: TypeRep[A], typeB: TypeRep[B], typeC: TypeRep[C]): Rep[(A => C)] = self match {
    case Def(x: LeftOuterJoinOpNew[_, _, _]) => x.leftHash
    case _                                   => super.leftOuterJoinOp_Field_LeftHash(self)
  }
  override def leftOuterJoinOp_Field_JoinCond[A <: ch.epfl.data.pardis.shallow.Record, B <: ch.epfl.data.pardis.shallow.Record, C](self: Rep[LeftOuterJoinOp[A, B, C]])(implicit typeA: TypeRep[A], typeB: TypeRep[B], typeC: TypeRep[C]): Rep[((A, B) => Boolean)] = self match {
    case Def(x: LeftOuterJoinOpNew[_, _, _]) => x.joinCond
    case _                                   => super.leftOuterJoinOp_Field_JoinCond(self)
  }
  override def leftOuterJoinOp_Field_RightParent[A <: ch.epfl.data.pardis.shallow.Record, B <: ch.epfl.data.pardis.shallow.Record, C](self: Rep[LeftOuterJoinOp[A, B, C]])(implicit typeA: TypeRep[A], typeB: TypeRep[B], typeC: TypeRep[C]): Rep[Operator[B]] = self match {
    case Def(x: LeftOuterJoinOpNew[_, _, _]) => x.rightParent
    case _                                   => super.leftOuterJoinOp_Field_RightParent(self)
  }
  override def leftOuterJoinOp_Field_LeftParent[A <: ch.epfl.data.pardis.shallow.Record, B <: ch.epfl.data.pardis.shallow.Record, C](self: Rep[LeftOuterJoinOp[A, B, C]])(implicit typeA: TypeRep[A], typeB: TypeRep[B], typeC: TypeRep[C]): Rep[Operator[A]] = self match {
    case Def(x: LeftOuterJoinOpNew[_, _, _]) => x.leftParent
    case _                                   => super.leftOuterJoinOp_Field_LeftParent(self)
  }
  override def hashJoinOpNew2[A <: ch.epfl.data.pardis.shallow.Record, B <: ch.epfl.data.pardis.shallow.Record, C](leftParent: Rep[Operator[A]], rightParent: Rep[Operator[B]], joinCond: Rep[((A, B) => Boolean)], leftHash: Rep[((A) => C)], rightHash: Rep[((B) => C)])(implicit typeA: TypeRep[A], typeB: TypeRep[B], typeC: TypeRep[C]): Rep[HashJoinOp[A, B, C]] = hashJoinOpNew1[A, B, C](leftParent, rightParent, unit(""), unit(""), joinCond, leftHash, rightHash)
}
