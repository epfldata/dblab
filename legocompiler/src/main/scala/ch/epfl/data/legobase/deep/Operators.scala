
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
  }
  // constructors
  def __newOperator[A](implicit evidence$1: Manifest[A], manifestA: Manifest[A]): Rep[Operator[A]] = operatorNew[A](manifestA)
  // case classes
  case class OperatorNew[A]()(implicit val manifestA: Manifest[A]) extends FunctionDef[Operator[A]](None, "new Operator", List())
  case class OperatorOpen[A](self: Rep[Operator[A]])(implicit val manifestA: Manifest[A]) extends FunctionDef[Unit](Some(self), "open", List())
  case class OperatorNext[A](self: Rep[Operator[A]])(implicit val manifestA: Manifest[A]) extends FunctionDef[A](Some(self), "next", List())
  case class OperatorClose[A](self: Rep[Operator[A]])(implicit val manifestA: Manifest[A]) extends FunctionDef[Unit](Some(self), "close", List())
  case class OperatorReset[A](self: Rep[Operator[A]])(implicit val manifestA: Manifest[A]) extends FunctionDef[Unit](Some(self), "reset", List())
  case class OperatorForeach[A](self: Rep[Operator[A]], f: Rep[((A) => Unit)])(implicit val manifestA: Manifest[A]) extends FunctionDef[Unit](Some(self), "foreach", List(List(f)))
  case class OperatorFindFirst[A](self: Rep[Operator[A]], cond: Rep[((A) => Boolean)])(implicit val manifestA: Manifest[A]) extends FunctionDef[A](Some(self), "findFirst", List(List(cond)))
  case class OperatorNullDynamicRecord[A, D](self: Rep[Operator[A]])(implicit val manifestA: Manifest[A], val manifestD: Manifest[D]) extends FunctionDef[D](Some(self), "NullDynamicRecord", List())
  // method definitions
  def operatorNew[A](implicit manifestA: Manifest[A]): Rep[Operator[A]] = OperatorNew[A]()
  def operatorOpen[A](self: Rep[Operator[A]])(implicit manifestA: Manifest[A]): Rep[Unit] = OperatorOpen[A](self)
  def operatorNext[A](self: Rep[Operator[A]])(implicit manifestA: Manifest[A]): Rep[A] = OperatorNext[A](self)
  def operatorClose[A](self: Rep[Operator[A]])(implicit manifestA: Manifest[A]): Rep[Unit] = OperatorClose[A](self)
  def operatorReset[A](self: Rep[Operator[A]])(implicit manifestA: Manifest[A]): Rep[Unit] = OperatorReset[A](self)
  def operatorForeach[A](self: Rep[Operator[A]], f: Rep[((A) => Unit)])(implicit manifestA: Manifest[A]): Rep[Unit] = OperatorForeach[A](self, f)
  def operatorFindFirst[A](self: Rep[Operator[A]], cond: Rep[((A) => Boolean)])(implicit manifestA: Manifest[A]): Rep[A] = OperatorFindFirst[A](self, cond)
  def operatorNullDynamicRecord[A, D](self: Rep[Operator[A]])(implicit manifestA: Manifest[A], manifestD: Manifest[D]): Rep[D] = OperatorNullDynamicRecord[A, D](self)
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
  }
  // constructors
  def __newScanOp[A](table: Rep[Array[A]])(implicit evidence$3: Manifest[A], manifestA: Manifest[A]): Rep[ScanOp[A]] = scanOpNew[A](table)(manifestA)
  // case classes
  case class ScanOpNew[A](table: Rep[Array[A]])(implicit val manifestA: Manifest[A]) extends FunctionDef[ScanOp[A]](None, "new ScanOp", List(List(table)))
  case class ScanOpForeach[A](self: Rep[ScanOp[A]], f: Rep[((A) => Unit)])(implicit val manifestA: Manifest[A]) extends FunctionDef[Unit](Some(self), "foreach", List(List(f)))
  case class ScanOpFindFirst[A](self: Rep[ScanOp[A]], cond: Rep[((A) => Boolean)])(implicit val manifestA: Manifest[A]) extends FunctionDef[A](Some(self), "findFirst", List(List(cond)))
  case class ScanOpNullDynamicRecord[A, D](self: Rep[ScanOp[A]])(implicit val manifestA: Manifest[A], val manifestD: Manifest[D]) extends FunctionDef[D](Some(self), "NullDynamicRecord", List())
  case class ScanOpOpen[A](self: Rep[ScanOp[A]])(implicit val manifestA: Manifest[A]) extends FunctionDef[Unit](Some(self), "open", List())
  case class ScanOpNext[A](self: Rep[ScanOp[A]])(implicit val manifestA: Manifest[A]) extends FunctionDef[A](Some(self), "next", List())
  case class ScanOpClose[A](self: Rep[ScanOp[A]])(implicit val manifestA: Manifest[A]) extends FunctionDef[Unit](Some(self), "close", List())
  case class ScanOpReset[A](self: Rep[ScanOp[A]])(implicit val manifestA: Manifest[A]) extends FunctionDef[Unit](Some(self), "reset", List())
  // method definitions
  def scanOpNew[A](table: Rep[Array[A]])(implicit manifestA: Manifest[A]): Rep[ScanOp[A]] = ScanOpNew[A](table)
  def scanOpForeach[A](self: Rep[ScanOp[A]], f: Rep[((A) => Unit)])(implicit manifestA: Manifest[A]): Rep[Unit] = ScanOpForeach[A](self, f)
  def scanOpFindFirst[A](self: Rep[ScanOp[A]], cond: Rep[((A) => Boolean)])(implicit manifestA: Manifest[A]): Rep[A] = ScanOpFindFirst[A](self, cond)
  def scanOpNullDynamicRecord[A, D](self: Rep[ScanOp[A]])(implicit manifestA: Manifest[A], manifestD: Manifest[D]): Rep[D] = ScanOpNullDynamicRecord[A, D](self)
  def scanOpOpen[A](self: Rep[ScanOp[A]])(implicit manifestA: Manifest[A]): Rep[Unit] = ScanOpOpen[A](self)
  def scanOpNext[A](self: Rep[ScanOp[A]])(implicit manifestA: Manifest[A]): Rep[A] = ScanOpNext[A](self)
  def scanOpClose[A](self: Rep[ScanOp[A]])(implicit manifestA: Manifest[A]): Rep[Unit] = ScanOpClose[A](self)
  def scanOpReset[A](self: Rep[ScanOp[A]])(implicit manifestA: Manifest[A]): Rep[Unit] = ScanOpReset[A](self)
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
  }
  // constructors
  def __newSelectOp[A](parent: Rep[Operator[A]])(selectPred: Rep[(A => Boolean)])(implicit evidence$4: Manifest[A], manifestA: Manifest[A]): Rep[SelectOp[A]] = selectOpNew[A](parent, selectPred)(manifestA)
  // case classes
  case class SelectOpNew[A](parent: Rep[Operator[A]], selectPred: Rep[((A) => Boolean)])(implicit val manifestA: Manifest[A]) extends FunctionDef[SelectOp[A]](None, "new SelectOp", List(List(parent), List(selectPred)))
  case class SelectOpForeach[A](self: Rep[SelectOp[A]], f: Rep[((A) => Unit)])(implicit val manifestA: Manifest[A]) extends FunctionDef[Unit](Some(self), "foreach", List(List(f)))
  case class SelectOpFindFirst[A](self: Rep[SelectOp[A]], cond: Rep[((A) => Boolean)])(implicit val manifestA: Manifest[A]) extends FunctionDef[A](Some(self), "findFirst", List(List(cond)))
  case class SelectOpNullDynamicRecord[A, D](self: Rep[SelectOp[A]])(implicit val manifestA: Manifest[A], val manifestD: Manifest[D]) extends FunctionDef[D](Some(self), "NullDynamicRecord", List())
  case class SelectOpOpen[A](self: Rep[SelectOp[A]])(implicit val manifestA: Manifest[A]) extends FunctionDef[Unit](Some(self), "open", List())
  case class SelectOpNext[A](self: Rep[SelectOp[A]])(implicit val manifestA: Manifest[A]) extends FunctionDef[A](Some(self), "next", List())
  case class SelectOpClose[A](self: Rep[SelectOp[A]])(implicit val manifestA: Manifest[A]) extends FunctionDef[Unit](Some(self), "close", List())
  case class SelectOpReset[A](self: Rep[SelectOp[A]])(implicit val manifestA: Manifest[A]) extends FunctionDef[Unit](Some(self), "reset", List())
  // method definitions
  def selectOpNew[A](parent: Rep[Operator[A]], selectPred: Rep[((A) => Boolean)])(implicit manifestA: Manifest[A]): Rep[SelectOp[A]] = SelectOpNew[A](parent, selectPred)
  def selectOpForeach[A](self: Rep[SelectOp[A]], f: Rep[((A) => Unit)])(implicit manifestA: Manifest[A]): Rep[Unit] = SelectOpForeach[A](self, f)
  def selectOpFindFirst[A](self: Rep[SelectOp[A]], cond: Rep[((A) => Boolean)])(implicit manifestA: Manifest[A]): Rep[A] = SelectOpFindFirst[A](self, cond)
  def selectOpNullDynamicRecord[A, D](self: Rep[SelectOp[A]])(implicit manifestA: Manifest[A], manifestD: Manifest[D]): Rep[D] = SelectOpNullDynamicRecord[A, D](self)
  def selectOpOpen[A](self: Rep[SelectOp[A]])(implicit manifestA: Manifest[A]): Rep[Unit] = SelectOpOpen[A](self)
  def selectOpNext[A](self: Rep[SelectOp[A]])(implicit manifestA: Manifest[A]): Rep[A] = SelectOpNext[A](self)
  def selectOpClose[A](self: Rep[SelectOp[A]])(implicit manifestA: Manifest[A]): Rep[Unit] = SelectOpClose[A](self)
  def selectOpReset[A](self: Rep[SelectOp[A]])(implicit manifestA: Manifest[A]): Rep[Unit] = SelectOpReset[A](self)
  type SelectOp[A] = ch.epfl.data.legobase.queryengine.volcano.SelectOp[A]
}
trait SelectOpImplicits { this: SelectOpComponent =>
  // Add implicit conversions here!
}
trait SelectOpComponent extends SelectOpOps with SelectOpImplicits { self: OperatorsComponent => }
trait AggOpOps extends Base { this: OperatorsComponent =>
  implicit class AggOpRep[A, B](self: Rep[AggOp[A, B]])(implicit manifestA: Manifest[A], manifestB: Manifest[B], evidence$6: Manifest[B], evidence$5: Manifest[A]) {
    def foreach(f: Rep[(A => Unit)]): Rep[Unit] = aggOpForeach[A, B](self, f)(manifestA, manifestB)
    def findFirst(cond: Rep[(A => Boolean)]): Rep[A] = aggOpFindFirst[A, B](self, cond)(manifestA, manifestB)
    def NullDynamicRecord[D](implicit manifestD: Manifest[D]): Rep[D] = aggOpNullDynamicRecord[A, B, D](self)(manifestA, manifestB, manifestD)
    def open(): Rep[Unit] = aggOpOpen[A, B](self)(manifestA, manifestB)
    def next(): Rep[AGGRecord[B]] = aggOpNext[A, B](self)(manifestA, manifestB)
    def close(): Rep[Unit] = aggOpClose[A, B](self)(manifestA, manifestB)
    def reset(): Rep[Unit] = aggOpReset[A, B](self)(manifestA, manifestB)
  }
  // constructors
  def __newAggOp[A, B](parent: Rep[Operator[A]], numAggs: Rep[Int])(grp: Rep[(A => B)])(aggFuncs: Rep[((A, Double) => Double)]*)(implicit evidence$5: Manifest[A], evidence$6: Manifest[B], manifestA: Manifest[A], manifestB: Manifest[B]): Rep[AggOp[A, B]] = aggOpNew[A, B](parent, numAggs, grp, aggFuncs: _*)(manifestA, manifestB)
  // case classes
  case class AggOpNew[A, B](parent: Rep[Operator[A]], numAggs: Rep[Int], grp: Rep[((A) => B)], aggFuncsOutput: Rep[Seq[((A, Double) => Double)]])(implicit val manifestA: Manifest[A], val manifestB: Manifest[B]) extends FunctionDef[AggOp[A, B]](None, "new AggOp", List(List(parent, numAggs), List(grp), List(__varArg(aggFuncsOutput))))
  case class AggOpForeach[A, B](self: Rep[AggOp[A, B]], f: Rep[((A) => Unit)])(implicit val manifestA: Manifest[A], val manifestB: Manifest[B]) extends FunctionDef[Unit](Some(self), "foreach", List(List(f)))
  case class AggOpFindFirst[A, B](self: Rep[AggOp[A, B]], cond: Rep[((A) => Boolean)])(implicit val manifestA: Manifest[A], val manifestB: Manifest[B]) extends FunctionDef[A](Some(self), "findFirst", List(List(cond)))
  case class AggOpNullDynamicRecord[A, B, D](self: Rep[AggOp[A, B]])(implicit val manifestA: Manifest[A], val manifestB: Manifest[B], val manifestD: Manifest[D]) extends FunctionDef[D](Some(self), "NullDynamicRecord", List())
  case class AggOpOpen[A, B](self: Rep[AggOp[A, B]])(implicit val manifestA: Manifest[A], val manifestB: Manifest[B]) extends FunctionDef[Unit](Some(self), "open", List())
  case class AggOpNext[A, B](self: Rep[AggOp[A, B]])(implicit val manifestA: Manifest[A], val manifestB: Manifest[B]) extends FunctionDef[AGGRecord[B]](Some(self), "next", List())
  case class AggOpClose[A, B](self: Rep[AggOp[A, B]])(implicit val manifestA: Manifest[A], val manifestB: Manifest[B]) extends FunctionDef[Unit](Some(self), "close", List())
  case class AggOpReset[A, B](self: Rep[AggOp[A, B]])(implicit val manifestA: Manifest[A], val manifestB: Manifest[B]) extends FunctionDef[Unit](Some(self), "reset", List())
  // method definitions
  def aggOpNew[A, B](parent: Rep[Operator[A]], numAggs: Rep[Int], grp: Rep[((A) => B)], aggFuncs: Rep[((A, Double) => Double)]*)(implicit manifestA: Manifest[A], manifestB: Manifest[B]): Rep[AggOp[A, B]] = {
    val aggFuncsOutput = __liftSeq(aggFuncs.toSeq)
    AggOpNew[A, B](parent, numAggs, grp, aggFuncsOutput)
  }
  def aggOpForeach[A, B](self: Rep[AggOp[A, B]], f: Rep[((A) => Unit)])(implicit manifestA: Manifest[A], manifestB: Manifest[B]): Rep[Unit] = AggOpForeach[A, B](self, f)
  def aggOpFindFirst[A, B](self: Rep[AggOp[A, B]], cond: Rep[((A) => Boolean)])(implicit manifestA: Manifest[A], manifestB: Manifest[B]): Rep[A] = AggOpFindFirst[A, B](self, cond)
  def aggOpNullDynamicRecord[A, B, D](self: Rep[AggOp[A, B]])(implicit manifestA: Manifest[A], manifestB: Manifest[B], manifestD: Manifest[D]): Rep[D] = AggOpNullDynamicRecord[A, B, D](self)
  def aggOpOpen[A, B](self: Rep[AggOp[A, B]])(implicit manifestA: Manifest[A], manifestB: Manifest[B]): Rep[Unit] = AggOpOpen[A, B](self)
  def aggOpNext[A, B](self: Rep[AggOp[A, B]])(implicit manifestA: Manifest[A], manifestB: Manifest[B]): Rep[AGGRecord[B]] = AggOpNext[A, B](self)
  def aggOpClose[A, B](self: Rep[AggOp[A, B]])(implicit manifestA: Manifest[A], manifestB: Manifest[B]): Rep[Unit] = AggOpClose[A, B](self)
  def aggOpReset[A, B](self: Rep[AggOp[A, B]])(implicit manifestA: Manifest[A], manifestB: Manifest[B]): Rep[Unit] = AggOpReset[A, B](self)
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
  }
  // constructors
  def __newSortOp[A](parent: Rep[Operator[A]])(orderingFunc: Rep[((A, A) => Int)])(implicit evidence$7: Manifest[A], manifestA: Manifest[A]): Rep[SortOp[A]] = sortOpNew[A](parent, orderingFunc)(manifestA)
  // case classes
  case class SortOpNew[A](parent: Rep[Operator[A]], orderingFunc: Rep[((A, A) => Int)])(implicit val manifestA: Manifest[A]) extends FunctionDef[SortOp[A]](None, "new SortOp", List(List(parent), List(orderingFunc)))
  case class SortOpForeach[A](self: Rep[SortOp[A]], f: Rep[((A) => Unit)])(implicit val manifestA: Manifest[A]) extends FunctionDef[Unit](Some(self), "foreach", List(List(f)))
  case class SortOpFindFirst[A](self: Rep[SortOp[A]], cond: Rep[((A) => Boolean)])(implicit val manifestA: Manifest[A]) extends FunctionDef[A](Some(self), "findFirst", List(List(cond)))
  case class SortOpNullDynamicRecord[A, D](self: Rep[SortOp[A]])(implicit val manifestA: Manifest[A], val manifestD: Manifest[D]) extends FunctionDef[D](Some(self), "NullDynamicRecord", List())
  case class SortOpOpen[A](self: Rep[SortOp[A]])(implicit val manifestA: Manifest[A]) extends FunctionDef[Unit](Some(self), "open", List())
  case class SortOpNext[A](self: Rep[SortOp[A]])(implicit val manifestA: Manifest[A]) extends FunctionDef[A](Some(self), "next", List())
  case class SortOpClose[A](self: Rep[SortOp[A]])(implicit val manifestA: Manifest[A]) extends FunctionDef[Unit](Some(self), "close", List())
  case class SortOpReset[A](self: Rep[SortOp[A]])(implicit val manifestA: Manifest[A]) extends FunctionDef[Unit](Some(self), "reset", List())
  // method definitions
  def sortOpNew[A](parent: Rep[Operator[A]], orderingFunc: Rep[((A, A) => Int)])(implicit manifestA: Manifest[A]): Rep[SortOp[A]] = SortOpNew[A](parent, orderingFunc)
  def sortOpForeach[A](self: Rep[SortOp[A]], f: Rep[((A) => Unit)])(implicit manifestA: Manifest[A]): Rep[Unit] = SortOpForeach[A](self, f)
  def sortOpFindFirst[A](self: Rep[SortOp[A]], cond: Rep[((A) => Boolean)])(implicit manifestA: Manifest[A]): Rep[A] = SortOpFindFirst[A](self, cond)
  def sortOpNullDynamicRecord[A, D](self: Rep[SortOp[A]])(implicit manifestA: Manifest[A], manifestD: Manifest[D]): Rep[D] = SortOpNullDynamicRecord[A, D](self)
  def sortOpOpen[A](self: Rep[SortOp[A]])(implicit manifestA: Manifest[A]): Rep[Unit] = SortOpOpen[A](self)
  def sortOpNext[A](self: Rep[SortOp[A]])(implicit manifestA: Manifest[A]): Rep[A] = SortOpNext[A](self)
  def sortOpClose[A](self: Rep[SortOp[A]])(implicit manifestA: Manifest[A]): Rep[Unit] = SortOpClose[A](self)
  def sortOpReset[A](self: Rep[SortOp[A]])(implicit manifestA: Manifest[A]): Rep[Unit] = SortOpReset[A](self)
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
  }
  // constructors
  def __newMapOp[A](parent: Rep[Operator[A]])(aggFuncs: Rep[(A => Unit)]*)(implicit evidence$8: Manifest[A], manifestA: Manifest[A]): Rep[MapOp[A]] = mapOpNew[A](parent, aggFuncs: _*)(manifestA)
  // case classes
  case class MapOpNew[A](parent: Rep[Operator[A]], aggFuncsOutput: Rep[Seq[((A) => Unit)]])(implicit val manifestA: Manifest[A]) extends FunctionDef[MapOp[A]](None, "new MapOp", List(List(parent), List(__varArg(aggFuncsOutput))))
  case class MapOpForeach[A](self: Rep[MapOp[A]], f: Rep[((A) => Unit)])(implicit val manifestA: Manifest[A]) extends FunctionDef[Unit](Some(self), "foreach", List(List(f)))
  case class MapOpFindFirst[A](self: Rep[MapOp[A]], cond: Rep[((A) => Boolean)])(implicit val manifestA: Manifest[A]) extends FunctionDef[A](Some(self), "findFirst", List(List(cond)))
  case class MapOpNullDynamicRecord[A, D](self: Rep[MapOp[A]])(implicit val manifestA: Manifest[A], val manifestD: Manifest[D]) extends FunctionDef[D](Some(self), "NullDynamicRecord", List())
  case class MapOpOpen[A](self: Rep[MapOp[A]])(implicit val manifestA: Manifest[A]) extends FunctionDef[Unit](Some(self), "open", List())
  case class MapOpNext[A](self: Rep[MapOp[A]])(implicit val manifestA: Manifest[A]) extends FunctionDef[A](Some(self), "next", List())
  case class MapOpClose[A](self: Rep[MapOp[A]])(implicit val manifestA: Manifest[A]) extends FunctionDef[Unit](Some(self), "close", List())
  case class MapOpReset[A](self: Rep[MapOp[A]])(implicit val manifestA: Manifest[A]) extends FunctionDef[Unit](Some(self), "reset", List())
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
  }
  // constructors
  def __newPrintOp[A](parent: Rep[Operator[A]])(printFunc: Rep[(A => Unit)], limit: Rep[(() => Boolean)])(implicit evidence$9: Manifest[A], manifestA: Manifest[A]): Rep[PrintOp[A]] = printOpNew[A](parent, printFunc, limit)(manifestA)
  // case classes
  case class PrintOpNew[A](parent: Rep[Operator[A]], printFunc: Rep[((A) => Unit)], limit: Rep[(() => Boolean)])(implicit val manifestA: Manifest[A]) extends FunctionDef[PrintOp[A]](None, "new PrintOp", List(List(parent), List(printFunc, limit)))
  case class PrintOpForeach[A](self: Rep[PrintOp[A]], f: Rep[((A) => Unit)])(implicit val manifestA: Manifest[A]) extends FunctionDef[Unit](Some(self), "foreach", List(List(f)))
  case class PrintOpFindFirst[A](self: Rep[PrintOp[A]], cond: Rep[((A) => Boolean)])(implicit val manifestA: Manifest[A]) extends FunctionDef[A](Some(self), "findFirst", List(List(cond)))
  case class PrintOpNullDynamicRecord[A, D](self: Rep[PrintOp[A]])(implicit val manifestA: Manifest[A], val manifestD: Manifest[D]) extends FunctionDef[D](Some(self), "NullDynamicRecord", List())
  case class PrintOpOpen[A](self: Rep[PrintOp[A]])(implicit val manifestA: Manifest[A]) extends FunctionDef[Unit](Some(self), "open", List())
  case class PrintOpNext[A](self: Rep[PrintOp[A]])(implicit val manifestA: Manifest[A]) extends FunctionDef[A](Some(self), "next", List())
  case class PrintOpClose[A](self: Rep[PrintOp[A]])(implicit val manifestA: Manifest[A]) extends FunctionDef[Unit](Some(self), "close", List())
  case class PrintOpReset[A](self: Rep[PrintOp[A]])(implicit val manifestA: Manifest[A]) extends FunctionDef[Unit](Some(self), "reset", List())
  // method definitions
  def printOpNew[A](parent: Rep[Operator[A]], printFunc: Rep[((A) => Unit)], limit: Rep[(() => Boolean)])(implicit manifestA: Manifest[A]): Rep[PrintOp[A]] = PrintOpNew[A](parent, printFunc, limit)
  def printOpForeach[A](self: Rep[PrintOp[A]], f: Rep[((A) => Unit)])(implicit manifestA: Manifest[A]): Rep[Unit] = PrintOpForeach[A](self, f)
  def printOpFindFirst[A](self: Rep[PrintOp[A]], cond: Rep[((A) => Boolean)])(implicit manifestA: Manifest[A]): Rep[A] = PrintOpFindFirst[A](self, cond)
  def printOpNullDynamicRecord[A, D](self: Rep[PrintOp[A]])(implicit manifestA: Manifest[A], manifestD: Manifest[D]): Rep[D] = PrintOpNullDynamicRecord[A, D](self)
  def printOpOpen[A](self: Rep[PrintOp[A]])(implicit manifestA: Manifest[A]): Rep[Unit] = PrintOpOpen[A](self)
  def printOpNext[A](self: Rep[PrintOp[A]])(implicit manifestA: Manifest[A]): Rep[A] = PrintOpNext[A](self)
  def printOpClose[A](self: Rep[PrintOp[A]])(implicit manifestA: Manifest[A]): Rep[Unit] = PrintOpClose[A](self)
  def printOpReset[A](self: Rep[PrintOp[A]])(implicit manifestA: Manifest[A]): Rep[Unit] = PrintOpReset[A](self)
  type PrintOp[A] = ch.epfl.data.legobase.queryengine.volcano.PrintOp[A]
}
trait PrintOpImplicits { this: PrintOpComponent =>
  // Add implicit conversions here!
}
trait PrintOpComponent extends PrintOpOps with PrintOpImplicits { self: OperatorsComponent => }
trait OperatorsComponent extends OperatorComponent with ScanOpComponent with SelectOpComponent with AggOpComponent with SortOpComponent with MapOpComponent with PrintOpComponent with AGGRecordComponent with CharacterComponent with DoubleComponent with IntComponent with LongComponent with ArrayComponent with LINEITEMRecordComponent with K2DBScannerComponent with IntegerComponent with BooleanComponent with HashMapComponent with SetComponent with TreeSetComponent with DefaultEntryComponent with ManualLiftedLegoBase { self: DeepDSL => }