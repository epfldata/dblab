package ch.epfl.data
package legobase
package deep

import scala.language.implicitConversions

// FIXME in the righthand side of the genreated case class invokations, type parameters should be filled in.

trait ManualLiftedLegoBase extends OptionOps with ListOps { this: DeepDSL =>

  // TODO auto generate this functions

  case class LoadLineItem() extends FunctionDef[Array[LINEITEMRecord]](None, "loadLineitem", List(Nil))
  def loadLineitem(): Rep[Array[LINEITEMRecord]] = LoadLineItem()
  case class ParseDate(date: Rep[String]) extends FunctionDef[Long](None, "parseDate", List(List(date)))
  def parseDate(date: Rep[String]): Rep[Long] = ParseDate(date)
  case class Println(x: Rep[Any]) extends FunctionDef[Unit](None, "println", List(List(x)))
  def println(x: Rep[Any]): Rep[Unit] = Println(x)
  case class Printf(text: Rep[String], xs: Rep[Any]*) extends FunctionDef[Unit](None, "printf", List(text :: xs.toList))
  def printf(text: Rep[String], xs: Rep[Any]*): Rep[Unit] = Printf(text, xs: _*)

  // TODO auto generate these fields

  case class LINEITEMRecord_Field_L_ORDERKEY(li: Rep[LINEITEMRecord]) extends FieldDef[Int](li, "L_ORDERKEY")
  case class LINEITEMRecord_Field_L_PARTKEY(li: Rep[LINEITEMRecord]) extends FieldDef[Int](li, "L_PARTKEY")
  case class LINEITEMRecord_Field_L_SUPPKEY(li: Rep[LINEITEMRecord]) extends FieldDef[Int](li, "L_SUPPKEY")
  case class LINEITEMRecord_Field_L_LINENUMBER(li: Rep[LINEITEMRecord]) extends FieldDef[Int](li, "L_LINENUMBER")
  case class LINEITEMRecord_Field_L_QUANTITY(li: Rep[LINEITEMRecord]) extends FieldDef[Double](li, "L_QUANTITY")
  case class LINEITEMRecord_Field_L_EXTENDEDPRICE(li: Rep[LINEITEMRecord]) extends FieldDef[Double](li, "L_EXTENDEDPRICE")
  case class LINEITEMRecord_Field_L_DISCOUNT(li: Rep[LINEITEMRecord]) extends FieldDef[Double](li, "L_DISCOUNT")
  case class LINEITEMRecord_Field_L_TAX(li: Rep[LINEITEMRecord]) extends FieldDef[Double](li, "L_TAX")
  case class LINEITEMRecord_Field_L_RETURNFLAG(li: Rep[LINEITEMRecord]) extends FieldDef[Character](li, "L_RETURNFLAG")
  case class LINEITEMRecord_Field_L_LINESTATUS(li: Rep[LINEITEMRecord]) extends FieldDef[Character](li, "L_LINESTATUS")
  case class LINEITEMRecord_Field_L_SHIPDATE(li: Rep[LINEITEMRecord]) extends FieldDef[Long](li, "L_SHIPDATE")
  case class LINEITEMRecord_Field_L_COMMITDATE(li: Rep[LINEITEMRecord]) extends FieldDef[Long](li, "L_COMMITDATE")
  case class LINEITEMRecord_Field_L_RECEIPTDATE(li: Rep[LINEITEMRecord]) extends FieldDef[Long](li, "L_RECEIPTDATE")
  case class LINEITEMRecord_Field_L_SHIPINSTRUCT(li: Rep[LINEITEMRecord]) extends FieldDef[Array[Byte]](li, "L_SHIPINSTRUCT")
  case class LINEITEMRecord_Field_L_SHIPMODE(li: Rep[LINEITEMRecord]) extends FieldDef[Array[Byte]](li, "L_SHIPMODE")
  case class LINEITEMRecord_Field_L_COMMENT(li: Rep[LINEITEMRecord]) extends FieldDef[Array[Byte]](li, "L_COMMENT")

  implicit class LINEITEMRecordRep2(self: Rep[LINEITEMRecord]) {
    def L_ORDERKEY: Rep[Int] = LINEITEMRecord_Field_L_ORDERKEY(self)
    def L_PARTKEY: Rep[Int] = LINEITEMRecord_Field_L_PARTKEY(self)
    def L_SUPPKEY: Rep[Int] = LINEITEMRecord_Field_L_SUPPKEY(self)
    def L_LINENUMBER: Rep[Int] = LINEITEMRecord_Field_L_LINENUMBER(self)
    def L_QUANTITY: Rep[Double] = LINEITEMRecord_Field_L_QUANTITY(self)
    def L_EXTENDEDPRICE: Rep[Double] = LINEITEMRecord_Field_L_EXTENDEDPRICE(self)
    def L_DISCOUNT: Rep[Double] = LINEITEMRecord_Field_L_DISCOUNT(self)
    def L_TAX: Rep[Double] = LINEITEMRecord_Field_L_TAX(self)
    def L_RETURNFLAG: Rep[Character] = LINEITEMRecord_Field_L_RETURNFLAG(self)
    def L_LINESTATUS: Rep[Character] = LINEITEMRecord_Field_L_LINESTATUS(self)
    def L_SHIPDATE: Rep[Long] = LINEITEMRecord_Field_L_SHIPDATE(self)
    def L_COMMITDATE: Rep[Long] = LINEITEMRecord_Field_L_COMMITDATE(self)
    def L_RECEIPTDATE: Rep[Long] = LINEITEMRecord_Field_L_RECEIPTDATE(self)
    def L_SHIPINSTRUCT: Rep[Array[Byte]] = LINEITEMRecord_Field_L_SHIPINSTRUCT(self)
    def L_SHIPMODE: Rep[Array[Byte]] = LINEITEMRecord_Field_L_SHIPMODE(self)
    def L_COMMENT: Rep[Array[Byte]] = LINEITEMRecord_Field_L_COMMENT(self)
  }

  // TODO auto generate this class

  case class GroupByClass_Field_L_RETURNFLAG(li: Rep[GroupByClass]) extends FieldDef[Character](li, "L_RETURNFLAG")
  case class GroupByClass_Field_L_LINESTATUS(li: Rep[GroupByClass]) extends FieldDef[Character](li, "L_LINESTATUS")

  implicit class GroupByClassRep(self: Rep[GroupByClass]) {
    def L_RETURNFLAG: Rep[Character] = GroupByClass_Field_L_RETURNFLAG(self)
    def L_LINESTATUS: Rep[Character] = GroupByClass_Field_L_LINESTATUS(self)
  }

  case class GroupByClass(val L_RETURNFLAG: java.lang.Character, val L_LINESTATUS: java.lang.Character);

  case class GroupByClassNew(L_RETURNFLAG: Rep[Character], L_LINESTATUS: Rep[Character]) extends FunctionDef[GroupByClass](None, "new GroupByClass", List(List(L_RETURNFLAG, L_LINESTATUS)))
  def groupByClassNew(L_RETURNFLAG: Rep[Character], L_LINESTATUS: Rep[Character]): Rep[GroupByClass] = GroupByClassNew(L_RETURNFLAG, L_LINESTATUS)

  // TODO auto generate these fields

  case class AGGRecord_Field_key[B](ar: Rep[AGGRecord[B]])(implicit manifestB: Manifest[B]) extends FieldDef[B](ar, "key")
  case class AGGRecord_Field_aggs[B](ar: Rep[AGGRecord[B]])(implicit manifestB: Manifest[B]) extends FieldDef[Array[Double]](ar, "aggs")

  // FIXME there's a bug here related to inner classes. Here QueryEngine.VolcanoPullEngine.AGGRecord[B] != AGGRecord[B] and for that we need this workaround

  implicit class AGGRecordRep2[B](self: Rep[queryengine.AGGRecord[B]])(implicit manifestB: Manifest[B]) {
    def key: Rep[B] = AGGRecord_Field_key(self.asInstanceOf[Rep[AGGRecord[B]]])
    def aggs: Rep[Array[Double]] = AGGRecord_Field_aggs(self.asInstanceOf[Rep[AGGRecord[B]]])
  }

  // FXIME handling default values (which needs macro or a compiler plugin)

  def __newPrintOp2[A](parent: Rep[Operator[A]])(printFunc: Rep[(A => Unit)], limit: Rep[(() => Boolean)] = {
    __lambda(() => unit(true))
  })(implicit manifestA: Manifest[A]): Rep[PrintOp[A]] = {
    __newPrintOp(parent)(printFunc, limit)
  }

  // TODO scala.Char class should be lifted instead of the java one

  case class Character$minus1(self: Rep[Character], x: Rep[Character]) extends FunctionDef[Int](Some(self), "-", List(List(x)))

  implicit class CharacterRep2(self: Rep[Character]) {
    def -(o: Rep[Character]): Rep[Int] = Character$minus1(self, o)
  }

  // TODO add variables to printop

  // TODO think about it, variables should be FieldAccess or FunctionDef or sth else?
  case class PrintOp_var_numRows[A](self: Rep[PrintOp[A]])(implicit manifestA: Manifest[A]) extends FieldDef[Int](self, "numRows")
  case class PrintOp_var_numRows_$equals[A](self: Rep[PrintOp[A]], v: Rep[Int])(implicit manifestA: Manifest[A]) extends FunctionDef[Unit](Some(self), "`numRows_=`", List(List(v)))
  case class PrintOp_Field_parent[A](self: Rep[PrintOp[A]])(implicit manifestA: Manifest[A]) extends FieldDef[Operator[A]](self, "parent")

  implicit class PrintOpRep2[A](self: Rep[PrintOp[A]])(implicit manifestA: Manifest[A]) {
    // val numRowsVar: Var[Int] = __newVar(PrintOp_var_numRows(self))
    // def numRows: Rep[Int] = readVar(numRowsVar)
    def numRows: Rep[Int] = PrintOp_var_numRows(self)
    def parent: Rep[Operator[A]] = printOp_Field_parent(self)
    def numRows_=(v: Rep[Int]): Rep[Unit] = printOpNumRows_$equals(self, v)
    // def printFunc: (Rep[A] => Rep[Unit]) = printOpPrintFunc(self)
    def printFunc: Rep[A => Unit] = printOpPrintFunc(self)
    def limit: Rep[() => Boolean] = printOpLimit(self)
  }

  def printOpNumRows_$equals[A](self: Rep[PrintOp[A]], v: Rep[Int])(implicit manifestA: Manifest[A]): Rep[Unit] = {
    // __assign(self.numRowsVar, v)
    PrintOp_var_numRows_$equals(self, v)
  }
  def printOp_Field_parent[A](self: Rep[PrintOp[A]])(implicit manifestA: Manifest[A]): Rep[Operator[A]] = PrintOp_Field_parent(self)
  def printOpPrintFunc[A](self: Rep[PrintOp[A]])(implicit manifestA: Manifest[A]): Rep[A => Unit] = self match {
    case Def(PrintOpNew(_, f, _)) => f
    case _                        => ???
  }
  def printOpLimit[A](self: Rep[PrintOp[A]])(implicit manifestA: Manifest[A]): Rep[() => Boolean] = self match {
    case Def(PrintOpNew(_, _, limit)) => limit
    case _                            => ???
  }

  case class SortOp_Field_parent[A](self: Rep[SortOp[A]])(implicit manifestA: Manifest[A]) extends FieldDef[Operator[A]](self, "parent")
  case class SortOp_Field_sortedTree[A](self: Rep[SortOp[A]])(implicit manifestA: Manifest[A]) extends FieldDef[TreeSet[A]](self, "sortedTree")

  implicit class SortOpRep2[A](self: Rep[SortOp[A]])(implicit manifestA: Manifest[A]) {
    def parent: Rep[Operator[A]] = sortOp_Field_parent(self)
    def sortedTree: Rep[TreeSet[A]] = sortOp_Field_sortedTree(self)
  }

  def sortOp_Field_parent[A](self: Rep[SortOp[A]])(implicit manifestA: Manifest[A]): Rep[Operator[A]] = SortOp_Field_parent(self)
  def sortOp_Field_sortedTree[A](self: Rep[SortOp[A]])(implicit manifestA: Manifest[A]): Rep[TreeSet[A]] = SortOp_Field_sortedTree(self)

  case class SelectOp_Field_parent[A](self: Rep[SelectOp[A]])(implicit manifestA: Manifest[A]) extends FieldDef[Operator[A]](self, "parent")
  case class SelectOp_Field_selectPred[A](self: Rep[SelectOp[A]])(implicit manifestA: Manifest[A]) extends FieldDef[Operator[A]](self, "parent")

  implicit class SelectOpRep2[A](self: Rep[SelectOp[A]])(implicit manifestA: Manifest[A]) {
    def parent: Rep[Operator[A]] = selectOp_Field_parent(self)
    def selectPred: Rep[A => Boolean] = selectOpSelectPred(self)
  }

  // surprisiningly it's a working solution!
  // TODO ideally it should be removed
  // case class InlinerLambda[T: Manifest, S: Manifest](inputSym: Rep[T], output: Block[S]) extends Function1[Rep[T], Rep[S]] {
  //   def apply(input: Rep[T]) = output match {
  //     case Block(stmts, res) => Block(Stm(inputSym.asInstanceOf[Sym[T]], ReadVal(input)) :: stmts, res)
  //   }
  // }

  def selectOp_Field_parent[A](self: Rep[SelectOp[A]])(implicit manifestA: Manifest[A]): Rep[Operator[A]] = SelectOp_Field_parent(self)
  def selectOpSelectPred[A](self: Rep[SelectOp[A]])(implicit manifestA: Manifest[A]): Rep[A => Boolean] = self match {
    case Def(SelectOpNew(_, f)) => f
    case _                      => ???
  }

  case class ScanOp_Field_table[A](self: Rep[ScanOp[A]])(implicit manifestA: Manifest[A]) extends FieldDef[Array[A]](self, "table")
  case class ScanOp_var_i[A](self: Rep[ScanOp[A]])(implicit manifestA: Manifest[A]) extends FieldDef[Int](self, "i")
  case class ScanOp_var_i_$equals[A](self: Rep[ScanOp[A]], v: Rep[Int])(implicit manifestA: Manifest[A]) extends FunctionDef[Unit](Some(self), "`i_=`", List(List(v)))

  implicit class ScanOpRep2[A](self: Rep[ScanOp[A]])(implicit manifestA: Manifest[A]) {
    def table: Rep[Array[A]] = scanOp_Field_table(self)
    def i: Rep[Int] = ScanOp_var_i(self)
    def i_=(v: Rep[Int]): Rep[Unit] = scanOpI_$equals(self, v)
  }

  def scanOp_Field_table[A](self: Rep[ScanOp[A]])(implicit manifestA: Manifest[A]): Rep[Array[A]] = ScanOp_Field_table(self)
  def scanOpI_$equals[A](self: Rep[ScanOp[A]], v: Rep[Int])(implicit manifestA: Manifest[A]): Rep[Unit] = {
    ScanOp_var_i_$equals(self, v)
  }

  case class MapOp_Field_parent[A](self: Rep[MapOp[A]])(implicit manifestA: Manifest[A]) extends FieldDef[Operator[A]](self, "parent")

  implicit class MapOpRep2[A](self: Rep[MapOp[A]])(implicit manifestA: Manifest[A]) {
    def parent: Rep[Operator[A]] = mapOp_Field_parent(self)
    def aggFuncs: Rep[Seq[A => Unit]] = mapOpAggFuncs(self)
  }

  def mapOp_Field_parent[A](self: Rep[MapOp[A]])(implicit manifestA: Manifest[A]): Rep[Operator[A]] = MapOp_Field_parent(self)
  def mapOpAggFuncs[A, B](self: Rep[MapOp[A]])(implicit manifestA: Manifest[A], manifestB: Manifest[B]): Rep[Seq[A => Unit]] = self match {
    case Def(MapOpNew(_, funs)) => funs
    case _                      => ???
  }

  case class AggOp_Field_parent[A, B](self: Rep[AggOp[A, B]])(implicit manifestA: Manifest[A], manifestB: Manifest[B]) extends FieldDef[Operator[A]](self, "parent")
  case class AggOp_Field_numAggs[A, B](self: Rep[AggOp[A, B]])(implicit manifestA: Manifest[A], manifestB: Manifest[B]) extends FieldDef[Int](self, "numAggs")
  case class AggOp_Field_hm[A, B](self: Rep[AggOp[A, B]])(implicit manifestA: Manifest[A], manifestB: Manifest[B]) extends FieldDef[HashMap[B, Array[Double]]](self, "hm")
  case class AggOp_Field_keySet[A, B](self: Rep[AggOp[A, B]])(implicit manifestA: Manifest[A], manifestB: Manifest[B]) extends FieldDef[Set[B]](self, "keySet")
  case class AggOpKeySet_$equals[A, B](self: Rep[AggOp[A, B]], v: Rep[Set[B]])(implicit manifestA: Manifest[A], manifestB: Manifest[B]) extends FunctionDef[Unit](Some(self), "`keySet_=`", List(List(v)))

  implicit class AggOpRep2[A, B](self: Rep[AggOp[A, B]])(implicit manifestA: Manifest[A], manifestB: Manifest[B]) {
    def parent: Rep[Operator[A]] = aggOp_Field_parent(self)(manifestA, manifestB)
    def numAggs: Rep[Int] = aggOp_Field_numAggs(self)(manifestA, manifestB)
    def grp: Rep[A => B] = aggOpGrp(self)(manifestA, manifestB)
    def hm: Rep[HashMap[B, Array[Double]]] = aggOp_Field_hm(self)
    def keySet: Rep[Set[B]] = aggOp_Field_keySet(self)
    def keySet_=(v: Rep[Set[B]]): Rep[Unit] = aggOpKeySet_$equals(self, v)
    def aggFuncs: Rep[Seq[((A, Double) => Double)]] = aggOpAggFuncs(self)
  }

  def aggOp_Field_parent[A, B](self: Rep[AggOp[A, B]])(implicit manifestA: Manifest[A], manifestB: Manifest[B]): Rep[Operator[A]] = AggOp_Field_parent(self)
  def aggOp_Field_numAggs[A, B](self: Rep[AggOp[A, B]])(implicit manifestA: Manifest[A], manifestB: Manifest[B]): Rep[Int] = AggOp_Field_numAggs(self)
  def aggOpGrp[A, B](self: Rep[AggOp[A, B]])(implicit manifestA: Manifest[A], manifestB: Manifest[B]): Rep[A => B] = self match {
    case Def(AggOpNew(_, _, f, _)) => f
    case _                         => ???
  }
  def aggOpAggFuncs[A, B](self: Rep[AggOp[A, B]])(implicit manifestA: Manifest[A], manifestB: Manifest[B]): Rep[Seq[((A, Double) => Double)]] = self match {
    case Def(AggOpNew(_, _, _, funs)) => funs
    case _                            => ???
  }
  def aggOp_Field_hm[A, B](self: Rep[AggOp[A, B]])(implicit manifestA: Manifest[A], manifestB: Manifest[B]): Rep[HashMap[B, Array[Double]]] = AggOp_Field_hm(self)
  def aggOp_Field_keySet[A, B](self: Rep[AggOp[A, B]])(implicit manifestA: Manifest[A], manifestB: Manifest[B]): Rep[Set[B]] = AggOp_Field_keySet(self)
  def aggOpKeySet_$equals[A, B](self: Rep[AggOp[A, B]], v: Rep[Set[B]])(implicit manifestA: Manifest[A], manifestB: Manifest[B]): Rep[Unit] = {
    AggOpKeySet_$equals(self, v)
  }

  def newAGGRecord[B](key: Rep[B], aggs: Rep[Array[Double]])(implicit manifestB: Manifest[B]): Rep[AGGRecord[B]] = aGGRecordNew[B](key, aggs)

  // TODO this thing should be removed, ideally every literal should be lifted using YY

  implicit def liftInt(i: scala.Int): Rep[Int] = unit(i)
}

// TODO should be generated automatically
trait OptionOps { this: DeepDSL =>
  implicit class OptionRep[A](self: Rep[Option[A]])(implicit manifestA: Manifest[A]) {
    def get(): Rep[A] = optionGet[A](self)(manifestA)
  }
  def optionGet[A](self: Rep[Option[A]])(implicit manifestA: Manifest[A]): Rep[A] = OptionGet[A](self)
  case class OptionGet[A](self: Rep[Option[A]])(implicit manifestA: Manifest[A]) extends FunctionDef[A](Some(self), "get", List())
}

trait ListOps { this: DeepDSL =>
  // def listForeach[A, U](self: Rep[List[A]], f: ((Rep[A]) => Rep[U]))(implicit manifestA: Manifest[A], manifestU: Manifest[U]): Rep[Unit] = {
  //   val fInput1 = fresh[A]
  //   val fOutput = reifyBlock(f(fInput1))
  //   ListForeach[A, U](self, fInput1, fOutput)
  // }
  // implicit class ListRep[A](self: Rep[List[A]])(implicit manifestA: Manifest[A]) {
  //   def foreach[U](f: (Rep[A] => Rep[U]))(implicit manifestU: Manifest[U]): Rep[Unit] = listForeach[A, U](self, f)(manifestA, manifestU)
  // }
  // case class ListForeach[A, U](self: Rep[List[A]], fInput1: Sym[A], fOutput: Block[U])(implicit manifestA: Manifest[A], manifestU: Manifest[U]) extends FunctionDef[Unit](Some(self), "foreach", List(List(Lambda(fInput1, fOutput))))
  // FIXME pure hack
  // case class LiftedList[A](underlying: List[Rep[A]])(implicit manifestA: Manifest[A]) extends FunctionDef[List[A]](None, "List", List(underlying))
  // def liftList[A](l: List[Rep[A]])(implicit manifestA: Manifest[A]): Rep[List[A]] = LiftedList(l)

  object Set {
    def apply[T: Manifest](seq: Rep[Seq[T]]): Rep[Set[T]] = SetNew(seq)
  }

  // case class SetNew[T: Manifest](seq: Rep[Seq[T]]) extends FunctionDef[Set[T]](None, "SetVarArg.apply", List(List(seq)))
  case class SetNew[T: Manifest](seq: Rep[Seq[T]]) extends FunctionDef[Set[T]](None, "Set", List(List(__varArg(seq))))
}
