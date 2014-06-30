package ch.epfl.data
package legobase
package deep

import scala.language.implicitConversions

// FIXME in the righthand side of the genreated case class invokations, type parameters should be filled in.

trait ManualLiftedLegoBase { this: DeepDSL =>

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

  // TODO all new operators should be change like this

  def selectOpNew2[A](parent: Rep[Operator[A]])(selectPred: ((Rep[A]) => Rep[Boolean]))(implicit manifestA: Manifest[A]): Rep[SelectOp[A]] = {
    val selectPredInput1 = fresh[A]
    val selectPredOutput = reifyBlock(selectPred(selectPredInput1))
    SelectOpNew[A](parent, selectPredInput1, selectPredOutput)
  }

  def aggOpNew2[A, B](parent: Rep[Operator[A]], numAggs: Rep[Int])(grp: ((Rep[A]) => Rep[B]))(aggFuncs: ((Rep[A], Rep[Double]) => Rep[Double])*)(implicit manifestA: Manifest[A], manifestB: Manifest[B]): Rep[AggOp[A, B]] = {
    aggOpNew(parent, numAggs, grp, aggFuncs: _*)
  }

  def mapOpNew2[A](parent: Rep[Operator[A]])(aggFuncs: ((Rep[A]) => Rep[Unit])*)(implicit manifestA: Manifest[A]): Rep[MapOp[A]] = MapOpNew(parent, aggFuncs: _*)

  def sortOpNew2[A](parent: Rep[Operator[A]])(orderingFunc: ((Rep[A], Rep[A]) => Rep[Int]))(implicit manifestA: Manifest[A]): Rep[SortOp[A]] = {
    sortOpNew(parent, orderingFunc)
  }

  // FXIME handling default values (which needs macro or a compiler plugin)

  def printOpNew2[A](parent: Rep[Operator[A]])(printFunc: ((Rep[A]) => Rep[Unit]), limit: (() => Rep[Boolean]) = () => unit(true))(implicit manifestA: Manifest[A]): Rep[PrintOp[A]] = {
    printOpNew(parent, printFunc, limit)
  }

  // TODO scala.Char class should be lifted instead of the java one

  case class Character$minus1(self: Rep[Character], x: Rep[Character]) extends FunctionDef[Int](Some(self), "-", List(List(x)))

  implicit class CharacterRep2(self: Rep[Character]) {
    def -(o: Rep[Character]): Rep[Int] = Character$minus1(self, o)
  }

  // TODO add variables to printop

  // TODO think about it, variables should be FieldAccess or FunctionDef or sth else?
  case class PrintOp_var_numRows[A](self: Rep[PrintOp[A]])(implicit manifestA: Manifest[A]) extends FieldDef[Int](self, "numRows")
  case class PrintOp_Field_parent[A](self: Rep[PrintOp[A]])(implicit manifestA: Manifest[A]) extends FieldDef[Operator[A]](self, "parent")

  implicit class PrintOpRep2[A](self: Rep[PrintOp[A]])(implicit manifestA: Manifest[A]) {
    def numRows: Rep[Int] = PrintOp_var_numRows(self)
    def parent: Rep[Operator[A]] = printOp_Field_parent(self)
  }

  def printOp_Field_parent[A](self: Rep[PrintOp[A]])(implicit manifestA: Manifest[A]): Rep[Operator[A]] = PrintOp_Field_parent(self)

  case class SortOp_Field_parent[A](self: Rep[SortOp[A]])(implicit manifestA: Manifest[A]) extends FieldDef[Operator[A]](self, "parent")

  implicit class SortOpRep2[A](self: Rep[SortOp[A]])(implicit manifestA: Manifest[A]) {
    def parent: Rep[Operator[A]] = sortOp_Field_parent(self)
  }

  def sortOp_Field_parent[A](self: Rep[SortOp[A]])(implicit manifestA: Manifest[A]): Rep[Operator[A]] = SortOp_Field_parent(self)

  case class MapOp_Field_parent[A](self: Rep[MapOp[A]])(implicit manifestA: Manifest[A]) extends FieldDef[Operator[A]](self, "parent")

  implicit class MapOpRep2[A](self: Rep[MapOp[A]])(implicit manifestA: Manifest[A]) {
    def parent: Rep[Operator[A]] = mapOp_Field_parent(self)
  }

  def mapOp_Field_parent[A](self: Rep[MapOp[A]])(implicit manifestA: Manifest[A]): Rep[Operator[A]] = MapOp_Field_parent(self)

  case class AggOp_Field_parent[A, B](self: Rep[AggOp[A, B]])(implicit manifestA: Manifest[A], manifestB: Manifest[B]) extends FieldDef[Operator[A]](self, "parent")

  implicit class AggOpRep2[A, B](self: Rep[AggOp[A, B]])(implicit manifestA: Manifest[A], manifestB: Manifest[B], evidence$6: Manifest[B], evidence$5: Manifest[A]) {
    def parent: Rep[Operator[A]] = aggOp_Field_parent(self)(manifestA, manifestB)
  }

  def aggOp_Field_parent[A, B](self: Rep[AggOp[A, B]])(implicit manifestA: Manifest[A], manifestB: Manifest[B]): Rep[Operator[A]] = AggOp_Field_parent(self)

  // TODO this thing should be removed, ideally every literal should be lifted using YY

  implicit def liftInt(i: scala.Int): Rep[Int] = unit(i)
}
