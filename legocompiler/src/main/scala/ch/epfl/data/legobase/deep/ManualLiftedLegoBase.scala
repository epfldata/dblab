package ch.epfl.data
package legobase
package deep

import scala.language.implicitConversions

// FIXME in the righthand side of the genreated case class invokations, type parameters should be filled in.

trait ManualLiftedLegoBase extends OptionOps with SetOps { this: DeepDSL =>

  // TODO auto generate this functions

  case class LoadLineItem() extends FunctionDef[Array[LINEITEMRecord]](None, "loadLineitem", List(Nil))
  def loadLineitem(): Rep[Array[LINEITEMRecord]] = LoadLineItem()
  case class ParseDate(date: Rep[String]) extends FunctionDef[Long](None, "parseDate", List(List(date)))
  def parseDate(date: Rep[String]): Rep[Long] = ParseDate(date)
  case class Println(x: Rep[Any]) extends FunctionDef[Unit](None, "println", List(List(x)))
  def println(x: Rep[Any]): Rep[Unit] = Println(x)
  case class Printf(text: Rep[String], xs: Rep[Any]*) extends FunctionDef[Unit](None, "printf", List(text :: xs.toList))
  def printf(text: Rep[String], xs: Rep[Any]*): Rep[Unit] = Printf(text, xs: _*)

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

trait SetOps { this: DeepDSL =>
  object Set {
    def apply[T: Manifest](seq: Rep[Seq[T]]): Rep[Set[T]] = SetNew(seq)
  }
  case class SetNew[T: Manifest](seq: Rep[Seq[T]]) extends FunctionDef[Set[T]](None, "Set", List(List(__varArg(seq))))
}
