package ch.epfl.data
package legobase
package deep

import scala.language.implicitConversions
import pardis.utils.Utils.{ manifestToString => m2s }

// FIXME in the righthand side of the genreated case class invokations, type parameters should be filled in.

trait ManualLiftedLegoBase extends OptionOps with SetOps with OrderingOps with ManifestOps { this: DeepDSL =>

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

  type Char = Character

  def newAGGRecord[B](key: Rep[B], aggs: Rep[Array[Double]])(implicit manifestB: Manifest[B]): Rep[AGGRecord[B]] = aGGRecordNew[B](key, aggs)

  // TODO this thing should be removed, ideally every literal should be lifted using YY

  implicit def liftInt(i: scala.Int): Rep[Int] = unit(i)

  def __newTreeSet2[A](ordering: Rep[Ordering[A]])(implicit manifestA: Manifest[A]): Rep[TreeSet[A]] = TreeSetNew2[A](ordering)(manifestA)
  // case classes
  case class TreeSetNew2[A](val ordering: Rep[Ordering[A]])(implicit val manifestA: Manifest[A]) extends FunctionDef[TreeSet[A]](None, "new TreeSet", List(Nil, List(ordering)))

  // constructors
  override def arrayNew[T](_length: Rep[Int])(implicit manifestT: Manifest[T]): Rep[Array[T]] = ArrayNew2[T](_length)(manifestT)

  // case classes
  case class ArrayNew2[T](_length: Rep[Int])(implicit val manifestT: Manifest[T]) extends FunctionDef[Array[T]](None, s"new Array[${m2s(manifestT)}]", List(List(_length)))

  implicit class ArrayRep2[T](self: Rep[Array[T]])(implicit manifestT: Manifest[T]) {
    def filter(p: Rep[T => Boolean]): Rep[Array[T]] = arrayFilter(self, p)
  }

  def arrayFilter[T](self: Rep[Array[T]], p: Rep[T => Boolean])(implicit manifestT: Manifest[T]): Rep[Array[T]] = ArrayFilter[T](self, p)

  case class ArrayFilter[T](self: Rep[Array[T]], p: Rep[T => Boolean])(implicit val manifestT: Manifest[T]) extends FunctionDef[Array[T]](Some(self), "filter", List(List(p)))

  override def hashMapNew2[A, B](implicit manifestA: Manifest[A], manifestB: Manifest[B]): Rep[HashMap[A, B]] = HashMapNew2_2[A, B]()

  case class HashMapNew2_2[A, B]()(implicit val manifestA: Manifest[A], val manifestB: Manifest[B]) extends FunctionDef[HashMap[A, B]](None, s"new HashMap[${m2s(manifestA)}, ${m2s(manifestB)}]", List())
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
    def apply[T: Manifest](): Rep[Set[T]] = SetNew2[T]()(manifest[T])
  }
  case class SetNew[T: Manifest](seq: Rep[Seq[T]]) extends FunctionDef[Set[T]](None, "Set", List(List(__varArg(seq))))
  case class SetNew2[T: Manifest]() extends FunctionDef[Set[T]](None, s"Set[${m2s(manifest[T])}]", List(List()))
}

trait OrderingOps { this: DeepDSL =>
  object Ordering {
    def apply[T: Manifest](comp: Rep[Function2[T, T, Int]]): Rep[Ordering[T]] = OrderingNew(comp)
  }
  case class OrderingNew[T: Manifest](comp: Rep[Function2[T, T, Int]]) extends FunctionDef[Ordering[T]](None, "OrderingFactory", List(List(comp)))
}

trait ManifestOps { this: DeepDSL =>
  object ManifestRep {
    def apply[T: Manifest](man: Manifest[T]): Rep[Manifest[T]] = ManifestNew[T](man)(man)
  }

  case class ManifestNew[T](man: Manifest[T])(implicit manifestT: Manifest[T]) extends FunctionDef[Manifest[T]](None, s"manifest[${m2s(man)}]", Nil)
}
