package ch.epfl.data
package legobase
package deep

import scala.language.implicitConversions
import pardis.utils.Utils.{ manifestToString => m2s }

// FIXME in the righthand side of the genreated case class invokations, type parameters should be filled in.

trait ManualLiftedLegoBase extends OptionOps with SetOps with OrderingOps with ManifestOps with IntPE { this: DeepDSL =>

  // TODO auto generate this functions

  case class LoadLineItem() extends FunctionDef[Array[LINEITEMRecord]](None, "loadLineitem", List(Nil)) {
    override def curriedConstructor = (x: Any) => copy()
  }
  def loadLineitem(): Rep[Array[LINEITEMRecord]] = LoadLineItem()
  case class ParseDate(date: Rep[String]) extends FunctionDef[Long](None, "parseDate", List(List(date))) {
    override def curriedConstructor = (copy _)
  }
  def parseDate(date: Rep[String]): Rep[Long] = ParseDate(date)
  case class Println(x: Rep[Any]) extends FunctionDef[Unit](None, "println", List(List(x))) {
    override def curriedConstructor = (copy _)
  }
  def println(x: Rep[Any]): Rep[Unit] = Println(x)
  // for performance reasons printf is written like this:
  case class Printf(text: Rep[String], xs: Rep[Any]*) extends FunctionDef[Unit](None, "printf", List(text :: xs.toList)) {
    override def rebuild(children: FunctionArg*) = Printf(children(0).asInstanceOf[Rep[String]], children.drop(1).toSeq.asInstanceOf[Seq[Rep[Any]]]: _*)
  }
  def printf(text: Rep[String], xs: Rep[Any]*): Rep[Unit] = Printf(text, xs: _*)
  // printf is not written like this for the reason mentioned above
  // case class Printf(text: Rep[String], xs: Rep[Seq[Any]]) extends FunctionDef[Unit](None, "printf", List(List(text, __varArg(xs)))) {
  //   override def curriedConstructor = (copy _).curried
  // }
  // def printf(text: Rep[String], xs: Rep[Any]*): Rep[Unit] = Printf(text, __liftSeq(xs.toSeq))
  def runQuery[T: Manifest](query: => Rep[T]): Rep[T] = {
    val b = reifyBlock(query)
    RunQuery(b)
  }
  case class RunQuery[T: Manifest](query: Block[T]) extends FunctionDef[T](None, "runQuery", List(List(query))) {
    override def curriedConstructor = (copy[T] _)
  }

  // TODO auto generate this class

  case class GroupByClass_Field_L_RETURNFLAG(li: Rep[GroupByClass]) extends FieldDef[Character](li, "L_RETURNFLAG") {
    override def curriedConstructor = (copy _)
  }
  case class GroupByClass_Field_L_LINESTATUS(li: Rep[GroupByClass]) extends FieldDef[Character](li, "L_LINESTATUS") {
    override def curriedConstructor = (copy _)
  }

  implicit class GroupByClassRep(self: Rep[GroupByClass]) {
    def L_RETURNFLAG: Rep[Character] = GroupByClass_Field_L_RETURNFLAG(self)
    def L_LINESTATUS: Rep[Character] = GroupByClass_Field_L_LINESTATUS(self)
  }

  case class GroupByClass(val L_RETURNFLAG: java.lang.Character, val L_LINESTATUS: java.lang.Character);

  case class GroupByClassNew(L_RETURNFLAG: Rep[Character], L_LINESTATUS: Rep[Character]) extends FunctionDef[GroupByClass](None, "new GroupByClass", List(List(L_RETURNFLAG, L_LINESTATUS))) {
    override def curriedConstructor = (copy _).curried
  }
  def groupByClassNew(L_RETURNFLAG: Rep[Character], L_LINESTATUS: Rep[Character]): Rep[GroupByClass] = GroupByClassNew(L_RETURNFLAG, L_LINESTATUS)

  // FXIME handling default values (which needs macro or a compiler plugin)

  def __newPrintOp2[A](parent: Rep[Operator[A]])(printFunc: Rep[(A => Unit)], limit: Rep[(() => Boolean)] = {
    __lambda(() => unit(true))
  })(implicit manifestA: Manifest[A]): Rep[PrintOp[A]] = {
    __newPrintOp(parent)(printFunc, limit)
  }

  // TODO scala.Char class should be lifted instead of the java one

  case class Character$minus1(self: Rep[Character], x: Rep[Character]) extends FunctionDef[Int](Some(self), "-", List(List(x))) {
    override def curriedConstructor = (copy _).curried
  }

  implicit class CharacterRep2(self: Rep[Character]) {
    def -(o: Rep[Character]): Rep[Int] = Character$minus1(self, o)
  }

  type Char = Character

  def newAGGRecord[B](key: Rep[B], aggs: Rep[Array[Double]])(implicit manifestB: Manifest[B]): Rep[AGGRecord[B]] = aGGRecordNew[B](key, aggs)

  // TODO this thing should be removed, ideally every literal should be lifted using YY

  implicit def liftInt(i: scala.Int): Rep[Int] = unit(i)

  def __newTreeSet2[A](ordering: Rep[Ordering[A]])(implicit manifestA: Manifest[A]): Rep[TreeSet[A]] = TreeSetNew2[A](ordering)(manifestA)
  // case classes
  case class TreeSetNew2[A](val ordering: Rep[Ordering[A]])(implicit val manifestA: Manifest[A]) extends FunctionDef[TreeSet[A]](None, "new TreeSet", List(Nil, List(ordering))) {
    override def curriedConstructor = (copy[A] _)
  }

  // constructors
  override def arrayNew[T](_length: Rep[Int])(implicit manifestT: Manifest[T]): Rep[Array[T]] = ArrayNew2[T](_length)(manifestT)

  // case classes
  case class ArrayNew2[T](_length: Rep[Int])(implicit val manifestT: Manifest[T]) extends FunctionDef[Array[T]](None, s"new Array[${m2s(manifestT)}]", List(List(_length))) {
    override def curriedConstructor = (copy[T] _)
  }

  implicit class ArrayRep2[T](self: Rep[Array[T]])(implicit manifestT: Manifest[T]) {
    def filter(p: Rep[T => Boolean]): Rep[Array[T]] = arrayFilter(self, p)
  }

  def arrayFilter[T](self: Rep[Array[T]], p: Rep[T => Boolean])(implicit manifestT: Manifest[T]): Rep[Array[T]] = ArrayFilter[T](self, p)

  case class ArrayFilter[T](self: Rep[Array[T]], p: Rep[T => Boolean])(implicit val manifestT: Manifest[T]) extends FunctionDef[Array[T]](Some(self), "filter", List(List(p))) {
    override def curriedConstructor = (copy[T] _).curried
  }

  override def hashMapNew2[A, B](implicit manifestA: Manifest[A], manifestB: Manifest[B]): Rep[HashMap[A, B]] = HashMapNew2_2[A, B]()

  case class HashMapNew2_2[A, B]()(implicit val manifestA: Manifest[A], val manifestB: Manifest[B]) extends FunctionDef[HashMap[A, B]](None, s"new HashMap[${m2s(manifestA)}, ${m2s(manifestB)}]", List()) {
    override def curriedConstructor = (x: Any) => copy[A, B]()
  }
}

// TODO should be generated automatically
trait OptionOps { this: DeepDSL =>
  implicit class OptionRep[A](self: Rep[Option[A]])(implicit manifestA: Manifest[A]) {
    def get(): Rep[A] = optionGet[A](self)(manifestA)
  }
  def optionGet[A](self: Rep[Option[A]])(implicit manifestA: Manifest[A]): Rep[A] = OptionGet[A](self)
  case class OptionGet[A](self: Rep[Option[A]])(implicit manifestA: Manifest[A]) extends FunctionDef[A](Some(self), "get", List()) {
    override def curriedConstructor = copy[A] _
  }
}

trait SetOps { this: DeepDSL =>
  object Set {
    def apply[T: Manifest](seq: Rep[Seq[T]]): Rep[Set[T]] = SetNew(seq)
    def apply[T: Manifest](): Rep[Set[T]] = SetNew2[T]()(manifest[T])
  }
  case class SetNew[T: Manifest](seq: Rep[Seq[T]]) extends FunctionDef[Set[T]](None, "Set", List(List(__varArg(seq)))) {
    override def curriedConstructor = copy[T] _
  }
  case class SetNew2[T: Manifest]() extends FunctionDef[Set[T]](None, s"Set[${m2s(manifest[T])}]", List(List())) {
    override def curriedConstructor = (x: Any) => copy[T]()
  }
}

trait OrderingOps { this: DeepDSL =>
  object Ordering {
    def apply[T: Manifest](comp: Rep[Function2[T, T, Int]]): Rep[Ordering[T]] = OrderingNew(comp)
  }
  case class OrderingNew[T: Manifest](comp: Rep[Function2[T, T, Int]]) extends FunctionDef[Ordering[T]](None, "OrderingFactory", List(List(comp))) {
    override def curriedConstructor = copy[T] _
  }
}

trait ManifestOps { this: DeepDSL =>
  object ManifestRep {
    def apply[T: Manifest](man: Manifest[T]): Rep[Manifest[T]] = ManifestNew[T](man)(man)
  }

  case class ManifestNew[T](man: Manifest[T])(implicit manifestT: Manifest[T]) extends FunctionDef[Manifest[T]](None, s"manifest[${m2s(man)}]", Nil) {
    override def curriedConstructor = copy[T] _
  }
}

trait IntPE extends scalalib.IntOps { this: DeepDSL =>
  case class Int$plus5PE(self: Rep[Int], x: Rep[Int]) extends FunctionDef[Int](Some(self), "+", List(List(x))) {
    override def isPure = true
    override def partialEvaluable: Boolean = true
    override def partialEvaluate(children: Any*): Int = children(0).asInstanceOf[Int] + children(1).asInstanceOf[Int]
    override def curriedConstructor = (copy _).curried
  }
  override def int$plus5(self: Rep[Int], x: Rep[Int]): Rep[Int] = Int$plus5PE(self, x)
}
