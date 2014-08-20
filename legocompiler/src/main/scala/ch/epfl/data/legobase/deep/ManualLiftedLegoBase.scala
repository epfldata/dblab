package ch.epfl.data
package legobase
package deep

import scala.reflect.runtime.universe.{ typeTag => tag }
import scala.language.implicitConversions
import pardis.utils.Utils.{ pardisTypeToString => t2s }
import pardis.shallow.AbstractRecord
import pardis.shallow.CaseClassRecord
import pardis.ir.pardisTypeImplicits._

trait ManualLiftedLegoBase extends OptionOps with SetOps with OrderingOps with ManifestOps with IntPE { this: DeepDSL =>

  /* TODO These methods should be lifted from storagemanager.Loader */
  case class LoadLineItem() extends FunctionDef[Array[LINEITEMRecord]](None, "loadLineitem", List(Nil)) {
    override def curriedConstructor = (x: Any) => copy()
  }
  def loadLineitem(): Rep[Array[LINEITEMRecord]] = LoadLineItem()
  case class LoadPart() extends FunctionDef[Array[PARTRecord]](None, "loadPart", List(Nil)) {
    override def curriedConstructor = (x: Any) => copy()
  }
  def loadPart(): Rep[Array[PARTRecord]] = LoadPart()
  case class LoadPartsupp() extends FunctionDef[Array[PARTSUPPRecord]](None, "loadPartsupp", List(Nil)) {
    override def curriedConstructor = (x: Any) => copy()
  }
  def loadPartsupp(): Rep[Array[PARTSUPPRecord]] = LoadPartsupp()
  case class LoadNation() extends FunctionDef[Array[NATIONRecord]](None, "loadNation", List(Nil)) {
    override def curriedConstructor = (x: Any) => copy()
  }
  def loadNation(): Rep[Array[NATIONRecord]] = LoadNation()

  case class LoadRegion() extends FunctionDef[Array[REGIONRecord]](None, "loadRegion", List(Nil)) {
    override def curriedConstructor = (x: Any) => copy()
  }
  def loadRegion(): Rep[Array[REGIONRecord]] = LoadRegion()

  case class LoadSupplier() extends FunctionDef[Array[SUPPLIERRecord]](None, "loadSupplier", List(Nil)) {
    override def curriedConstructor = (x: Any) => copy()
  }
  def loadSupplier(): Rep[Array[SUPPLIERRecord]] = LoadSupplier()

  case class LoadOrders() extends FunctionDef[Array[ORDERSRecord]](None, "loadOrders", List(Nil)) {
    override def curriedConstructor = (x: Any) => copy()
  }
  def loadOrders(): Rep[Array[ORDERSRecord]] = LoadOrders()
  case class LoadCustomer() extends FunctionDef[Array[CUSTOMERRecord]](None, "loadCustomer", List(Nil)) {
    override def curriedConstructor = (x: Any) => copy()
  }
  def loadCustomer(): Rep[Array[CUSTOMERRecord]] = LoadCustomer()

  case class FileLineCount(file: Rep[String]) extends FunctionDef[Int](None, "fileLineCount", List(List(file))) {
    override def curriedConstructor = (copy _)
  }
  def fileLineCount(file: Rep[String]): Rep[Int] = FileLineCount(file)

  /* These methods should be lifted from Legobase.scala */
  case class ParseDate(date: Rep[String]) extends FunctionDef[Long](None, "parseDate", List(List(date))) {
    override def curriedConstructor = (copy _)
  }
  def parseDate(date: Rep[String]): Rep[Long] = ParseDate(date)
  case class ParseString(string: Rep[String]) extends FunctionDef[LBString](None, "parseString", List(List(string))) {
    override def curriedConstructor = (copy _)
  }
  def parseString(string: Rep[String]): Rep[LBString] = ParseString(string)

  /* TODO These methods should be lifted from scala.Predef */
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

  /* TODO This method should be lifted from Queries.scala */
  def runQuery[T: TypeRep](query: => Rep[T]): Rep[T] = {
    val b = reifyBlock(query)
    RunQuery(b)
  }
  case class RunQuery[T: TypeRep](query: Block[T]) extends FunctionDef[T](None, "runQuery", List(List(query))) {
    override def curriedConstructor = (copy[T] _)
  }
  def dateToString(long: Rep[Long]): Rep[String] = DateToString(long)
  case class DateToString(long: Rep[Long]) extends FunctionDef[String](None, "dateToString", List(List(long))) {
    override def curriedConstructor = (copy _)
  }

  // FIXME handling default values (which needs macro or a compiler plugin)
  def __newPrintOp2[A](parent: Rep[Operator[A]])(printFunc: Rep[(A => Unit)], limit: Rep[(() => Boolean)] = {
    __lambda(() => unit(true))
  })(implicit evidence$9: Manifest[A], typeA: TypeRep[A]): Rep[PrintOp[A]] = {
    __newPrintOp(parent)(printFunc, limit)
  }

  def __newHashJoinOp2[A <: AbstractRecord, B <: AbstractRecord, C](leftParent: Rep[Operator[A]], rightParent: Rep[Operator[B]], leftAlias: Rep[String] = unit(""), rightAlias: Rep[String] = unit(""))(joinCond: Rep[((A, B) => Boolean)])(leftHash: Rep[(A => C)])(rightHash: Rep[(B => C)])(implicit evidence$10: Manifest[A], evidence$11: Manifest[B], evidence$12: Manifest[C], typeA: TypeRep[A], typeB: TypeRep[B], typeC: TypeRep[C]): Rep[HashJoinOp[A, B, C]] = __newHashJoinOp[A, B, C](leftParent, rightParent, leftAlias, rightAlias)(joinCond)(leftHash)(rightHash)

  // TODO scala.Char class should be lifted instead of the java one
  case class Character$minus1(self: Rep[Character], x: Rep[Character]) extends FunctionDef[Int](Some(self), "-", List(List(x))) {
    override def curriedConstructor = (copy _).curried
  }

  implicit class CharacterRep2(self: Rep[Character]) {
    def -(o: Rep[Character]): Rep[Int] = Character$minus1(self, o)
  }

  type Char = Character

  /* TODO should be automatically generated from GenericEngine */
  object GenericEngine {
    def newAGGRecord[B](key: Rep[B], aggs: Rep[Array[Double]])(implicit typeB: TypeRep[B]): Rep[AGGRecord[B]] = aGGRecordNew[B](key, aggs)
    def newWindowRecord[B, C](key: Rep[B], wnd: Rep[C])(implicit typeB: TypeRep[B], typeC: TypeRep[C]): Rep[WindowRecord[B, C]] = __newWindowRecord(key, wnd)
  }

  // TODO this thing should be removed, ideally every literal should be lifted using YY
  implicit def liftInt(i: scala.Int): Rep[Int] = unit(i)

  // TODO implicit parameters should be handled in a correct way to remove this manually lifted one
  def __newTreeSet2[A](ordering: Rep[Ordering[A]])(implicit typeA: TypeRep[A]): Rep[TreeSet[A]] = TreeSetNew2[A](ordering)(typeA)
  // case classes
  case class TreeSetNew2[A](val ordering: Rep[Ordering[A]])(implicit val typeA: TypeRep[A]) extends FunctionDef[TreeSet[A]](None, "new TreeSet", List(Nil, List(ordering))) {
    override def curriedConstructor = (copy[A] _)
  }

  // this one is needed to rewrire `ArrayBuffer.apply()` to `new ArrayBuffer()`
  override def arrayBufferApplyObject[T]()(implicit typeT: TypeRep[T]): Rep[ArrayBuffer[T]] = __newArrayBuffer[T]()

  /* TODO should be automatically generated with HashMap */
  case class ContentsType[T, S](typeT: TypeRep[T], typeS: TypeRep[S]) extends TypeRep[Contents[T, S]] {
    def rebuild(newArguments: TypeRep[_]*): TypeRep[_] = ContentsType(newArguments(0), newArguments(1))
    private implicit val tagT = typeT.typeTag
    private implicit val tagS = typeS.typeTag
    val name = s"Contents[${typeT.name}, ${typeS.name}]"
    val typeArguments = List(typeT, typeS)
    val typeTag = tag[Contents[T, S]]
  }
  implicit def typeContents[T: TypeRep, S: TypeRep] = ContentsType[T, S](implicitly[TypeRep[T]], implicitly[TypeRep[S]])

  /* TODO there's a bug with the design of records which if it's solved there's no need for this manual node */
  override def aGGRecordNew[B](key: Rep[B], aggs: Rep[Array[Double]])(implicit typeB: TypeRep[B]): Rep[AGGRecord[B]] = AGGRecordNew2[B](key, aggs)
  case class AGGRecordNew2[B](key: Rep[B], aggs: Rep[Array[Double]])(implicit val typeB: TypeRep[B]) extends ConstructorDef[AGGRecord[B]](List(), "AGGRecord", List(List(key, aggs))) {
    override def curriedConstructor = (copy[B] _).curried
  }
}

// TODO should be generated automatically
trait OptionOps { this: DeepDSL =>
  implicit class OptionRep[A](self: Rep[Option[A]])(implicit typeA: TypeRep[A]) {
    def get(): Rep[A] = optionGet[A](self)(typeA)
  }
  def optionGet[A](self: Rep[Option[A]])(implicit typeA: TypeRep[A]): Rep[A] = OptionGet[A](self)
  case class OptionGet[A](self: Rep[Option[A]])(implicit typeA: TypeRep[A]) extends FunctionDef[A](Some(self), "get", List()) {
    override def curriedConstructor = copy[A] _
  }
}

trait SetOps extends scalalib.SetOps { this: DeepDSL =>
  case class SetNew[T: TypeRep](seq: Rep[Seq[T]]) extends FunctionDef[Set[T]](None, "Set", List(List(__varArg(seq)))) {
    override def curriedConstructor = copy[T] _
  }

  // These are needed for rewiring `Set.apply()` to `new Set()`
  // TODO if YinYang virtualizes `:_*` correctly there's no need to lift this one manually,
  // only the reflected one should be changed to var arg one.
  override def setApplyObject1[T](seq: Rep[Seq[T]])(implicit typeT: TypeRep[T]): Rep[Set[T]] = SetNew[T](seq)
  // TODO as Set doesn't have a constructor they should be manually lifted
  override def setApplyObject2[T]()(implicit typeT: TypeRep[T]): Rep[Set[T]] = SetNew2[T]()
  case class SetNew2[T: TypeRep]() extends FunctionDef[Set[T]](None, s"Set[${t2s(implicitly[TypeRep[T]])}]", List(List())) {
    override def curriedConstructor = (x: Any) => copy[T]()
  }
}

// TODO needs lifting type classes
trait OrderingOps { this: DeepDSL =>
  object Ordering {
    def apply[T: TypeRep](comp: Rep[Function2[T, T, Int]]): Rep[Ordering[T]] = OrderingNew(comp)
  }
  case class OrderingNew[T: TypeRep](comp: Rep[Function2[T, T, Int]]) extends FunctionDef[Ordering[T]](None, "OrderingFactory", List(List(comp))) {
    override def curriedConstructor = copy[T] _
  }
}

// TODO needs lifting type classes
trait ManifestOps { this: DeepDSL =>
  object ManifestRep {
    def apply[T: TypeRep](man: Manifest[T]): Rep[Manifest[T]] = ManifestNew[T](man)
  }

  case class ManifestNew[T](man: Manifest[T])(implicit typeT: TypeRep[T]) extends FunctionDef[Manifest[T]](None, s"manifest[${t2s(typeT)}]", Nil) {
    override def curriedConstructor = copy[T] _
  }
}

// TODO needs generation of partial evaluation for every node
trait IntPE extends pardis.deep.scalalib.IntOps { this: DeepDSL =>
  case class Int$plus5PE(self: Rep[Int], x: Rep[Int]) extends FunctionDef[Int](Some(self), "+", List(List(x))) {
    override def isPure = true
    override def partialEvaluable: Boolean = true
    override def partialEvaluate(children: Any*): Int = children(0).asInstanceOf[Int] + children(1).asInstanceOf[Int]
    override def curriedConstructor = (copy _).curried
  }
  override def int$plus5(self: Rep[Int], x: Rep[Int]): Rep[Int] = Int$plus5PE(self, x)
}
