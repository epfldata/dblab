/* Generated by AutoLifter © 2014 */

package ch.epfl.data
package legobase
package deep
package scalalib

import pardis.ir._

trait HashMapOps extends Base { this: DeepDSL =>
  implicit class HashMapRep[A, B](self: Rep[HashMap[A, B]])(implicit manifestA: Manifest[A], manifestB: Manifest[B]) {
    def getOrElseUpdate(key: Rep[A], op: => Rep[B]): Rep[B] = hashMapGetOrElseUpdate[A, B](self, key, op)(manifestA, manifestB)
    def clear(): Rep[Unit] = hashMapClear[A, B](self)(manifestA, manifestB)
    def size(): Rep[Int] = hashMapSize[A, B](self)(manifestA, manifestB)
    def contains(key: Rep[A]): Rep[Boolean] = hashMapContains[A, B](self, key)(manifestA, manifestB)
    def apply(key: Rep[A]): Rep[B] = hashMapApply[A, B](self, key)(manifestA, manifestB)
    def update(key: Rep[A], value: Rep[B]): Rep[Unit] = hashMapUpdate[A, B](self, key, value)(manifestA, manifestB)
    def remove(key: Rep[A]): Rep[Option[B]] = hashMapRemove[A, B](self, key)(manifestA, manifestB)
    def keySet(): Rep[Set[A]] = hashMapKeySet[A, B](self)(manifestA, manifestB)
    def contents(): Rep[Contents[A, DefaultEntry[A, B]]] = hashMap_Field_Contents[A, B](self)(manifestA, manifestB)
  }
  // constructors
  def __newHashMap[A, B](contents: Rep[Contents[A, DefaultEntry[A, B]]])(implicit overload1: Overloaded1, manifestA: Manifest[A], manifestB: Manifest[B]): Rep[HashMap[A, B]] = hashMapNew1[A, B](contents)(manifestA, manifestB)
  def __newHashMap[A, B](implicit overload2: Overloaded2, manifestA: Manifest[A], manifestB: Manifest[B]): Rep[HashMap[A, B]] = hashMapNew2[A, B](manifestA, manifestB)
  // case classes
  case class HashMapNew1[A, B](contents: Rep[Contents[A, DefaultEntry[A, B]]])(implicit val manifestA: Manifest[A], val manifestB: Manifest[B]) extends FunctionDef[HashMap[A, B]](None, "new HashMap", List(List(contents)))
  case class HashMapNew2[A, B]()(implicit val manifestA: Manifest[A], val manifestB: Manifest[B]) extends FunctionDef[HashMap[A, B]](None, "new HashMap", List())
  case class HashMapGetOrElseUpdate[A, B](self: Rep[HashMap[A, B]], key: Rep[A], opOutput: Block[B])(implicit val manifestA: Manifest[A], val manifestB: Manifest[B]) extends FunctionDef[B](Some(self), "getOrElseUpdate", List(List(key, opOutput)))
  case class HashMapClear[A, B](self: Rep[HashMap[A, B]])(implicit val manifestA: Manifest[A], val manifestB: Manifest[B]) extends FunctionDef[Unit](Some(self), "clear", List())
  case class HashMapSize[A, B](self: Rep[HashMap[A, B]])(implicit val manifestA: Manifest[A], val manifestB: Manifest[B]) extends FunctionDef[Int](Some(self), "size", List())
  case class HashMapContains[A, B](self: Rep[HashMap[A, B]], key: Rep[A])(implicit val manifestA: Manifest[A], val manifestB: Manifest[B]) extends FunctionDef[Boolean](Some(self), "contains", List(List(key)))
  case class HashMapApply[A, B](self: Rep[HashMap[A, B]], key: Rep[A])(implicit val manifestA: Manifest[A], val manifestB: Manifest[B]) extends FunctionDef[B](Some(self), "apply", List(List(key)))
  case class HashMapUpdate[A, B](self: Rep[HashMap[A, B]], key: Rep[A], value: Rep[B])(implicit val manifestA: Manifest[A], val manifestB: Manifest[B]) extends FunctionDef[Unit](Some(self), "update", List(List(key, value)))
  case class HashMapRemove[A, B](self: Rep[HashMap[A, B]], key: Rep[A])(implicit val manifestA: Manifest[A], val manifestB: Manifest[B]) extends FunctionDef[Option[B]](Some(self), "remove", List(List(key)))
  case class HashMapKeySet[A, B](self: Rep[HashMap[A, B]])(implicit val manifestA: Manifest[A], val manifestB: Manifest[B]) extends FunctionDef[Set[A]](Some(self), "keySet", List())
  case class HashMap_Field_Contents[A, B](self: Rep[HashMap[A, B]])(implicit val manifestA: Manifest[A], val manifestB: Manifest[B]) extends FieldDef[Contents[A, DefaultEntry[A, B]]](self, "contents")
  // method definitions
  def hashMapNew1[A, B](contents: Rep[Contents[A, DefaultEntry[A, B]]])(implicit manifestA: Manifest[A], manifestB: Manifest[B]): Rep[HashMap[A, B]] = HashMapNew1[A, B](contents)
  def hashMapNew2[A, B](implicit manifestA: Manifest[A], manifestB: Manifest[B]): Rep[HashMap[A, B]] = HashMapNew2[A, B]()
  def hashMapGetOrElseUpdate[A, B](self: Rep[HashMap[A, B]], key: Rep[A], op: => Rep[B])(implicit manifestA: Manifest[A], manifestB: Manifest[B]): Rep[B] = {
    val opOutput = reifyBlock(op)
    HashMapGetOrElseUpdate[A, B](self, key, opOutput)
  }
  def hashMapClear[A, B](self: Rep[HashMap[A, B]])(implicit manifestA: Manifest[A], manifestB: Manifest[B]): Rep[Unit] = HashMapClear[A, B](self)
  def hashMapSize[A, B](self: Rep[HashMap[A, B]])(implicit manifestA: Manifest[A], manifestB: Manifest[B]): Rep[Int] = HashMapSize[A, B](self)
  def hashMapContains[A, B](self: Rep[HashMap[A, B]], key: Rep[A])(implicit manifestA: Manifest[A], manifestB: Manifest[B]): Rep[Boolean] = HashMapContains[A, B](self, key)
  def hashMapApply[A, B](self: Rep[HashMap[A, B]], key: Rep[A])(implicit manifestA: Manifest[A], manifestB: Manifest[B]): Rep[B] = HashMapApply[A, B](self, key)
  def hashMapUpdate[A, B](self: Rep[HashMap[A, B]], key: Rep[A], value: Rep[B])(implicit manifestA: Manifest[A], manifestB: Manifest[B]): Rep[Unit] = HashMapUpdate[A, B](self, key, value)
  def hashMapRemove[A, B](self: Rep[HashMap[A, B]], key: Rep[A])(implicit manifestA: Manifest[A], manifestB: Manifest[B]): Rep[Option[B]] = HashMapRemove[A, B](self, key)
  def hashMapKeySet[A, B](self: Rep[HashMap[A, B]])(implicit manifestA: Manifest[A], manifestB: Manifest[B]): Rep[Set[A]] = HashMapKeySet[A, B](self)
  def hashMap_Field_Contents[A, B](self: Rep[HashMap[A, B]])(implicit manifestA: Manifest[A], manifestB: Manifest[B]): Rep[Contents[A, DefaultEntry[A, B]]] = HashMap_Field_Contents[A, B](self)
  type HashMap[A, B] = scala.collection.mutable.HashMap[A, B]
}
trait HashMapImplicits { this: HashMapComponent =>
  // Add implicit conversions here!
}
trait HashMapComponent extends HashMapOps with HashMapImplicits { self: DeepDSL => }

trait SetOps extends Base { this: DeepDSL =>
  implicit class SetRep[A](self: Rep[Set[A]])(implicit manifestA: Manifest[A]) {
    def head(): Rep[A] = setHead[A](self)(manifestA)
    def apply(elem: Rep[A]): Rep[Boolean] = setApply[A](self, elem)(manifestA)
    def toSeq(): Rep[Seq[A]] = setToSeq[A](self)(manifestA)
    def remove(elem: Rep[A]): Rep[Boolean] = setRemove[A](self, elem)(manifestA)
  }
  // constructors

  // case classes
  case class SetHead[A](self: Rep[Set[A]])(implicit val manifestA: Manifest[A]) extends FunctionDef[A](Some(self), "head", List())
  case class SetApply[A](self: Rep[Set[A]], elem: Rep[A])(implicit val manifestA: Manifest[A]) extends FunctionDef[Boolean](Some(self), "apply", List(List(elem)))
  case class SetToSeq[A](self: Rep[Set[A]])(implicit val manifestA: Manifest[A]) extends FunctionDef[Seq[A]](Some(self), "toSeq", List())
  case class SetRemove[A](self: Rep[Set[A]], elem: Rep[A])(implicit val manifestA: Manifest[A]) extends FunctionDef[Boolean](Some(self), "remove", List(List(elem)))
  // method definitions
  def setHead[A](self: Rep[Set[A]])(implicit manifestA: Manifest[A]): Rep[A] = SetHead[A](self)
  def setApply[A](self: Rep[Set[A]], elem: Rep[A])(implicit manifestA: Manifest[A]): Rep[Boolean] = SetApply[A](self, elem)
  def setToSeq[A](self: Rep[Set[A]])(implicit manifestA: Manifest[A]): Rep[Seq[A]] = SetToSeq[A](self)
  def setRemove[A](self: Rep[Set[A]], elem: Rep[A])(implicit manifestA: Manifest[A]): Rep[Boolean] = SetRemove[A](self, elem)
  type Set[A] = scala.collection.mutable.Set[A]
}
trait SetImplicits { this: SetComponent =>
  // Add implicit conversions here!
}
trait SetComponent extends SetOps with SetImplicits { self: DeepDSL => }

trait TreeSetOps extends Base { this: DeepDSL =>
  implicit class TreeSetRep[A](self: Rep[TreeSet[A]])(implicit manifestA: Manifest[A], ordering: Ordering[A]) {
    def head(): Rep[A] = treeSetHead[A](self)(manifestA, ordering)
    def size(): Rep[Int] = treeSetSize[A](self)(manifestA, ordering)
    def -=(elem: Rep[A]): Rep[TreeSet[A]] = treeSet$minus$eq[A](self, elem)(manifestA, ordering)
    def +=(elem: Rep[A]): Rep[TreeSet[A]] = treeSet$plus$eq[A](self, elem)(manifestA, ordering)
    def from(): Rep[Option[A]] = treeSet_Field_From[A](self)(manifestA)
    def until(): Rep[Option[A]] = treeSet_Field_Until[A](self)(manifestA)
    def ordering: Rep[Ordering[A]] = treeSet_Field_Ordering[A](self)(manifestA)
    def notProjection: Rep[Boolean] = treeSet_Field_NotProjection[A](self)(manifestA)
  }
  // constructors
  def __newTreeSet[A](implicit ordering: Ordering[A], manifestA: Manifest[A]): Rep[TreeSet[A]] = treeSetNew[A](manifestA, ordering)
  // case classes
  case class TreeSetNew[A]()(implicit val manifestA: Manifest[A], val ordering: Ordering[A]) extends FunctionDef[TreeSet[A]](None, "new TreeSet", List())
  case class TreeSetHead[A](self: Rep[TreeSet[A]])(implicit val manifestA: Manifest[A], val ordering: Ordering[A]) extends FunctionDef[A](Some(self), "head", List())
  case class TreeSetSize[A](self: Rep[TreeSet[A]])(implicit val manifestA: Manifest[A], val ordering: Ordering[A]) extends FunctionDef[Int](Some(self), "size", List())
  case class TreeSet$minus$eq[A](self: Rep[TreeSet[A]], elem: Rep[A])(implicit val manifestA: Manifest[A], val ordering: Ordering[A]) extends FunctionDef[TreeSet[A]](Some(self), "$minus$eq", List(List(elem)))
  case class TreeSet$plus$eq[A](self: Rep[TreeSet[A]], elem: Rep[A])(implicit val manifestA: Manifest[A], val ordering: Ordering[A]) extends FunctionDef[TreeSet[A]](Some(self), "$plus$eq", List(List(elem)))
  case class TreeSet_Field_From[A](self: Rep[TreeSet[A]])(implicit val manifestA: Manifest[A]) extends FieldDef[Option[A]](self, "from")
  case class TreeSet_Field_Until[A](self: Rep[TreeSet[A]])(implicit val manifestA: Manifest[A]) extends FieldDef[Option[A]](self, "until")
  case class TreeSet_Field_Ordering[A](self: Rep[TreeSet[A]])(implicit val manifestA: Manifest[A]) extends FieldDef[Ordering[A]](self, "ordering")
  case class TreeSet_Field_NotProjection[A](self: Rep[TreeSet[A]])(implicit val manifestA: Manifest[A]) extends FieldDef[Boolean](self, "notProjection")
  // method definitions
  def treeSetNew[A](implicit manifestA: Manifest[A], ordering: Ordering[A]): Rep[TreeSet[A]] = TreeSetNew[A]()
  def treeSetHead[A](self: Rep[TreeSet[A]])(implicit manifestA: Manifest[A], ordering: Ordering[A]): Rep[A] = TreeSetHead[A](self)
  def treeSetSize[A](self: Rep[TreeSet[A]])(implicit manifestA: Manifest[A], ordering: Ordering[A]): Rep[Int] = TreeSetSize[A](self)
  def treeSet$minus$eq[A](self: Rep[TreeSet[A]], elem: Rep[A])(implicit manifestA: Manifest[A], ordering: Ordering[A]): Rep[TreeSet[A]] = TreeSet$minus$eq[A](self, elem)
  def treeSet$plus$eq[A](self: Rep[TreeSet[A]], elem: Rep[A])(implicit manifestA: Manifest[A], ordering: Ordering[A]): Rep[TreeSet[A]] = TreeSet$plus$eq[A](self, elem)
  def treeSet_Field_From[A](self: Rep[TreeSet[A]])(implicit manifestA: Manifest[A]): Rep[Option[A]] = TreeSet_Field_From[A](self)
  def treeSet_Field_Until[A](self: Rep[TreeSet[A]])(implicit manifestA: Manifest[A]): Rep[Option[A]] = TreeSet_Field_Until[A](self)
  def treeSet_Field_Ordering[A](self: Rep[TreeSet[A]])(implicit manifestA: Manifest[A]): Rep[Ordering[A]] = TreeSet_Field_Ordering[A](self)
  def treeSet_Field_NotProjection[A](self: Rep[TreeSet[A]])(implicit manifestA: Manifest[A]): Rep[Boolean] = TreeSet_Field_NotProjection[A](self)
  type TreeSet[A] = scala.collection.mutable.TreeSet[A]
}
trait TreeSetImplicits { this: TreeSetComponent =>
  // Add implicit conversions here!
}
trait TreeSetComponent extends TreeSetOps with TreeSetImplicits { self: DeepDSL => }

trait DefaultEntryOps extends Base { this: DeepDSL =>
  implicit class DefaultEntryRep[A, B](self: Rep[DefaultEntry[A, B]])(implicit manifestA: Manifest[A], manifestB: Manifest[B]) {
    def chainString(): Rep[String] = defaultEntryChainString[A, B](self)(manifestA, manifestB)
    def key: Rep[A] = defaultEntry_Field_Key[A, B](self)(manifestA, manifestB)
    def value: Rep[B] = defaultEntry_Field_Value[A, B](self)(manifestA, manifestB)
    def value_=(x$1: Rep[B]): Rep[Unit] = defaultEntry_Field_Value_$eq[A, B](self, x$1)(manifestA, manifestB)
  }
  // constructors
  def __newDefaultEntry[A, B](key: Rep[A], value: Rep[B])(implicit manifestA: Manifest[A], manifestB: Manifest[B]): Rep[DefaultEntry[A, B]] = defaultEntryNew[A, B](key, value)(manifestA, manifestB)
  // case classes
  case class DefaultEntryNew[A, B](key: Rep[A], value: Rep[B])(implicit val manifestA: Manifest[A], val manifestB: Manifest[B]) extends FunctionDef[DefaultEntry[A, B]](None, "new DefaultEntry", List(List(key, value)))
  case class DefaultEntryChainString[A, B](self: Rep[DefaultEntry[A, B]])(implicit val manifestA: Manifest[A], val manifestB: Manifest[B]) extends FunctionDef[String](Some(self), "chainString", List())
  case class DefaultEntry_Field_Key[A, B](self: Rep[DefaultEntry[A, B]])(implicit val manifestA: Manifest[A], val manifestB: Manifest[B]) extends FieldDef[A](self, "key")
  case class DefaultEntry_Field_Value[A, B](self: Rep[DefaultEntry[A, B]])(implicit val manifestA: Manifest[A], val manifestB: Manifest[B]) extends FieldDef[B](self, "value")
  case class DefaultEntry_Field_Value_$eq[A, B](self: Rep[DefaultEntry[A, B]], x$1: Rep[B])(implicit val manifestA: Manifest[A], val manifestB: Manifest[B]) extends FunctionDef[Unit](Some(self), "value_$eq", List(List(x$1)))
  // method definitions
  def defaultEntryNew[A, B](key: Rep[A], value: Rep[B])(implicit manifestA: Manifest[A], manifestB: Manifest[B]): Rep[DefaultEntry[A, B]] = DefaultEntryNew[A, B](key, value)
  def defaultEntryChainString[A, B](self: Rep[DefaultEntry[A, B]])(implicit manifestA: Manifest[A], manifestB: Manifest[B]): Rep[String] = DefaultEntryChainString[A, B](self)
  def defaultEntry_Field_Key[A, B](self: Rep[DefaultEntry[A, B]])(implicit manifestA: Manifest[A], manifestB: Manifest[B]): Rep[A] = DefaultEntry_Field_Key[A, B](self)
  def defaultEntry_Field_Value[A, B](self: Rep[DefaultEntry[A, B]])(implicit manifestA: Manifest[A], manifestB: Manifest[B]): Rep[B] = DefaultEntry_Field_Value[A, B](self)
  def defaultEntry_Field_Value_$eq[A, B](self: Rep[DefaultEntry[A, B]], x$1: Rep[B])(implicit manifestA: Manifest[A], manifestB: Manifest[B]): Rep[Unit] = DefaultEntry_Field_Value_$eq[A, B](self, x$1)
  type DefaultEntry[A, B] = scala.collection.mutable.DefaultEntry[A, B]
}
trait DefaultEntryImplicits { this: DefaultEntryComponent =>
  // Add implicit conversions here!
}
trait DefaultEntryComponent extends DefaultEntryOps with DefaultEntryImplicits { self: DeepDSL => }
