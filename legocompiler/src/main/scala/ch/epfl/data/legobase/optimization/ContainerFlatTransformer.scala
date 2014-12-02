package ch.epfl.data
package legobase
package optimization

import scala.language.implicitConversions
import pardis.ir._
import reflect.runtime.universe.{ TypeTag, Type }
import pardis.optimization._
import deep._
import pardis.types._
import pardis.types.PardisTypeImplicits._
import pardis.deep.scalalib.collection._

object ContainerFlatTransformer extends TransformerHandler {
  def apply[Lang <: Base, T: PardisType](context: Lang)(block: context.Block[T]): context.Block[T] = {
    new ContainerFlatTransformer(context.asInstanceOf[LoweringLegoBase]).optimize(block)
  }
}

class ContainerFlatTransformer(override val IR: pardis.deep.scalalib.ArrayComponent with pardis.deep.scalalib.Tuple2Component with ContOps) extends pardis.optimization.RecursiveRuleBasedTransformer[pardis.deep.scalalib.ArrayComponent with pardis.deep.scalalib.Tuple2Component with ContOps](IR) {
  import IR._
  type Rep[T] = IR.Rep[T]
  type Var[T] = IR.Var[T]

  val recordTypes = scala.collection.mutable.Set[TypeRep[Any]]()

  def shouldBeLowered[T](sym: Rep[T]): Boolean =
    isContRecord(sym.tp)

  def isContRecord[T](tp: TypeRep[T]): Boolean = tp match {
    case ContType(t) if t.isRecord => true
    case _                         => false
  }

  def addSymRecordType[T](sym: Rep[T]): Unit =
    if (isContRecord(sym.tp)) {
      val tp = sym.tp.typeArguments(0).asInstanceOf[TypeRep[Any]]
      // System.out.println(s"tp $tp added")
      recordTypes += tp
    }

  override def transformType[T: PardisType]: PardisType[Any] = ({
    val tp = typeRep[T]
    tp match {
      case ContType(t) if t.isRecord => t
      case _                         => super.transformType(tp)
    }
  }).asInstanceOf[PardisType[Any]]

  analysis += statement {
    case sym -> (node @ ContNew(elem, next)) if shouldBeLowered(sym) =>
      addSymRecordType(sym)
  }

  analysis += rule {
    case Cont_Field_Next_$eq(self, x) if shouldBeLowered(self) =>
      addSymRecordType(self)
  }

  analysis += rule {
    case Cont_Field_Next(self) if shouldBeLowered(self) =>
      addSymRecordType(self)
  }

  analysis += rule {
    case Cont_Field_Elem(self) if shouldBeLowered(self) =>
      addSymRecordType(self)
  }

  rewrite += statement {
    case sym -> (node @ ContNew(elem, next)) if shouldBeLowered(sym) =>
      fieldSetter(elem, "next", next)
      elem
  }

  rewrite += rule {
    case Cont_Field_Next_$eq(self, x) if shouldBeLowered(self) =>
      fieldSetter(self, "next", x)
  }

  rewrite += rule {
    case Cont_Field_Next(self) if shouldBeLowered(self) =>
      fieldGetter(self, "next")
  }

  rewrite += rule {
    case Cont_Field_Elem(self) if shouldBeLowered(self) =>
      self
  }

  rewrite += rule {
    case node @ Struct(tag, elems, methods) if recordTypes.exists(x => x == node.tp) =>
      System.out.println(s"appending next to ${node.tp}")
      Struct(tag, elems :+ PardisStructArg("next", true, infix_asInstanceOf(unit(null))(node.tp)), methods)
  }

  analysis += statement {
    case sym -> ArrayNew(len) if isContRecord(sym.tp.typeArguments(0)) => {
      recordTypes += sym.tp.typeArguments(0).typeArguments(0).asInstanceOf[TypeRep[Any]]
      ()
    }
  }

  rewrite += statement {
    case sym -> ArrayNew(len) if isContRecord(sym.tp.typeArguments(0)) => {
      class B
      implicit val typeB = sym.tp.typeArguments(0).typeArguments(0).asInstanceOf[TypeRep[B]]
      __newArray[B](len).asInstanceOf[Rep[Any]]
    }
  }

  analysis += statement {
    case sym -> Tuple2New(_1, _2) if isContRecord(sym.tp.typeArguments(1)) => {
      // System.out.println(s"tuple 2 new lowering ${_2}")
      recordTypes += sym.tp.typeArguments(1).typeArguments(0).asInstanceOf[TypeRep[Any]]
      ()
    }
  }

  rewrite += statement {
    case sym -> Tuple2New(node_1, node_2) if isContRecord(sym.tp.typeArguments(1)) => {
      class A
      class B
      val _1 = node_1.asInstanceOf[Rep[A]]
      val _2 = node_2.asInstanceOf[Rep[B]]

      implicit val typeA = node_1.tp.asInstanceOf[TypeRep[A]]
      implicit val typeB = node_2.tp.typeArguments(0).asInstanceOf[TypeRep[B]]
      // System.out.println(s"tuple 2 lowering ${typeB}")
      __newTuple2(_1, _2).asInstanceOf[Rep[Any]]
    }
  }

  rewrite += statement {
    case sym -> (node @ Cast(v)) if isContRecord(sym.tp) =>
      infix_asInstanceOf(v)(apply(sym.tp))
  }
}
