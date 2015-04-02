package ch.epfl.data
package dblab.legobase
package optimization

import scala.language.implicitConversions
import sc.pardis.ir._
import reflect.runtime.universe.{ TypeTag, Type }
import sc.pardis.optimization._
import deep._
import sc.pardis.types._
import sc.pardis.types.PardisTypeImplicits._
import sc.pardis.deep.scalalib.collection._

object ContainerLowering extends TransformerHandler {
  def apply[Lang <: Base, T: PardisType](context: Lang)(block: context.Block[T]): context.Block[T] = {
    new ContainerLowering(context.asInstanceOf[LoweringLegoBase]).optimize(block)
  }
}

class ContainerLowering[Lang <: sc.pardis.deep.scalalib.ArrayComponent with sc.pardis.deep.scalalib.Tuple2Component with ContOps](override val IR: Lang) extends sc.pardis.optimization.RecursiveRuleBasedTransformer[Lang](IR)
  with ArrayEscapeLowering[Lang]
  with VarEscapeLowering[Lang]
  with Tuple2EscapeLowering[Lang]
  with LambdaEscapeLowering[Lang]
  with RuleBasedLowering[Lang] {
  import IR._
  type Rep[T] = IR.Rep[T]
  type Var[T] = IR.Var[T]

  def lowerType[T: PardisType]: PardisType[Any] = ({
    val tp = typeRep[T]
    tp match {
      case ContType(t) => new RecordType(getClassTag[T](tp), Some(tp))
      case _           => tp
    }
  }).asInstanceOf[PardisType[Any]]

  def mayBeLowered[T](sym: Rep[T]): Boolean =
    isContRecord(sym.tp)

  def mustBeLowered[T](sym: Rep[T]): Boolean =
    // recordTypes.exists(x => x == node.tp) // && getStructDef(sym.tp).get.fields.forall(f => f.name != "next")
    mayBeLowered(sym)

  def addToLowered[T](sym: Rep[T]): Unit = ()

  def isContRecord[T](tp: TypeRep[T]): Boolean = tp match {
    case ContType(t) => true
    case _           => false
  }

  class A

  rewrite += statement {
    case sym -> (node @ ContNew(elem, next)) if mayBeLowered(sym) =>

      implicit val typeA = transformType(node.tp.typeArguments(0)).asInstanceOf[TypeRep[A]]
      __new[Cont[A]](
        ("elem", false, elem),
        ("next", true, next))
  }

  rewrite += rule {
    case Cont_Field_Next__eq(self, x) if mayBeLowered(self) =>
      fieldSetter(self, "next", x)
  }

  rewrite += rule {
    case n @ Cont_Field_Next(self) if mayBeLowered(self) =>
      fieldGetter(self, "next")(lowerType(n.tp))
  }

  rewrite += rule {
    case n @ Cont_Field_Elem(self) if mayBeLowered(self) =>
      field(self, "elem")(apply(n.tp))
  }

  // rewrite += statement {
  //   case sym -> (node @ Struct(tag, elems, methods)) if mustBeLowered(node) && elems.forall(f => f.name != "next") =>
  //     System.out.println(s"appending next to ${node.tp}, $sym")
  //     // IR.asInstanceOf[LoweringLegoBase].printf(unit(s"struct creation $tag: ${node.tp}"))
  //     toAtom(Struct(tag, elems :+ PardisStructArg("next", true, infix_asInstanceOf(unit(null))(node.tp)), methods)(node.tp))(node.tp)
  // }

  // analysis += statement {
  //   case sym -> ArrayNew(len) if isContRecord(sym.tp.typeArguments(0)) => {
  //     recordTypes += sym.tp.typeArguments(0).typeArguments(0).asInstanceOf[TypeRep[Any]]
  //     ()
  //   }
  // }

  // rewrite += statement {
  //   case sym -> ArrayNew(len) if isContRecord(sym.tp.typeArguments(0)) => {
  //     class B
  //     implicit val typeB = sym.tp.typeArguments(0).typeArguments(0).asInstanceOf[TypeRep[B]]
  //     __newArray[B](len).asInstanceOf[Rep[Any]]
  //   }
  // }

  // analysis += statement {
  //   case sym -> Tuple2New(_1, _2) if isContRecord(sym.tp.typeArguments(1)) => {
  //     // System.out.println(s"tuple 2 new lowering ${_2}")
  //     recordTypes += sym.tp.typeArguments(1).typeArguments(0).asInstanceOf[TypeRep[Any]]
  //     ()
  //   }
  // }

  // rewrite += statement {
  //   case sym -> Tuple2New(node_1, node_2) if isContRecord(sym.tp.typeArguments(1)) => {
  //     class A
  //     class B
  //     val _1 = node_1.asInstanceOf[Rep[A]]
  //     val _2 = node_2.asInstanceOf[Rep[B]]

  //     implicit val typeA = node_1.tp.asInstanceOf[TypeRep[A]]
  //     implicit val typeB = node_2.tp.typeArguments(0).asInstanceOf[TypeRep[B]]
  //     // System.out.println(s"tuple 2 lowering ${typeB}")
  //     __newTuple2(_1, _2).asInstanceOf[Rep[Any]]
  //   }
  // }

  // rewrite += statement {
  //   case sym -> (node @ Cast(v)) if isContRecord(sym.tp) =>
  //     infix_asInstanceOf(v)(apply(sym.tp))
  // }
}
