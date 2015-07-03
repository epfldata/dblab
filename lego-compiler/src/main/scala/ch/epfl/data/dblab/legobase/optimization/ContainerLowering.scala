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

/**
 * Factory for creating instances of [[ContainerLowering]]
 */
object ContainerLowering extends TransformerHandler {
  def apply[Lang <: Base, T: PardisType](context: Lang)(block: context.Block[T]): context.Block[T] = {
    new ContainerLowering(context.asInstanceOf[LoweringLegoBase]).optimize(block)
  }
}

/**
 * A lowering transformation for converting container objects into records.
 * Example:
 * {{{
 *    // class Container[T] { val elem: T, var next: Container[T] }
 *    // struct RecordA { val fieldA: Int, val fieldB: String }
 *    val rec1 = new RecordA(...)
 *    val contRecA = new Containter[RecordA](rec1)
 * }}}
 * is converted to:
 * {{{
 *    // struct RecordA { val fieldA: Int, val fieldB: String }
 *    // struct ContainerRecordA { val elem: RecordA, var next: ContainerRecordA }
 *    val rec1 = new RecordA(...)
 *    val contRecA = new ContainerRecordA(rec1)
 * }}}
 *
 * @param IR the polymorphic embedding trait which contains the reified program.
 */
class ContainerLowering[Lang <: sc.pardis.deep.scalalib.ArrayComponent with sc.pardis.deep.scalalib.Tuple2Component with ContOps](override val IR: Lang)
  extends sc.pardis.optimization.RecursiveRuleBasedTransformer[Lang](IR)
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
}
