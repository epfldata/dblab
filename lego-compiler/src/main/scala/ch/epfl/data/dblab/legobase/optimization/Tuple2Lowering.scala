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
 * A lowering transformation for converting Tuple2 objects into records.
 *
 * @param IR the polymorphic embedding trait which contains the reified program.
 */
class Tuple2Lowering[Lang <: sc.pardis.deep.scalalib.ArrayComponent with sc.pardis.deep.scalalib.Tuple2Component](override val IR: Lang)
  extends sc.pardis.optimization.RecursiveRuleBasedTransformer[Lang](IR)
  with ArrayEscapeLowering[Lang]
  with VarEscapeLowering[Lang]
  with LambdaEscapeLowering[Lang]
  with RuleBasedLowering[Lang] {
  import IR._
  type Rep[T] = IR.Rep[T]
  type Var[T] = IR.Var[T]

  def lowerType[T: PardisType]: PardisType[Any] = ({
    val tp = typeRep[T]
    tp match {
      case Tuple2Type(_, _) => new RecordType(getClassTag[T](tp), Some(tp))
      case _                => tp
    }
  }).asInstanceOf[PardisType[Any]]

  def mayBeLowered[T](sym: Rep[T]): Boolean =
    isTuple2Record(sym.tp)

  def mustBeLowered[T](sym: Rep[T]): Boolean =
    // recordTypes.exists(x => x == node.tp) // && getStructDef(sym.tp).get.fields.forall(f => f.name != "next")
    mayBeLowered(sym)

  def addToLowered[T](sym: Rep[T]): Unit = ()

  def isTuple2Record[T](tp: TypeRep[T]): Boolean = tp match {
    case Tuple2Type(_, _) => true
    case _                => false
  }

  class A1
  class A2

  object Tuple2Create {
    def unapply[T](d: Def[T]): Option[(Rep[Any], Rep[Any])] = d match {
      case Tuple2ApplyObject(_1, _2) => Some(_1 -> _2)
      case Tuple2New(_1, _2)         => Some(_1 -> _2)
      case _                         => None
    }
  }

  rewrite += statement {
    case sym -> (node @ Tuple2Create(_1, _2)) if mayBeLowered(sym) =>

      implicit val typeA1 = transformType(node.tp.typeArguments(0)).asInstanceOf[TypeRep[A1]]
      implicit val typeA2 = transformType(node.tp.typeArguments(1)).asInstanceOf[TypeRep[A2]]
      __new[Tuple2[A1, A2]](
        ("_1", false, _1),
        ("_2", false, _2))
  }

  rewrite += rule {
    case n @ Tuple2_Field__1(sym) if mustBeLowered(sym) =>
      field(sym, "_1")(sym.tp.typeArguments(0).asInstanceOf[TypeRep[Any]])
  }

  rewrite += rule {
    case n @ Tuple2_Field__2(sym) if mustBeLowered(sym) =>
      field(sym, "_2")(sym.tp.typeArguments(1).asInstanceOf[TypeRep[Any]])
  }

}
