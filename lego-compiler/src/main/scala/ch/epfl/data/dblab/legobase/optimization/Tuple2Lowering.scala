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
class Tuple2Lowering[Lang <: LegoBaseExp](override val IR: Lang)
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

  analysis += rule {
    case Lambda2(f, i1, i2, o) if mayBeLowered(i1) || mayBeLowered(i2) => {
      addToLowered(i1)
      addToLowered(i2)
      ()
    }
  }

  // TODO move to SC?
  analysis += rule {
    case ArrayUpdate(arr, i, v) if mayBeLowered(v) => {
      addToLowered(v)
      loweredEscapeArrays += arr
      ()
    }
  }

  rewrite += rule {
    case ArrayUpdate(nodearr, i, v) if loweredEscapeArrays.contains(nodearr) => {
      class B
      implicit val typeB = apply(nodearr).tp.typeArguments(0).asInstanceOf[TypeRep[B]]
      val arr = apply(nodearr).asInstanceOf[Rep[Array[B]]]
      arr(i) = apply(v).asInstanceOf[Rep[B]]
    }
  }

  rewrite += rule {
    case Lambda2(f, i1, i2, o) if mustBeLowered(i1) || mustBeLowered(i2) => {
      class A
      class B
      implicit val typeA = lowerType(i1.tp).asInstanceOf[TypeRep[A]]
      implicit val typeB = lowerType(i2.tp).asInstanceOf[TypeRep[B]]
      val newI1 = fresh[A]
      val newI2 = fresh[B]
      subst += i1 -> newI1
      subst += i2 -> newI2
      val newO = transformBlockTyped(o)(o.tp, transformType(o.tp)).asInstanceOf[Block[Any]]
      val newF = (x: Rep[Any], y: Rep[Any]) => substitutionContext(newI1 -> x, newI2 -> y) {
        inlineBlock(newO)
      }
      addToLowered(newI1)
      addToLowered(newI2)
      Lambda2(newF, newI1, newI2, newO).asInstanceOf[Def[Any]]
    }
  }

  rewrite += rule {
    case pc @ PardisCast(exp) => {
      infix_asInstanceOf(apply(exp))(lowerType(pc.castTp))
      // PardisCast(transformExp[Any, Any](exp))(lowerType(exp.tp), lowerType(pc.castTp))
    }
  }

  val loweredEscapeTreeSets = scala.collection.mutable.ArrayBuffer[Rep[Any]]()

  analysis += statement {
    case sym -> TreeSetHead(treeSet) if mayBeLowered(sym) => {
      addToLowered(sym)
      loweredEscapeTreeSets += treeSet
      ()
    }
  }

  rewrite += statement {
    case sym -> TreeSetNew2(ordering) if loweredEscapeTreeSets.contains(sym) => {
      class B
      implicit val typeB = lowerType(sym.tp.typeArguments(0)).asInstanceOf[TypeRep[B]]
      __newTreeSet2[B](ordering.asInstanceOf[Rep[Ordering[B]]]).asInstanceOf[Rep[Any]]
    }
  }

  rewrite += statement {
    case sym -> TreeSetHead(treeSet) if loweredEscapeTreeSets.contains(treeSet) => {
      class B
      implicit val typeB = apply(treeSet).tp.typeArguments(0).asInstanceOf[TypeRep[B]]
      val treeSet2 = apply(treeSet).asInstanceOf[Rep[TreeSet[B]]]
      treeSet2.head.asInstanceOf[Rep[Any]]
    }
  }

}
