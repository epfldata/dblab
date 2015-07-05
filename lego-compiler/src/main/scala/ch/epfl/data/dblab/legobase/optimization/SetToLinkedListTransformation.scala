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
import sc.pardis.deep.scalalib._
import sc.pardis.deep.scalalib.collection._

object SetToLinkedListTransformation {
  type Lang = SetOps with ContOps with ArrayOps with IntOps with BooleanOps with OptionOps
}

import SetToLinkedListTransformation.Lang

class SetToLinkedListTransformation(
  override val IR: Lang) extends sc.pardis.deep.scalalib.collection.SetLinkedListTransformation(IR)
  with ArrayEscapeLowering[Lang]
  with VarEscapeLowering[Lang]
  with Tuple2EscapeLowering[Lang]
  with OptionEscapeLowering[Lang]
  with LambdaEscapeLowering[Lang]
  with RuleBasedLowering[Lang] {
  import IR._

  val loweredSets = scala.collection.mutable.ArrayBuffer[Rep[Any]]()

  def mustBeLowered[T](sym: Rep[T]): Boolean =
    loweredSets.contains(sym.asInstanceOf[Rep[Any]])
  def mayBeLowered[T](sym: Rep[T]): Boolean =
    sym.tp.isInstanceOf[SetType[_]]
  def addToLowered[T](sym: Rep[T]): Unit =
    loweredSets += sym.asInstanceOf[Rep[Any]]

  def lowerType[T: PardisType]: PardisType[Any] = ({
    val tp = typeRep[T]
    tp match {
      case SetType(t) => ContType(t)
      case _          => tp
    }
  }).asInstanceOf[PardisType[Any]]

  override def set_Field_HeadCont_$eq[A](self: Rep[Set[A]], x$1: Rep[Cont[A]])(implicit typeA: TypeRep[A]): Rep[Unit] = {
    self match {
      case Def(ReadVar(v)) =>
        v.e match {
          case Def(NewVar(Def(ArrayApply(arr, i)))) => {
            arr.asInstanceOf[Rep[Array[Cont[A]]]](i) = x$1
          }
          case _ => ()
        }
        __assign(v.asInstanceOf[Var[Cont[A]]], x$1)
      case Def(ArrayApply(arr, i)) =>
        arr.asInstanceOf[Rep[Array[Cont[A]]]](i) = x$1
      case Def(OptionGet(Def(OptionApplyObject(Def(ArrayApply(arr, i)))))) =>
        arr.asInstanceOf[Rep[Array[Cont[A]]]](i) = x$1
      case _ =>
        val v = Var(self.asInstanceOf[Rep[Var[Cont[A]]]])
        __assign(v, x$1)
    }
  }
  override def set_Field_HeadCont[A](self: Rep[Set[A]])(implicit typeA: TypeRep[A]): Rep[Cont[A]] = {
    self match {
      case Def(ReadVar(v)) => {
        v.e match {
          case Def(NewVar(arrApp @ Def(ArrayApply(arr, i)))) => apply(arrApp).asInstanceOf[Rep[Cont[A]]]
          case _ => __readVar(v.asInstanceOf[Var[Cont[A]]])
        }
      }
      case _ =>
        self.asInstanceOf[Rep[Cont[A]]]
    }
  }

  analysis += statement {
    case sym -> (node @ SetApplyObject2()) => {
      addToLowered(sym)
      ()
    }
  }

  rewrite += statement {
    case sym -> (node @ SetApplyObject2()) =>
      implicit val typeA = transformType(node.tp.typeArguments(0)).asInstanceOf[TypeRep[A]]

      val newCont = __newCont(unit[A](null), unit[Cont[A]](null))
      readVar(__newVarNamed[Cont[A]](newCont, "newSet"))
  }

  // TODO cmp handling should be automatically generated
  rewrite += rule {
    case node @ SetMinBy(nodeself, nodef) if mustBeLowered(nodeself) =>
      class B
      val self = nodeself.asInstanceOf[Rep[Set[A]]]
      val f = nodef.asInstanceOf[Rep[((A) => B)]]

      implicit val typeA = transformType(nodeself.tp.typeArguments(0)).asInstanceOf[TypeRep[A]]
      implicit val typeB = transformType(f.tp.typeArguments(1)).asInstanceOf[TypeRep[B]]

      val MIR = IR.asInstanceOf[ManualLiftedLegoBase]
      import MIR.{ OrderingRep, OrderingOps }

      val cmp = OrderingRep[B]

      {
        var min: this.Var[A] = __newVarNamed(self.head, "min");
        var minValue: this.Var[B] = __newVarNamed(__app[A, B](f).apply(__readVar(min)), "minValue");
        self.foreach(__lambda(((e: this.Rep[A]) => {
          val v: this.Rep[B] = __app[A, B](f).apply(e);
          __ifThenElse(cmp.lt(v, __readVar(minValue)), {
            __assign(min, e);
            __assign(minValue, v)
          }, unit(()))
        })));
        __readVar(min)
      }
  }
}
