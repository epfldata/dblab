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

/**
 * Converts a Set collection, which there is no duplicate element inserted into it,
 * to a linked list.
 * To check how this transformation is implemented take a look at the implementation
 * of [[sc.pardis.shallow.scalalib.collection.SetLinkedList]]. Purgatory automatically
 * generates the skeleton of the core part of this transformation out of the shallow
 * implementation.
 *
 * Example:
 * {{{
 *    val elements: Collection[A] = // some collection of elements of type A
 *    val set = Set[A]()
 *    for(elem <- elements) {
 *      set += elem
 *    }
 *    ...
 *    for(e <- set) { // desugared into `set.foreach(e => ...)`
 *      process(e)
 *    }
 * }}}
 * is converted into:
 * {{{
 *    val elements: Collection[A] = // some collection of elements of type A
 *    var setLinkedList = new Container[A](null)
 *    for(elem <- elements) {
 *      if (setLinkedList == null) {
 *        setLinkedList = new Container(elem, null)
 *      } else {
 *        val prevNext = setLinkedList.next
 *        val current = new Cont(elem, prevNext)
 *        setLinkedList.next = current
 *      }
 *    }
 *    ...
 *    var current = setLinkedList
 *    while (current != null) {
 *      val next = current.next
 *      process(current.elem)
 *      current = next
 *    }
 * }}}
 *
 * Precondition:
 * The elements inserted into the set should not have any duplication. Otherwise,
 * this transformation does not work correctly. There is no analysis phase involved
 * for check this contraint, which means it should be made sure by the user.
 */
class SetToLinkedListTransformation(
  override val IR: Lang)
  extends sc.pardis.deep.scalalib.collection.SetLinkedListTransformation(IR)
  with ArrayEscapeLowering[Lang]
  with VarEscapeLowering[Lang]
  with Tuple2EscapeLowering[Lang]
  with OptionEscapeLowering[Lang]
  with LambdaEscapeLowering[Lang]
  with RuleBasedLowering[Lang] {
  import IR._

  /**
   * The list of Set symbols that are lowered
   */
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

  /*
   * As a Set is directly lowered into a local variable, instead of first lowering
   * it into a record and then promoting the field of that record to local variable,
   * while we are updating or accessing the head of the lowered linked list, 
   * we have to take care of the cases handled by scalar replacement optimizations.
   * The following two methods are responsible for handling this task.
   */

  override def set_Field_HeadCont_$eq[A](self: Rep[Set[A]],
                                         x$1: Rep[Cont[A]])(implicit typeA: TypeRep[A]): Rep[Unit] = {
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
          case Def(NewVar(arrApp @ Def(ArrayApply(arr, i)))) =>
            apply(arrApp).asInstanceOf[Rep[Cont[A]]]
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
