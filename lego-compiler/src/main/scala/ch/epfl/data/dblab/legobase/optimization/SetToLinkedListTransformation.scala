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

  // private implicit class SetRep1[A](self: Rep[Set[A]]) {
  //   implicit val typeA = transformType(self.tp.typeArguments(0)).asInstanceOf[TypeRep[A]]
  //   def headCont_=(x$1: Rep[Cont[A]]): Rep[Unit] = {
  //     // __assign(headContMap(self), x$1)
  //     // val v = Var(self.asInstanceOf[Rep[Var[Cont[A]]]])
  //     // __assign(v, x$1)
  //     // System.out.println(s"assigning to headCont $self ${apply(self)} ${apply(x$1)}")
  //     // printf(unit(s"assigning to headCont $self ${apply(self)} ${apply(x$1)} ${self.correspondingNode}"))
  //     self match {
  //       case Def(ReadVar(v)) =>
  //         // System.out.println(s"with var $v ${apply(v.e)} ${v.e.correspondingNode}")
  //         v.e match {
  //           case Def(NewVar(Def(ArrayApply(arr, i)))) => {
  //             // printf(unit(s"arr update ${apply(arr)} ${apply(i)} ${apply(x$1)}"))
  //             arr.asInstanceOf[Rep[Array[Cont[A]]]](i) = x$1
  //           }
  //           case _ => ()
  //         }
  //         __assign(v.asInstanceOf[Var[Cont[A]]], x$1)
  //       case Def(ArrayApply(arr, i)) =>
  //         arr.asInstanceOf[Rep[Array[Cont[A]]]](i) = x$1
  //       case Def(OptionGet(Def(OptionApplyObject(Def(ArrayApply(arr, i)))))) =>
  //         arr.asInstanceOf[Rep[Array[Cont[A]]]](i) = x$1
  //       case _ =>
  //         System.out.println(s"Assigning Default ${apply(self)}, ${apply(self).correspondingNode}")
  //         val v = Var(self.asInstanceOf[Rep[Var[Cont[A]]]])
  //         __assign(v, x$1)
  //       // new SetRep1(apply(self).asInstanceOf[Rep[Set[A]]]).headCont_=(x$1)
  //     }
  //   }
  //   def headCont: Rep[Cont[A]] = {
  //     // val v = headContMap(self).asInstanceOf[Var[Cont[A]]]
  //     // __readVar(v).asInstanceOf[Rep[Cont[A]]]
  //     // val v = Var(self.asInstanceOf[Rep[Var[Cont[A]]]])
  //     // __readVar(v).asInstanceOf[Rep[Cont[A]]]
  //     self match {
  //       case Def(ReadVar(v)) => {
  //         v.e match {
  //           case Def(NewVar(arrApp @ Def(ArrayApply(arr, i)))) => apply(arrApp).asInstanceOf[Rep[Cont[A]]]
  //           case _ => __readVar(v.asInstanceOf[Var[Cont[A]]])
  //         }
  //         // System.out.println(s"reading $v $self")

  //       }
  //       case _ =>
  //         // val v = Var(self.asInstanceOf[Rep[Var[Cont[A]]]])
  //         // __readVar(v).asInstanceOf[Rep[Cont[A]]]
  //         self.asInstanceOf[Rep[Cont[A]]]
  //     }
  //   }
  // }

  override def set_Field_HeadCont_$eq[A](self: Rep[Set[A]], x$1: Rep[Cont[A]])(implicit typeA: TypeRep[A]): Rep[Unit] = {
    self match {
      case Def(ReadVar(v)) =>
        // System.out.println(s"with var $v ${apply(v.e)} ${v.e.correspondingNode}")
        v.e match {
          case Def(NewVar(Def(ArrayApply(arr, i)))) => {
            // printf(unit(s"arr update ${apply(arr)} ${apply(i)} ${apply(x$1)}"))
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
        System.out.println(s"Assigning Default ${apply(self)}, ${apply(self).correspondingNode}")
        val v = Var(self.asInstanceOf[Rep[Var[Cont[A]]]])
        __assign(v, x$1)
      // new SetRep1(apply(self).asInstanceOf[Rep[Set[A]]]).headCont_=(x$1)
    }
  }
  override def set_Field_HeadCont[A](self: Rep[Set[A]])(implicit typeA: TypeRep[A]): Rep[Cont[A]] = {
    self match {
      case Def(ReadVar(v)) => {
        v.e match {
          case Def(NewVar(arrApp @ Def(ArrayApply(arr, i)))) => apply(arrApp).asInstanceOf[Rep[Cont[A]]]
          case _ => __readVar(v.asInstanceOf[Var[Cont[A]]])
        }
        // System.out.println(s"reading $v $self")

      }
      case _ =>
        // val v = Var(self.asInstanceOf[Rep[Var[Cont[A]]]])
        // __readVar(v).asInstanceOf[Rep[Cont[A]]]
        self.asInstanceOf[Rep[Cont[A]]]
    }
  }

  // Set

  analysis += statement {
    case sym -> (node @ SetApplyObject2()) => {
      addToLowered(sym)
      // System.out.println(s"sym $sym added to sets")
      ()
    }
  }

  // rewrite += statement {
  //   case sym -> (node @ SetNew()) =>

  //     implicit val typeA = transformType(node.tp.typeArguments(0)).asInstanceOf[TypeRep[A]]

  //     // headContMap(sym) = __newVar[Cont[A]](unit(null))

  //     unit(null.asInstanceOf[Any])(node.tp.asInstanceOf[TypeRep[Any]])
  // }

  rewrite += statement {
    case sym -> (node @ SetApplyObject2()) =>
      implicit val typeA = transformType(node.tp.typeArguments(0)).asInstanceOf[TypeRep[A]]

      // headContMap(sym) = __newVar[Cont[A]](unit(null))

      // unit(null.asInstanceOf[Any])(node.tp.asInstanceOf[TypeRep[Any]])
      // System.out.println(s"handling $sym added to sets")
      // readVar(__newVar[Cont[A]](unit(null)))
      // readVar(__newVar[Cont[A]](__newCont(unit(null), unit(null))))
      val newCont = __newCont(unit[A](null), unit[Cont[A]](null))
      readVar(__newVarNamed[Cont[A]](newCont, "newSet"))
  }

  // def __newSetLinkedList[A]()(implicit typeA: TypeRep[A]): Rep[Set[A]] = SetNew[A]()

  // rewrite += rule {
  //   case node @ Set$plus$eq(nodeself, nodeelem) if mustBeLowered(nodeself) =>

  //     val self = nodeself.asInstanceOf[Rep[Set[A]]]
  //     val elem = nodeelem.asInstanceOf[Rep[A]]
  //     implicit val typeA = transformType(nodeself.tp.typeArguments(0)).asInstanceOf[TypeRep[A]]

  //     {
  //       // val prevHead: this.Rep[ch.epfl.data.sc.pardis.shallow.scalalib.collection.Cont[A]] = self.headCont;
  //       // // printf(unit(s"set+=, prevHead: $prevHead"))
  //       // self.headCont_$eq(__newCont(elem, prevHead))

  //       val prevHead: this.Rep[ch.epfl.data.sc.pardis.shallow.scalalib.collection.Cont[A]] = self.headCont;
  //       __ifThenElse(infix_==(prevHead, unit(null)), {
  //         self.headCont_$eq(__newCont(elem, unit(null)))
  //       }, {
  //         val prevNext = prevHead.next
  //         val c = __newCont(elem, prevNext)
  //         prevHead.next = c
  //         // self.headCont_$eq(c)
  //       })

  //       // val prevHead: this.Rep[ch.epfl.data.sc.pardis.shallow.scalalib.collection.Cont[A]] = self.headCont;
  //       // printf(unit(s"set+=, prevHead: $prevHead"))
  //       // self.headCont_$eq(__newCont(elem, unit(null)))
  //     }
  // }

  // rewrite += rule {
  //   case node @ SetForeach(nodeself, nodef) if mustBeLowered(nodeself) =>

  //     val self = nodeself.asInstanceOf[Rep[Set[A]]]
  //     val f = nodef.asInstanceOf[Rep[((A) => Unit)]]
  //     implicit val typeA = transformType(nodeself.tp.typeArguments(0)).asInstanceOf[TypeRep[A]]

  //     {
  //       var current: this.Var[ch.epfl.data.sc.pardis.shallow.scalalib.collection.Cont[A]] = __newVarNamed(self.headCont, "current");
  //       __whileDo(infix_$bang$eq(__readVar(current), unit(null)), {
  //         val next: this.Rep[ch.epfl.data.sc.pardis.shallow.scalalib.collection.Cont[A]] = __readVar(current).next;
  //         __app[A, Unit](f).apply(__readVar(current).elem);
  //         __assign(current, next)
  //       })
  //     }
  // }

  // rewrite += rule {
  //   case node @ SetRetain(nodeself, nodep) if mustBeLowered(nodeself) =>

  //     val self = nodeself.asInstanceOf[Rep[Set[A]]]
  //     val p = nodep.asInstanceOf[Rep[((A) => Boolean)]]
  //     implicit val typeA = transformType(nodeself.tp.typeArguments(0)).asInstanceOf[TypeRep[A]]

  //     {
  //       var prev: this.Var[ch.epfl.data.sc.pardis.shallow.scalalib.collection.Cont[A]] = __newVarNamed(infix_asInstanceOf[ch.epfl.data.sc.pardis.shallow.scalalib.collection.Cont[A]](unit(null)), "prev");
  //       var current: this.Var[ch.epfl.data.sc.pardis.shallow.scalalib.collection.Cont[A]] = __newVarNamed(self.headCont, "current");
  //       __whileDo(infix_$bang$eq(__readVar(current), unit(null)), {
  //         __ifThenElse(__app[A, Boolean](p).apply(__readVar(current).elem).unary_$bang, __ifThenElse(infix_$eq$eq(self.headCont, __readVar(current)), self.headCont_$eq(unit(null)), unit(())), {
  //           __ifThenElse(infix_$bang$eq(__readVar(prev), unit(null)), __readVar(prev).next_$eq(__readVar(current)), unit(()));
  //           __ifThenElse(infix_$eq$eq(self.headCont, unit(null)), self.headCont_$eq(__readVar(current)), unit(()));
  //           __assign(prev, __readVar(current))
  //         });
  //         __assign(current, __readVar(current).next)
  //       })
  //     }
  // }

  // rewrite += rule {
  //   case node @ SetExists(nodeself, nodep) if mustBeLowered(nodeself) =>

  //     val self = nodeself.asInstanceOf[Rep[Set[A]]]
  //     val p = nodep.asInstanceOf[Rep[((A) => Boolean)]]
  //     implicit val typeA = transformType(nodeself.tp.typeArguments(0)).asInstanceOf[TypeRep[A]]

  //     {
  //       var current: this.Var[ch.epfl.data.sc.pardis.shallow.scalalib.collection.Cont[A]] = __newVarNamed(self.headCont, "current");
  //       var found: this.Var[Boolean] = __newVarNamed(unit(false), "found");
  //       __whileDo(infix_$bang$eq(__readVar(current), unit(null)).$amp$amp(__readVar(found).unary_$bang), {
  //         __ifThenElse(__app[A, Boolean](p).apply(__readVar(current).elem), __assign(found, unit(true)), unit(()));
  //         __assign(current, __readVar(current).next)
  //       });
  //       __readVar(found)
  //     }
  // }

  // rewrite += rule {
  //   case node @ SetHead(nodeself) if mustBeLowered(nodeself) =>

  //     val self = nodeself.asInstanceOf[Rep[Set[A]]]
  //     implicit val typeA = transformType(nodeself.tp.typeArguments(0)).asInstanceOf[TypeRep[A]]

  //     self.headCont.elem
  // }

  // rewrite += rule {
  //   case node @ SetFoldLeft(nodeself, nodez, nodeop) if mustBeLowered(nodeself) =>
  //     class B
  //     val self = nodeself.asInstanceOf[Rep[Set[A]]]
  //     val z = nodez.asInstanceOf[Rep[B]]
  //     val op = nodeop.asInstanceOf[Rep[((B, A) => B)]]
  //     implicit val typeA = transformType(nodeself.tp.typeArguments(0)).asInstanceOf[TypeRep[A]]
  //     implicit val typeB = transformType(z.tp).asInstanceOf[TypeRep[B]]

  //     {
  //       var acc: this.Var[B] = __newVarNamed(z, "acc");
  //       self.foreach(__lambda(((e: this.Rep[A]) => __assign(acc, __app[B, A, B](op).apply(__readVar(acc), e)))));
  //       __readVar(acc)
  //     }
  // }

  // rewrite += rule {
  //   case node @ SetSize(nodeself) if mustBeLowered(nodeself) =>

  //     val self = nodeself.asInstanceOf[Rep[Set[A]]]
  //     implicit val typeA = transformType(nodeself.tp.typeArguments(0)).asInstanceOf[TypeRep[A]]

  //     {
  //       var s: this.Var[Int] = __newVarNamed(unit(0), "s");
  //       self.foreach(__lambda(((e: this.Rep[A]) => __assign(s, __readVar(s).$plus(unit(1))))));
  //       __readVar(s)
  //     }
  // }

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

  // rewrite += rule {
  //   case node @ SetFind(nodeself, nodep) if mustBeLowered(nodeself) =>

  //     val self = nodeself.asInstanceOf[Rep[Set[A]]]
  //     val p = nodep.asInstanceOf[Rep[((A) => Boolean)]]
  //     implicit val typeA = transformType(nodeself.tp.typeArguments(0)).asInstanceOf[TypeRep[A]]

  //     {
  //       var current: this.Var[ch.epfl.data.sc.pardis.shallow.scalalib.collection.Cont[A]] = __newVarNamed(self.headCont, "current");
  //       var found: this.Var[Boolean] = __newVarNamed(unit(false), "found");
  //       __whileDo(infix_$bang$eq(__readVar(current), unit(null)).$amp$amp(__readVar(found).unary_$bang), __ifThenElse(__app[A, Boolean](p).apply(__readVar(current).elem), __assign(found, unit(true)), __assign(current, __readVar(current).next)));
  //       __ifThenElse(__readVar(found).unary_$bang, Option.apply[A](infix_asInstanceOf[A](unit(null))), Option.apply[A](__readVar(current).elem))
  //     }
  // }

}

