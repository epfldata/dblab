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

object SetLinkedListTransformation extends TransformerHandler {
  def apply[Lang <: Base, T: PardisType](context: Lang)(block: context.Block[T]): context.Block[T] = {
    new SetLinkedListTransformation(context.asInstanceOf[LoweringLegoBase]).optimize(block)
  }
}

trait RuleBasedLowering[Lang <: Base] { this: RecursiveRuleBasedTransformer[Lang] =>
  import IR._
  def lowerType[T: PardisType]: PardisType[Any]
  def mustBeLowered[T](sym: Rep[T]): Boolean
  def addToLowered[T](sym: Rep[T]): Unit
  def mayBeLowered[T](sym: Rep[T]): Boolean
}

trait ArrayEscapeLowering[Lang <: Base with pardis.deep.scalalib.ArrayComponent with pardis.deep.scalalib.IntComponent] extends RuleBasedLowering[Lang] { this: RecursiveRuleBasedTransformer[Lang] =>
  import IR._
  val loweredEscapeArrays = scala.collection.mutable.ArrayBuffer[Rep[Any]]()

  // Array usage

  analysis += statement {
    case sym -> ArrayApply(arr, i) if mayBeLowered(sym) => {
      addToLowered(sym)
      loweredEscapeArrays += arr
      ()
    }
  }

  rewrite += statement {
    case sym -> ArrayNew(len) if loweredEscapeArrays.contains(sym) => {
      class B
      implicit val typeB = lowerType(sym.tp.typeArguments(0)).asInstanceOf[TypeRep[B]]
      __newArray[B](len).asInstanceOf[Rep[Any]]
    }
  }

  rewrite += statement {
    case sym -> ArrayApply(arr, i) if loweredEscapeArrays.contains(arr) => {
      class B
      implicit val typeB = apply(arr).tp.typeArguments(0).asInstanceOf[TypeRep[B]]
      apply(arr).asInstanceOf[Rep[Array[B]]](i).asInstanceOf[Rep[Any]]
    }
  }
}

class SetLinkedListTransformation(override val IR: SetComponent with pardis.deep.scalalib.OptionComponent with pardis.deep.scalalib.Tuple2Component with pardis.deep.scalalib.ArrayComponent with pardis.deep.scalalib.IntComponent with pardis.deep.scalalib.BooleanComponent with ContOps with ManualLiftedLegoBase) extends pardis.optimization.RecursiveRuleBasedTransformer[SetComponent with pardis.deep.scalalib.OptionComponent with pardis.deep.scalalib.Tuple2Component with pardis.deep.scalalib.ArrayComponent with pardis.deep.scalalib.IntComponent with pardis.deep.scalalib.BooleanComponent with ContOps with ManualLiftedLegoBase](IR) with ArrayEscapeLowering[SetComponent with pardis.deep.scalalib.OptionComponent with pardis.deep.scalalib.Tuple2Component with pardis.deep.scalalib.ArrayComponent with pardis.deep.scalalib.IntComponent with pardis.deep.scalalib.BooleanComponent with ContOps with ManualLiftedLegoBase] {
  import IR._
  type Rep[T] = IR.Rep[T]
  type Var[T] = IR.Var[T]

  class A

  // val headContMap = scala.collection.mutable.Map[Rep[Any], Var[Any]]()
  val loweredSets = scala.collection.mutable.ArrayBuffer[Rep[Any]]()
  // val loweredSetArrays = scala.collection.mutable.ArrayBuffer[Rep[Any]]()
  val loweredSetVars = scala.collection.mutable.ArrayBuffer[Rep[Any]]()
  val loweredSetTuples = scala.collection.mutable.ArrayBuffer[Rep[Any]]()
  val loweredSetOptions = scala.collection.mutable.ArrayBuffer[Rep[Any]]()
  def mustBeLowered[T](sym: Rep[T]): Boolean =
    loweredSets.contains(sym.asInstanceOf[Rep[Any]])
  def mayBeLowered[T](sym: Rep[T]): Boolean =
    sym.tp.isInstanceOf[SetType[_]]
  def addToLowered[T](sym: Rep[T]): Unit =
    loweredSets += sym.asInstanceOf[Rep[Any]]

  private implicit class SetRep1[A](self: Rep[Set[A]]) {
    implicit val typeA = transformType(self.tp.typeArguments(0)).asInstanceOf[TypeRep[A]]
    def headCont_=(x$1: Rep[Cont[A]]): Rep[Unit] = {
      // __assign(headContMap(self), x$1)
      // val v = Var(self.asInstanceOf[Rep[Var[Cont[A]]]])
      // __assign(v, x$1)
      // System.out.println(s"assigning to headCont $self ${apply(self)} ${apply(x$1)}")
      self match {
        case Def(ReadVar(v)) =>
          // System.out.println(s"with var $v ${apply(v.e)} ${v.e.correspondingNode}")
          v.e match {
            case Def(NewVar(Def(ArrayApply(arr, i)))) => arr.asInstanceOf[Rep[Array[Cont[A]]]](i) = x$1
            case _                                    => ()
          }
          __assign(v.asInstanceOf[Var[Cont[A]]], x$1)
        case Def(ArrayApply(arr, i)) =>
          arr.asInstanceOf[Rep[Array[Cont[A]]]](i) = x$1
        case Def(OptionGet(Def(OptionApplyObject(Def(ArrayApply(arr, i)))))) =>
          arr.asInstanceOf[Rep[Array[Cont[A]]]](i) = x$1
        case _ =>
          System.out.println(s"Assigning Default ${apply(self)}")
          val v = Var(self.asInstanceOf[Rep[Var[Cont[A]]]])
          __assign(v, x$1)
      }
    }
    def headCont: Rep[Cont[A]] = {
      // val v = headContMap(self).asInstanceOf[Var[Cont[A]]]
      // __readVar(v).asInstanceOf[Rep[Cont[A]]]
      // val v = Var(self.asInstanceOf[Rep[Var[Cont[A]]]])
      // __readVar(v).asInstanceOf[Rep[Cont[A]]]
      self match {
        case Def(ReadVar(v)) => {
          v.e match {
            case Def(NewVar(arrApp @ Def(ArrayApply(arr, i)))) => apply(arrApp).asInstanceOf[Rep[Cont[A]]]
            case _ => __readVar(v.asInstanceOf[Var[Cont[A]]])
          }
          // System.out.println(s"reading $v $self")

        }
        case _ =>
          val v = Var(self.asInstanceOf[Rep[Var[Cont[A]]]])
          __readVar(v).asInstanceOf[Rep[Cont[A]]]
        // self.asInstanceOf[Rep[Cont[A]]]
      }
    }
  }

  def lowerType[T: PardisType]: PardisType[Any] = ({
    val tp = typeRep[T]
    tp match {
      case SetType(t) => ContType(t)
      case _          => tp
    }
  }).asInstanceOf[PardisType[Any]]

  // Array usage

  // analysis += statement {
  //   case sym -> ArrayApply(arr, i) if sym.tp.isInstanceOf[SetType[_]] => {
  //     loweredSets += sym
  //     loweredSetArrays += arr
  //     ()
  //   }
  // }

  // rewrite += statement {
  //   case sym -> ArrayNew(len) if loweredSetArrays.contains(sym) => {
  //     class B
  //     implicit val typeB = sym.tp.typeArguments(0).typeArguments(0).asInstanceOf[TypeRep[B]]
  //     __newArray[Cont[B]](len).asInstanceOf[Rep[Any]]
  //   }
  // }

  // rewrite += statement {
  //   case sym -> ArrayApply(arr, i) if loweredSetArrays.contains(arr) => {
  //     class B
  //     implicit val typeB = apply(arr).tp.typeArguments(0).asInstanceOf[TypeRep[Cont[B]]]
  //     apply(arr).asInstanceOf[Rep[Array[Cont[B]]]](i).asInstanceOf[Rep[Any]]
  //   }
  // }

  // Var usage

  analysis += statement {
    case sym -> NewVar(v) if mustBeLowered(v) => {
      loweredSetVars += sym
      ()
    }
  }

  analysis += statement {
    case sym -> ReadVar(v) if loweredSetVars.contains(v.e) => {
      loweredSets += sym
      ()
    }
  }

  rewrite += statement {
    case sym -> NewVar(nodev) if loweredSetVars.contains(sym) => {
      class B
      val v = nodev.asInstanceOf[Rep[Cont[B]]]
      implicit val typeB = v.tp.typeArguments(0).asInstanceOf[TypeRep[B]]
      val e = __newVar[Cont[B]](apply(v)).e
      // System.out.println(s"new var $e $v ${apply(v)}")
      e.asInstanceOf[Rep[Any]]
    }
  }

  rewrite += statement {
    case sym -> ReadVar(nodev) if mustBeLowered(sym) => {
      class B
      // System.out.println(s"tp ${sym.tp}")
      implicit val typeB = sym.tp.typeArguments(0).asInstanceOf[TypeRep[B]]
      val v = Var(apply(nodev.e)).asInstanceOf[Var[Cont[B]]]
      readVar(v).asInstanceOf[Rep[Any]]
    }
  }

  // rewrite += rule {
  //   case Assign(lhs, rhs) if loweredSetVars.contains(lhs) => {
  //     class B
  //     val v = nodev.asInstanceOf[Rep[Cont[B]]]
  //     implicit val typeB = v.tp.typeArguments(0).asInstanceOf[TypeRep[B]]
  //     __newVar[Cont[B]](v).e.asInstanceOf[Rep[Any]]
  //   }
  // }

  // Lambda

  analysis += rule {
    case Lambda(f, i, o) if i.tp.isInstanceOf[SetType[_]] => {
      // System.out.println(s"lambda added $i to loweredSets")
      loweredSets += i
      ()
    }
  }

  rewrite += rule {
    case Lambda(f, i, o) if i.tp.isInstanceOf[SetType[_]] => {
      implicit val typeA = i.tp.typeArguments(0).asInstanceOf[TypeRep[A]]
      val newI = fresh[Cont[A]]
      subst += i -> newI
      val newO = transformBlockTyped(o)(o.tp, transformType(o.tp)).asInstanceOf[Block[Any]]
      val newF = (x: Rep[Any]) => substitutionContext(newI -> x) {
        inlineBlock(newO)
      }
      loweredSets += newI
      Lambda(newF, newI, newO)
    }
  }

  // Tuple

  // TODO should be generalized
  analysis += statement {
    case sym -> Tuple2New(_1, _2) if mustBeLowered(_2) => {
      loweredSetTuples += sym
      ()
    }
  }

  analysis += statement {
    case sym -> Tuple2_Field__2(self) if loweredSetTuples.contains(self) => {
      loweredSets += sym
      ()
    }
  }

  rewrite += statement {
    case sym -> Tuple2New(node_1, node_2) if mustBeLowered(node_2) => {
      class B
      val _1 = node_1.asInstanceOf[Rep[A]]
      val _2 = apply(node_2).asInstanceOf[Rep[Cont[B]]]
      // System.out.println(s"tp for tuple2: ${sym.tp} ${node_2.tp} ${apply(node_2).tp}")
      implicit val typeA = node_1.tp.asInstanceOf[TypeRep[A]]
      implicit val typeB = apply(node_2).tp.typeArguments(0).asInstanceOf[TypeRep[B]]
      __newTuple2(_1, _2).asInstanceOf[Rep[Any]]
    }
  }

  rewrite += statement {
    case sym -> Tuple2_Field__2(nodeself) if mustBeLowered(sym) => {
      class B
      val self = apply(nodeself).asInstanceOf[Rep[(A, Cont[B])]]
      // System.out.println(s"tp for $nodeself._2=$sym: ${sym.tp} ${nodeself.tp} ${apply(nodeself).tp}")
      implicit val typeA = apply(nodeself).tp.typeArguments(1).asInstanceOf[TypeRep[A]]
      implicit val typeB = sym.tp.typeArguments(0).asInstanceOf[TypeRep[Cont[B]]]
      /*val res = */ self._2.asInstanceOf[Rep[Any]]
      // System.out.println(s"res $res for sym $sym")
      // res
    }
  }

  // Option

  analysis += statement {
    case sym -> OptionApplyObject(v) if mustBeLowered(v) =>
      loweredSetOptions += sym
      ()
  }

  analysis += statement {
    case sym -> OptionGet(v) if loweredSetOptions.contains(v) =>
      loweredSets += sym
      ()
  }

  rewrite += statement {
    case sym -> OptionApplyObject(nodev) if mustBeLowered(nodev) =>
      implicit val typeA = apply(nodev).tp.asInstanceOf[TypeRep[A]]
      val v = apply(nodev).asInstanceOf[Rep[A]]
      Option(v).asInstanceOf[Rep[Any]]
  }

  rewrite += statement {
    case sym -> OptionGet(nodev) if mustBeLowered(sym) =>
      implicit val typeA = apply(nodev).tp.typeArguments(0).asInstanceOf[TypeRep[A]]
      val v = apply(nodev).asInstanceOf[Rep[Option[A]]]
      v.get.asInstanceOf[Rep[Any]]
  }

  // Set

  analysis += statement {
    case sym -> (node @ SetNew2()) => {
      loweredSets += sym
      // System.out.println(s"sym $sym added to sets")
      ()
    }
  }

  rewrite += statement {
    case sym -> (node @ SetNew()) =>

      implicit val typeA = transformType(node.tp.typeArguments(0)).asInstanceOf[TypeRep[A]]

      // headContMap(sym) = __newVar[Cont[A]](unit(null))

      unit(null.asInstanceOf[Any])(node.tp.asInstanceOf[TypeRep[Any]])
  }

  rewrite += statement {
    case sym -> (node @ SetNew2()) =>
      implicit val typeA = transformType(node.tp.typeArguments(0)).asInstanceOf[TypeRep[A]]

      // headContMap(sym) = __newVar[Cont[A]](unit(null))

      // unit(null.asInstanceOf[Any])(node.tp.asInstanceOf[TypeRep[Any]])
      // System.out.println(s"handling $sym added to sets")
      readVar(__newVar[Cont[A]](unit(null)))
  }

  def __newSetLinkedList[A]()(implicit typeA: TypeRep[A]): Rep[Set[A]] = SetNew[A]()

  rewrite += rule {
    case node @ Set$plus$eq(nodeself, nodeelem) if mustBeLowered(nodeself) =>

      val self = nodeself.asInstanceOf[Rep[Set[A]]]
      val elem = nodeelem.asInstanceOf[Rep[A]]
      implicit val typeA = transformType(nodeself.tp.typeArguments(0)).asInstanceOf[TypeRep[A]]

      {
        val prevHead: this.Rep[ch.epfl.data.pardis.shallow.scalalib.collection.Cont[A]] = self.headCont;
        self.headCont_$eq(__newCont(elem, prevHead))
      }
  }

  rewrite += rule {
    case node @ SetForeach(nodeself, nodef) if mustBeLowered(nodeself) =>

      val self = nodeself.asInstanceOf[Rep[Set[A]]]
      val f = nodef.asInstanceOf[Rep[((A) => Unit)]]
      implicit val typeA = transformType(nodeself.tp.typeArguments(0)).asInstanceOf[TypeRep[A]]

      {
        var current: this.Var[ch.epfl.data.pardis.shallow.scalalib.collection.Cont[A]] = __newVar(self.headCont);
        __whileDo(infix_$bang$eq(__readVar(current), unit(null)), {
          val next: this.Rep[ch.epfl.data.pardis.shallow.scalalib.collection.Cont[A]] = __readVar(current).next;
          __app[A, Unit](f).apply(__readVar(current).elem);
          __assign(current, next)
        })
      }
  }

  rewrite += rule {
    case node @ SetRetain(nodeself, nodep) if mustBeLowered(nodeself) =>

      val self = nodeself.asInstanceOf[Rep[Set[A]]]
      val p = nodep.asInstanceOf[Rep[((A) => Boolean)]]
      implicit val typeA = transformType(nodeself.tp.typeArguments(0)).asInstanceOf[TypeRep[A]]

      {
        var prev: this.Var[ch.epfl.data.pardis.shallow.scalalib.collection.Cont[A]] = __newVar(infix_asInstanceOf[ch.epfl.data.pardis.shallow.scalalib.collection.Cont[A]](unit(null)));
        var current: this.Var[ch.epfl.data.pardis.shallow.scalalib.collection.Cont[A]] = __newVar(self.headCont);
        __whileDo(infix_$bang$eq(__readVar(current), unit(null)), {
          __ifThenElse(__app[A, Boolean](p).apply(__readVar(current).elem).unary_$bang, __ifThenElse(infix_$eq$eq(self.headCont, __readVar(current)), self.headCont_$eq(unit(null)), unit(())), {
            __ifThenElse(infix_$bang$eq(__readVar(prev), unit(null)), __readVar(prev).next_$eq(__readVar(current)), unit(()));
            __ifThenElse(infix_$eq$eq(self.headCont, unit(null)), self.headCont_$eq(__readVar(current)), unit(()));
            __assign(prev, __readVar(current))
          });
          __assign(current, __readVar(current).next)
        })
      }
  }

  rewrite += rule {
    case node @ SetExists(nodeself, nodep) if mustBeLowered(nodeself) =>

      val self = nodeself.asInstanceOf[Rep[Set[A]]]
      val p = nodep.asInstanceOf[Rep[((A) => Boolean)]]
      implicit val typeA = transformType(nodeself.tp.typeArguments(0)).asInstanceOf[TypeRep[A]]

      {
        var current: this.Var[ch.epfl.data.pardis.shallow.scalalib.collection.Cont[A]] = __newVar(self.headCont);
        var found: this.Var[Boolean] = __newVar(unit(false));
        __whileDo(infix_$bang$eq(__readVar(current), unit(null)).$amp$amp(__readVar(found).unary_$bang), {
          __ifThenElse(__app[A, Boolean](p).apply(__readVar(current).elem), __assign(found, unit(true)), unit(()));
          __assign(current, __readVar(current).next)
        });
        __readVar(found)
      }
  }

  rewrite += rule {
    case node @ SetHead(nodeself) if mustBeLowered(nodeself) =>

      val self = nodeself.asInstanceOf[Rep[Set[A]]]
      implicit val typeA = transformType(nodeself.tp.typeArguments(0)).asInstanceOf[TypeRep[A]]

      self.headCont.elem
  }

  rewrite += rule {
    case node @ SetFoldLeft(nodeself, nodez, nodeop) if mustBeLowered(nodeself) =>
      class B
      val self = nodeself.asInstanceOf[Rep[Set[A]]]
      val z = nodez.asInstanceOf[Rep[B]]
      val op = nodeop.asInstanceOf[Rep[((B, A) => B)]]
      implicit val typeA = transformType(nodeself.tp.typeArguments(0)).asInstanceOf[TypeRep[A]]
      implicit val typeB = transformType(z.tp).asInstanceOf[TypeRep[B]]

      {
        var acc: this.Var[B] = __newVar(z);
        self.foreach(__lambda(((e: this.Rep[A]) => __assign(acc, __app[B, A, B](op).apply(__readVar(acc), e)))));
        __readVar(acc)
      }
  }

  rewrite += rule {
    case node @ SetSize(nodeself) if mustBeLowered(nodeself) =>

      val self = nodeself.asInstanceOf[Rep[Set[A]]]
      implicit val typeA = transformType(nodeself.tp.typeArguments(0)).asInstanceOf[TypeRep[A]]

      {
        var s: this.Var[Int] = __newVar(unit(0));
        self.foreach(__lambda(((e: this.Rep[A]) => __assign(s, __readVar(s).$plus(unit(1))))));
        __readVar(s)
      }
  }

  // TODO cmp handling should be automatically generated
  rewrite += rule {
    case node @ SetMinBy(nodeself, nodef) if mustBeLowered(nodeself) =>
      class B
      val self = nodeself.asInstanceOf[Rep[Set[A]]]
      val f = nodef.asInstanceOf[Rep[((A) => B)]]

      implicit val typeA = transformType(nodeself.tp.typeArguments(0)).asInstanceOf[TypeRep[A]]
      implicit val typeB = transformType(f.tp.typeArguments(1)).asInstanceOf[TypeRep[B]]

      val cmp = OrderingRep[B]

      {
        var min: this.Var[A] = __newVar(self.head);
        var minValue: this.Var[B] = __newVar(__app[A, B](f).apply(__readVar(min)));
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

  rewrite += rule {
    case node @ SetFind(nodeself, nodep) if mustBeLowered(nodeself) =>

      val self = nodeself.asInstanceOf[Rep[Set[A]]]
      val p = nodep.asInstanceOf[Rep[((A) => Boolean)]]
      implicit val typeA = transformType(nodeself.tp.typeArguments(0)).asInstanceOf[TypeRep[A]]

      {
        var current: this.Var[ch.epfl.data.pardis.shallow.scalalib.collection.Cont[A]] = __newVar(self.headCont);
        var found: this.Var[Boolean] = __newVar(unit(false));
        __whileDo(infix_$bang$eq(__readVar(current), unit(null)).$amp$amp(__readVar(found).unary_$bang), __ifThenElse(__app[A, Boolean](p).apply(__readVar(current).elem), __assign(found, unit(true)), __assign(current, __readVar(current).next)));
        __ifThenElse(__readVar(found).unary_$bang, Option.apply[A](infix_asInstanceOf[A](unit(null))), Option.apply[A](__readVar(current).elem))
      }
  }

}

