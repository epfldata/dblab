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

object SetArrayTransformation extends TransformerHandler {
  def apply[Lang <: Base, T: PardisType](context: Lang)(block: context.Block[T]): context.Block[T] = {
    new SetArrayTransformation(context.asInstanceOf[LoweringLegoBase]).optimize(block)
  }
}

class SetArrayTransformation[Lang <: SetComponent with pardis.deep.scalalib.OptionComponent with pardis.deep.scalalib.Tuple2Component with pardis.deep.scalalib.ArrayComponent with pardis.deep.scalalib.IntComponent with pardis.deep.scalalib.BooleanComponent with ContOps with ManualLiftedLegoBase](
  override val IR: Lang) extends pardis.optimization.RecursiveRuleBasedTransformer[Lang](IR)
  with ArrayEscapeLowering[Lang]
  with VarEscapeLowering[Lang]
  with Tuple2EscapeLowering[Lang]
  with OptionEscapeLowering[Lang]
  with LambdaEscapeLowering[Lang]
  with RuleBasedLowering[Lang] {
  import IR._
  type Rep[T] = IR.Rep[T]
  type Var[T] = IR.Var[T]

  class A

  val loweredSets = scala.collection.mutable.ArrayBuffer[Rep[Any]]()
  def mustBeLowered[T](sym: Rep[T]): Boolean =
    loweredSets.contains(sym.asInstanceOf[Rep[Any]])
  def mayBeLowered[T](sym: Rep[T]): Boolean =
    sym.tp.isInstanceOf[SetType[_]]
  def addToLowered[T](sym: Rep[T]): Unit =
    loweredSets += sym.asInstanceOf[Rep[Any]]

  private implicit class SetRep1[A](self: Rep[Set[A]]) {
    implicit val typeA = transformType(self.tp.typeArguments(0)).asInstanceOf[TypeRep[A]]
    def maxSize_=(x$1: Rep[Int]): Rep[Unit] = {
      fieldSetter(self, "maxSize", x$1)
    }
    def maxSize: Rep[Int] = {
      fieldGetter[Int](self, "maxSize")
    }
    def array: Rep[Array[A]] = {
      field[Array[A]](self, "array")
    }
  }

  def lowerType[T: PardisType]: PardisType[Any] = ({
    val tp = typeRep[T]
    tp match {
      case SetType(t) => new RecordType(getClassTag[T](tp))
      case _          => tp
    }
  }).asInstanceOf[PardisType[Any]]

  // Set

  analysis += statement {
    case sym -> (node @ SetApplyObject2()) => {
      loweredSets += sym
      ()
    }
  }

  rewrite += statement {
    case sym -> (node @ SetNew()) =>

      implicit val typeA = transformType(node.tp.typeArguments(0)).asInstanceOf[TypeRep[A]]

      unit(null.asInstanceOf[Any])(node.tp.asInstanceOf[TypeRep[Any]])
  }

  rewrite += statement {
    case sym -> (node @ SetApplyObject2()) =>
      implicit val typeA = transformType(node.tp.typeArguments(0)).asInstanceOf[TypeRep[A]]

      __new[Set[A]](
        ("maxSize", true, unit[Int](0)),
        ("array", false, __newArray[A](unit(256))))
  }

  rewrite += rule {
    case node @ Set$plus$eq(nodeself, nodeelem) if mustBeLowered(nodeself) =>

      val self = nodeself.asInstanceOf[Rep[Set[A]]]
      val elem = nodeelem.asInstanceOf[Rep[A]]
      implicit val typeA = transformType(nodeself.tp.typeArguments(0)).asInstanceOf[TypeRep[A]]

      {
        self.array.update(self.maxSize, elem);
        self.maxSize_$eq(self.maxSize.$plus(unit(1)))
      }
  }

  rewrite += rule {
    case node @ SetForeach(nodeself, nodef) if mustBeLowered(nodeself) =>

      val self = nodeself.asInstanceOf[Rep[Set[A]]]
      val f = nodef.asInstanceOf[Rep[((A) => Unit)]]
      implicit val typeA = transformType(nodeself.tp.typeArguments(0)).asInstanceOf[TypeRep[A]]

      Range.apply(unit(0), self.maxSize).foreach[Unit](__lambda(((i: this.Rep[Int]) => {
        val elem: this.Rep[A] = self.array.apply(i);
        __app[A, Unit](f).apply(elem)
      })))
  }

  rewrite += rule {
    case node @ SetRetain(nodeself, nodep) if mustBeLowered(nodeself) =>

      val self = nodeself.asInstanceOf[Rep[Set[A]]]
      val p = nodep.asInstanceOf[Rep[((A) => Boolean)]]
      implicit val typeA = transformType(nodeself.tp.typeArguments(0)).asInstanceOf[TypeRep[A]]

      {
        var lastIndex: this.Var[Int] = __newVar(unit(0));
        Range.apply(unit(0), self.maxSize).foreach[Unit](__lambda(((i: this.Rep[Int]) => {
          val elem: this.Rep[A] = self.array.apply(i);
          __ifThenElse(infix_$bang$eq(elem, unit(null)).$amp$amp(__app[A, Boolean](p).apply(elem).unary_$bang), self.array.update(i, infix_asInstanceOf[A](unit(null))), {
            self.array.update(__readVar(lastIndex), elem);
            __assign(lastIndex, __readVar(lastIndex).$plus(unit(1)))
          })
        })));
        self.maxSize_$eq(__readVar(lastIndex))
      }
  }

  rewrite += rule {
    case node @ SetExists(nodeself, nodep) if mustBeLowered(nodeself) =>

      val self = nodeself.asInstanceOf[Rep[Set[A]]]
      val p = nodep.asInstanceOf[Rep[((A) => Boolean)]]
      implicit val typeA = transformType(nodeself.tp.typeArguments(0)).asInstanceOf[TypeRep[A]]

      {
        var i: this.Var[Int] = __newVar(unit(0));
        var found: this.Var[Boolean] = __newVar(unit(false));
        __whileDo(__readVar(i).$less(self.maxSize).$amp$amp(__readVar(found).unary_$bang), {
          __ifThenElse(__app[A, Boolean](p).apply(self.array.apply(__readVar(i))), __assign(found, unit(true)), unit(()));
          __assign(i, __readVar(i).$plus(unit(1)))
        });
        __readVar(found)
      }
  }

  rewrite += rule {
    case node @ SetHead(nodeself) if mustBeLowered(nodeself) =>

      val self = nodeself.asInstanceOf[Rep[Set[A]]]
      implicit val typeA = transformType(nodeself.tp.typeArguments(0)).asInstanceOf[TypeRep[A]]

      self.array.apply(unit(0))
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

      self.maxSize
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
        var i: this.Var[Int] = __newVar(unit(0));
        var found: this.Var[Boolean] = __newVar(unit(false));
        __whileDo(__readVar(i).$less(self.maxSize).$amp$amp(__readVar(found).unary_$bang), __ifThenElse(__app[A, Boolean](p).apply(self.array.apply(__readVar(i))), __assign(found, unit(true)), __assign(i, __readVar(i).$plus(unit(1)))));
        __ifThenElse(__readVar(found).unary_$bang, Option.apply[A](infix_asInstanceOf[A](unit(null))), Option.apply[A](self.array.apply(__readVar(i))))
      }
  }

}
