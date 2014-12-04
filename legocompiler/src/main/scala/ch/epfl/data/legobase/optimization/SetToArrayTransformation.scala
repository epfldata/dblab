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

  // val maxSizeMap = scala.collection.mutable.Map[Rep[Any], Var[Any]]()
  // val arrayMap = scala.collection.mutable.Map[Rep[Any], Var[Any]]()

  val loweredSets = scala.collection.mutable.ArrayBuffer[Rep[Any]]()
  // val loweredSetArrays = scala.collection.mutable.ArrayBuffer[Rep[Any]]()
  // val loweredSetVars = scala.collection.mutable.ArrayBuffer[Rep[Any]]()
  // val loweredSetTuples = scala.collection.mutable.ArrayBuffer[Rep[Any]]()
  // val loweredSetOptions = scala.collection.mutable.ArrayBuffer[Rep[Any]]()
  def mustBeLowered[T](sym: Rep[T]): Boolean =
    loweredSets.contains(sym.asInstanceOf[Rep[Any]])
  def mayBeLowered[T](sym: Rep[T]): Boolean =
    sym.tp.isInstanceOf[SetType[_]]
  def addToLowered[T](sym: Rep[T]): Unit =
    loweredSets += sym.asInstanceOf[Rep[Any]]

  private implicit class SetRep1[A](self: Rep[Set[A]]) {
    implicit val typeA = transformType(self.tp.typeArguments(0)).asInstanceOf[TypeRep[A]]
    def maxSize_=(x$1: Rep[Int]): Rep[Unit] = {
      // __assign(maxSizeMap(self), x$1)
      fieldSetter(self, "maxSize", x$1)
    }
    def maxSize: Rep[Int] = {
      // val v = maxSizeMap(self).asInstanceOf[Var[Int]]
      // __readVar(v).asInstanceOf[Rep[Int]]
      fieldGetter[Int](self, "maxSize")
    }
    def array: Rep[Array[A]] = {
      // val v = arrayMap(self).asInstanceOf[Var[Array[A]]]
      // __readVar(v).asInstanceOf[Rep[Array[A]]]
      field[Array[A]](self, "array")
    }
  }

  // override def transformType[T: PardisType]: PardisType[Any] = ({
  //   val tp = typeRep[T]
  //   tp match {
  //     case SetType(t) => new RecordType(getClassTag[T](tp))
  //     case _          => super.transformType(tp)
  //   }
  // }).asInstanceOf[PardisType[Any]]

  def lowerType[T: PardisType]: PardisType[Any] = ({
    val tp = typeRep[T]
    tp match {
      case SetType(t) => new RecordType(getClassTag[T](tp))
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
  //     implicit val typeB = lowerType(sym.tp.typeArguments(0)).asInstanceOf[TypeRep[B]]
  //     __newArray[B](len).asInstanceOf[Rep[Any]]
  //   }
  // }

  // // Var usage

  // analysis += statement {
  //   case sym -> NewVar(v) if mustBeLowered(v) => {
  //     loweredSetVars += sym
  //     // System.out.println(s"analysis new var $v $sym")
  //     ()
  //   }
  // }

  // analysis += statement {
  //   case sym -> ReadVar(v) if loweredSetVars.contains(v.e) => {
  //     loweredSets += sym
  //     ()
  //   }
  // }

  // rewrite += statement {
  //   case sym -> NewVar(nodev) if loweredSetVars.contains(sym) => {
  //     class B
  //     val v = nodev.asInstanceOf[Rep[B]]
  //     // implicit val typeB = apply(v)(v.tp).correspondingNode.tp.asInstanceOf[TypeRep[B]]
  //     implicit val typeB = lowerType(v.tp).asInstanceOf[TypeRep[B]]
  //     val e = __newVar[B](apply(v)).e
  //     // System.out.println(s"new var $e $v ${apply(v)} ${apply(v).correspondingNode.tp}")
  //     e.asInstanceOf[Rep[Any]]
  //   }
  // }

  // // rewrite += statement {
  // //   case sym -> NewVar(nodev) if !loweredSetVars.contains(sym) => {
  // //     class B
  // //     val v = nodev.asInstanceOf[Rep[B]]
  // //     // implicit val typeB = apply(v)(v.tp).correspondingNode.tp.asInstanceOf[TypeRep[B]]
  // //     implicit val typeB = v.tp.asInstanceOf[TypeRep[B]]
  // //     val e = __newVar[B](apply(v)).e
  // //     System.out.println(s"new var !!! $e $v ${apply(v)} ${apply(v).correspondingNode.tp}")
  // //     e.asInstanceOf[Rep[Any]]
  // //   }
  // // }

  // // Lambda

  // analysis += rule {
  //   case Lambda(f, i, o) if i.tp.isInstanceOf[SetType[_]] => {
  //     // System.out.println(s"lambda added $i to loweredSets")
  //     loweredSets += i
  //     ()
  //   }
  // }

  // rewrite += rule {
  //   case Lambda(f, i, o) if i.tp.isInstanceOf[SetType[_]] => {
  //     implicit val typeA = lowerType(i.tp).asInstanceOf[TypeRep[A]]
  //     val newI = fresh[A]
  //     subst += i -> newI
  //     val newO = transformBlockTyped(o)(o.tp, transformType(o.tp)).asInstanceOf[Block[Any]]
  //     val newF = (x: Rep[Any]) => substitutionContext(newI -> x) {
  //       inlineBlock(newO)
  //     }
  //     loweredSets += newI
  //     Lambda(newF, newI, newO)
  //   }
  // }

  // // Tuple

  // // TODO should be generalized
  // analysis += statement {
  //   case sym -> Tuple2New(_1, _2) if mustBeLowered(_2) => {
  //     loweredSetTuples += sym
  //     ()
  //   }
  // }

  // analysis += statement {
  //   case sym -> Tuple2_Field__2(self) if loweredSetTuples.contains(self) => {
  //     loweredSets += sym
  //     ()
  //   }
  // }

  // rewrite += statement {
  //   case sym -> Tuple2New(node_1, node_2) if loweredSetTuples.contains(sym) => {
  //     class B
  //     val _1 = node_1.asInstanceOf[Rep[A]]
  //     val _2 = node_2.asInstanceOf[Rep[B]]
  //     implicit val typeA = node_1.tp.asInstanceOf[TypeRep[A]]
  //     implicit val typeB = lowerType(node_2.tp).asInstanceOf[TypeRep[B]]
  //     __newTuple2(_1, _2).asInstanceOf[Rep[Any]]
  //   }
  // }

  // // Option

  // analysis += statement {
  //   case sym -> OptionApplyObject(v) if mustBeLowered(v) =>
  //     loweredSetOptions += sym
  //     ()
  // }

  // analysis += statement {
  //   case sym -> OptionGet(v) if loweredSetOptions.contains(v) =>
  //     loweredSets += sym
  //     ()
  // }

  // Set

  analysis += statement {
    case sym -> (node @ SetNew2()) => {
      loweredSets += sym
      ()
    }
  }

  rewrite += statement {
    case sym -> (node @ SetNew()) =>

      implicit val typeA = transformType(node.tp.typeArguments(0)).asInstanceOf[TypeRep[A]]

      // maxSizeMap(sym) = __newVar[Int](unit(0))
      // arrayMap(sym) = __newVar[Array[A]](infix_asInstanceOf[Array[A]](__newArray[Any](unit(256))))

      unit(null.asInstanceOf[Any])(node.tp.asInstanceOf[TypeRep[Any]])
  }

  rewrite += statement {
    case sym -> (node @ SetNew2()) =>
      implicit val typeA = transformType(node.tp.typeArguments(0)).asInstanceOf[TypeRep[A]]

      // headContMap(sym) = __newVar[Cont[A]](unit(null))

      // unit(null.asInstanceOf[Any])(node.tp.asInstanceOf[TypeRep[Any]])
      // System.out.println(s"handling $sym added to sets")
      // readVar(__newVar[Cont[A]](unit(null)))
      val res = __new[Set[A]](
        // ("hm", true, to.__newHashMap3[Any, Any](ho.leftHash.asInstanceOf[Rep[Any => Any]],
        //   newSize)(apply(mc), apply(ma.asInstanceOf[TypeRep[Any]]))),
        ("maxSize", true, unit[Int](0)),
        // ("array", true, infix_asInstanceOf[Array[A]](__newArray[Any](unit(256)))))
        ("array", false, __newArray[A](unit(256))))
      // System.out.println(s"tp ${res.tp.name}")
      res
  }

  // def __newSetArray[A]()(implicit typeA: TypeRep[A]): Rep[Set[A]] = SetNew[A]()

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
