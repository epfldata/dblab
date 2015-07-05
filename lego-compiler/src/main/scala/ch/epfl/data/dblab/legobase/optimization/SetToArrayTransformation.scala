package ch.epfl.data
package dblab.legobase
package optimization

import schema._
import scala.language.implicitConversions
import sc.pardis.ir._
import reflect.runtime.universe.{ TypeTag, Type }
import sc.pardis.optimization._
import deep._
import sc.pardis.types._
import sc.pardis.types.PardisTypeImplicits._
import sc.pardis.deep.scalalib.collection._

object SetToArrayTransformation {
  import sc.pardis.deep.scalalib._
  import sc.pardis.deep.scalalib.collection._
  type Lang = SetOps with RangeOps with ArrayOps with IntOps with BooleanOps with OptionOps
}

import SetToArrayTransformation.Lang

class SetToArrayTransformation(
  override val IR: Lang, val schema: Schema) extends sc.pardis.deep.scalalib.collection.SetArrayTransformation(IR)
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
      case SetType(t) => new RecordType(getClassTag[T](tp), Some(tp))
      case _          => tp
    }
  }).asInstanceOf[PardisType[Any]]

  analysis += statement {
    case sym -> (node @ SetApplyObject2()) => {
      loweredSets += sym
      ()
    }
  }

  override def postAnalyseProgram[T: TypeRep](node: Block[T]): Unit = {
    for (sym <- loweredSets) {
      val originalType = sym.tp.typeArguments(0).name
      val loweredType = lowerType(sym.tp).name
      val arrayType = "Array_" + originalType
      schema.stats.addDependency(loweredType, originalType, x => x)
      schema.stats.addDependency(arrayType, originalType, x => x)
    }
  }

  rewrite += statement {
    case sym -> (node @ SetApplyObject2()) =>
      implicit val typeA = transformType(node.tp.typeArguments(0)).asInstanceOf[TypeRep[A]]

      __new[Set[A]](
        ("maxSize", true, unit[Int](0)),
        ("array", false, __newArray[A](unit(256))))
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
