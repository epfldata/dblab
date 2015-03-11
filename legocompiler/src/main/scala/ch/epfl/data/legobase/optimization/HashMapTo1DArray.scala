package ch.epfl.data
package legobase
package optimization

import scala.language.implicitConversions
import pardis.ir._
import reflect.runtime.universe.{ TypeTag, Type }
import pardis.optimization._
import deep._
import pardis.types.PardisTypeImplicits._
import pardis.effects._
import pardis.deep._
import pardis.deep.scalalib._
import pardis.deep.scalalib.collection._
import pardis.deep.scalalib.io._
class HashMapTo1DArray(override val IR: HashMapOps with RangeOps with ArrayOps with OptionOps with IntOps with Tuple2Ops) extends pardis.optimization.RecursiveRuleBasedTransformer[HashMapOps with RangeOps with ArrayOps with OptionOps with IntOps with Tuple2Ops](IR) {
  import IR._
  type Rep[T] = IR.Rep[T]
  type Var[T] = IR.Var[T]

  class A
  class B

  analysis += rule {
    case node @ HashMapGetOrElseUpdate(nodeself, nodekey, nodeopOutput) if nodeopOutput.tp.name == "AGGRecord_Int" =>
      loweredHashMaps += nodeself
      ()
  }

  val loweredHashMaps = scala.collection.mutable.Set[Rep[Any]]()

  def mustBeLowered[T](sym: Rep[T]): Boolean =
    // sym.asInstanceOf[Sym[T]].id == 649
    loweredHashMaps.contains(sym)

  val lastIndexMap = scala.collection.mutable.Map[Rep[Any], Var[Any]]()
  val tableMap = scala.collection.mutable.Map[Rep[Any], Rep[Any]]()

  private implicit class HashMapRep1[A, B](self: Rep[HashMap[A, B]]) {
    implicit val typeA = transformType(self.tp.typeArguments(0)).asInstanceOf[TypeRep[A]]
    implicit val typeB = transformType(self.tp.typeArguments(1)).asInstanceOf[TypeRep[B]]
    def buckets: Rep[Int] = hashMapBuckets[A, B](self)(typeA, typeB)
    def hash(k: Rep[A]): Rep[Int] = hashMapHash[A, B](self, k)(typeA, typeB)
    def extractKey(value: Rep[B]): Rep[A] = hashMapExtractKey[A, B](self, value)(typeA, typeB)
    def lastIndex_=(x$1: Rep[Int]): Rep[Unit] = hashMap_Field_LastIndex_$eq[A, B](self, x$1)(typeA, typeB)
    def lastIndex: Rep[Int] = hashMap_Field_LastIndex[A, B](self)(typeA, typeB)
    def table: Rep[Array[B]] = hashMap_Field_Table[A, B](self)(typeA, typeB)
  }
  def hashMapBuckets[A, B](self: Rep[HashMap[A, B]])(implicit typeA: TypeRep[A], typeB: TypeRep[B]): Rep[Int] = //unit(1048576)
    unit(49000000)
  def hashMapHash[A, B](self: Rep[HashMap[A, B]], k: Rep[A])(implicit typeA: TypeRep[A], typeB: TypeRep[B]): Rep[Int] = //infix_hashCode(k).$amp(self.table.length.$minus(unit(1)))
    k.asInstanceOf[Rep[Int]]
  def hashMapExtractKey[A, B](self: Rep[HashMap[A, B]], value: Rep[B])(implicit typeA: TypeRep[A], typeB: TypeRep[B]): Rep[A] = ???
  def hashMap_Field_LastIndex_$eq[A, B](self: Rep[HashMap[A, B]], x$1: Rep[Int])(implicit typeA: TypeRep[A], typeB: TypeRep[B]): Rep[Unit] = {
    __assign(lastIndexMap(self), x$1)
  }
  def hashMap_Field_LastIndex[A, B](self: Rep[HashMap[A, B]])(implicit typeA: TypeRep[A], typeB: TypeRep[B]): Rep[Int] = {
    val v = lastIndexMap(self).asInstanceOf[Var[Int]]
    __readVar(v).asInstanceOf[Rep[Int]]
  }
  def hashMap_Field_Table[A, B](self: Rep[HashMap[A, B]])(implicit typeA: TypeRep[A], typeB: TypeRep[B]): Rep[Array[B]] = tableMap(self).asInstanceOf[Rep[Array[B]]]
  rewrite += statement {
    case sym -> (node @ HashMapNew()) if mustBeLowered(sym) =>

      implicit val typeA = transformType(node.tp.typeArguments(0)).asInstanceOf[TypeRep[A]]
      implicit val typeB = transformType(node.tp.typeArguments(1)).asInstanceOf[TypeRep[B]]
      val self = sym.asInstanceOf[Rep[HashMap[A, B]]]

      lastIndexMap(sym) = __newVar[Int](unit(0))
      // tableMap(sym) = infix_asInstanceOf[Array[B]](__newArray[Any](self.buckets))
      tableMap(sym) = __newArray[B](self.buckets)

      unit(null.asInstanceOf[HashMap[A, B]])(node.tp.asInstanceOf[TypeRep[HashMap[A, B]]])
  }

  // def __newHashMapOptimalNoCollision[A, B]()(implicit typeA: TypeRep[A], typeB: TypeRep[B]): Rep[HashMap[A, B]] = HashMapNew[A, B]()

  rewrite += rule {
    case node @ HashMapGetOrElseUpdate(nodeself, nodekey, nodeopOutput) if mustBeLowered(nodeself) =>

      val self = nodeself.asInstanceOf[Rep[HashMap[A, B]]]
      val key = nodekey.asInstanceOf[Rep[A]]
      val op = nodeopOutput.asInstanceOf[Block[B]]
      implicit val typeA = transformType(nodeself.tp.typeArguments(0)).asInstanceOf[TypeRep[A]]
      implicit val typeB = transformType(nodeself.tp.typeArguments(1)).asInstanceOf[TypeRep[B]]

      {
        val h: this.Rep[Int] = self.hash(key);
        val list: this.Rep[B] = self.table.apply(h);
        __ifThenElse(h.$greater(self.lastIndex), self.lastIndex_$eq(h), unit(()));
        __ifThenElse(infix_$bang$eq(list, unit(null)), list, {
          val v: this.Rep[B] = op;
          self.table.update(h, v);
          v
        })
      }
  }

  // rewrite += rule {
  //   case node @ HashMapRemove(nodeself, nodekey) if mustBeLowered(nodeself) =>

  //     val self = nodeself.asInstanceOf[Rep[HashMap[A, B]]]
  //     val key = nodekey.asInstanceOf[Rep[A]]
  //     implicit val typeA = transformType(nodeself.tp.typeArguments(0)).asInstanceOf[TypeRep[A]]
  //     implicit val typeB = transformType(nodeself.tp.typeArguments(1)).asInstanceOf[TypeRep[B]]

  //     {
  //       val h: this.Rep[Int] = self.hash(key);
  //       val list: this.Rep[B] = self.table.apply(h);
  //       __ifThenElse(infix_$bang$eq(list, unit(null)), {
  //         self.table.update(h, infix_asInstanceOf[B](unit(null)));
  //         Option.apply[B](list)
  //       }, Option.apply[B](infix_asInstanceOf[B](unit(null))))
  //     }
  // }

  rewrite += rule {
    case node @ HashMapForeach(nodeself, nodef) if mustBeLowered(nodeself) =>
      class C
      val self = nodeself.asInstanceOf[Rep[HashMap[A, B]]]
      val f = nodef.asInstanceOf[Rep[((Tuple2[A, B]) => C)]]
      implicit val typeA = transformType(nodeself.tp.typeArguments(0)).asInstanceOf[TypeRep[A]]
      implicit val typeB = transformType(nodeself.tp.typeArguments(1)).asInstanceOf[TypeRep[B]]
      implicit val typeC = transformType(f.tp.typeArguments(1)).asInstanceOf[TypeRep[C]]

      Range.apply(unit(0), self.lastIndex.$plus(unit(1))).foreach[Unit](__lambda(((i: this.Rep[Int]) => {
        val list: this.Rep[B] = self.table.apply(i);
        __ifThenElse(infix_$bang$eq(list, unit(null)), {
          __app[Tuple2[A, B], C](f).apply(Tuple2.apply[A, B](infix_asInstanceOf[A](unit(null)), list));
          unit(())
        }, unit(()))
      })))
  }

}

