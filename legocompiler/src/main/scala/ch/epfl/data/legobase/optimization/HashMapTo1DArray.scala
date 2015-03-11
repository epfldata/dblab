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

  implicit class TypeRepOps[T](tp: TypeRep[T]) {
    def isLoweredRecordType: Boolean = tp.name == "AGGRecord_Int"
  }

  def changeType[T](implicit tp: TypeRep[T]): TypeRep[Any] = {
    tp match {
      case t if t.isLoweredRecordType => ArrayType(DoubleType)
      case _                          => tp
    }
  }.asInstanceOf[TypeRep[Any]]

  analysis += rule {
    case node @ HashMapGetOrElseUpdate(nodeself, nodekey, nodeopOutput) if nodeopOutput.tp.isLoweredRecordType =>
      loweredHashMaps += nodeself
      hashMapElemValue(nodeself) = nodeopOutput
      System.out.println(s"lowered: $nodeself")
      ()
  }

  analysis += rule {
    case StructImmutableField(s, _) if s.tp.isLoweredRecordType =>
      flattennedStructs += s
      ()
  }

  analysis += statement {
    case sym -> Struct(_, _, _) if sym.tp.isLoweredRecordType =>
      flattennedStructs += sym
      ()
  }

  val loweredHashMaps = scala.collection.mutable.Set[Rep[Any]]()
  val flattennedStructs = scala.collection.mutable.Set[Rep[Any]]()
  val hashMapElemValue = scala.collection.mutable.Map[Rep[Any], Rep[Any]]()

  def mustBeLowered[T](sym: Rep[T]): Boolean =
    // sym.asInstanceOf[Sym[T]].id == 649
    loweredHashMaps.contains(sym)

  def isFlattennedStruct[T](sym: Rep[T]): Boolean =
    flattennedStructs.contains(sym)

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
      implicit val typeB = changeType(node.tp.typeArguments(1)).asInstanceOf[TypeRep[B]]
      val self = sym.asInstanceOf[Rep[HashMap[A, B]]]

      lastIndexMap(sym) = __newVar[Int](unit(0))
      // tableMap(sym) = infix_asInstanceOf[Array[B]](__newArray[Any](self.buckets))
      tableMap(sym) = __newArray[B](self.buckets)

      unit(null.asInstanceOf[HashMap[A, B]])(node.tp.asInstanceOf[TypeRep[HashMap[A, B]]])
  }

  // def __newHashMapOptimalNoCollision[A, B]()(implicit typeA: TypeRep[A], typeB: TypeRep[B]): Rep[HashMap[A, B]] = HashMapNew[A, B]()

  var keyIndex: Rep[Int] = _

  rewrite += rule {
    case node @ HashMapGetOrElseUpdate(nodeself, nodekey, nodeopOutput) if mustBeLowered(nodeself) =>

      val self = nodeself.asInstanceOf[Rep[HashMap[A, B]]]
      val key = nodekey.asInstanceOf[Rep[A]]
      val op = nodeopOutput.asInstanceOf[Block[B]]
      implicit val typeA = transformType(nodeself.tp.typeArguments(0)).asInstanceOf[TypeRep[A]]
      implicit val typeB = changeType(nodeself.tp.typeArguments(1)).asInstanceOf[TypeRep[B]]

      {
        val h: this.Rep[Int] = self.hash(key);
        val list: this.Rep[B] = self.table.apply(h);
        __ifThenElse(h.$greater(self.lastIndex), self.lastIndex_$eq(h), unit(()));
        // __ifThenElse(infix_$bang$eq(list, unit(null)), list, {
        //   val v: this.Rep[B] = inlineBlock(op);
        //   self.table.update(h, v);
        //   v
        // })
        list
      }
  }

  rewrite += statement {
    case sym -> Struct(_, fields, _) if isFlattennedStruct(sym) =>
      fields.find(f => f.name == "aggs").get.init
  }

  rewrite += rule {
    case node @ StructImmutableField(s, "aggs") if isFlattennedStruct(s) =>
      apply(s)
  }
  rewrite += rule {
    case node @ StructImmutableField(s, "key") if isFlattennedStruct(s) && keyIndex != null =>
      keyIndex
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
      implicit val typeB = changeType(nodeself.tp.typeArguments(1)).asInstanceOf[TypeRep[B]]
      implicit val typeC = transformType(f.tp.typeArguments(1)).asInstanceOf[TypeRep[C]]

      Range.apply(unit(0), self.lastIndex.$plus(unit(1))).foreach[Unit](__lambda(((i: this.Rep[Int]) => {
        val list: this.Rep[B] = self.table.apply(i);
        keyIndex = i
        // __ifThenElse(infix_$bang$eq(list, unit(null)), {
        //   __app[Tuple2[A, B], C](f).apply(Tuple2.apply[A, B](infix_asInstanceOf[A](unit(null)), list));
        //   unit(())
        // }, unit(()))
        __ifThenElse(infix_$bang$eq(list, unit(null)), {
          f match {
            case Def(Lambda(_, i, body)) => {
              val input = body.stmts.head.sym
              substitutionContext(input -> list) {
                body.stmts.tail.foreach(transformStm)
              }
              unit(())
            }
          }
          unit(())
        }, unit(()))
      })))
  }

}

