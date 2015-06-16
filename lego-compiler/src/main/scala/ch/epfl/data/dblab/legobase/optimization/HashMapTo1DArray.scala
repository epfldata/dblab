package ch.epfl.data
package dblab.legobase
package optimization

import scala.language.implicitConversions
import sc.pardis.ir._
import reflect.runtime.universe.{ TypeTag, Type }
import sc.pardis.optimization._
import deep._
import sc.pardis.types.PardisTypeImplicits._
import sc.pardis.effects._
import sc.pardis.deep._
import sc.pardis.deep.scalalib._
import sc.pardis.deep.scalalib.collection._
import sc.pardis.deep.scalalib.io._

/**
 * Transforms HashMaps which have no collision in the hash function computation and also
 * in the case that the key has a continuous value into a one dimensional Array.
 *
 * The main difference with [[HashMapNoCollisionTransformation]] is that this transformer,
 * goes one step further and transforms also the values (which are records which duplicate the key)
 * into other values which do not contain the key anymore. As a result, they save more computation
 * and space.
 *
 * @param IR the polymorphic embedding trait which contains the reified program.
 */
class HashMapTo1DArray[Lang <: HashMapOps with RangeOps with ArrayOps with OptionOps with IntOps with Tuple2Ops](override val IR: Lang) extends sc.pardis.optimization.RuleBasedTransformer[Lang](IR) {
  import IR._
  type Rep[T] = IR.Rep[T]
  type Var[T] = IR.Var[T]

  class A
  class B

  implicit class TypeRepOps[T](tp: TypeRep[T]) {
    def isLoweredRecordType: Boolean =
      potentiallyLoweredHashMaps.exists(_.tp.typeArguments(1) == tp)
  }

  def changeType[T](implicit tp: TypeRep[T]): TypeRep[Any] = {
    tp match {
      case t if t.isLoweredRecordType => //ArrayType(DoubleType)
        DoubleType
      case _ => tp
    }
  }.asInstanceOf[TypeRep[Any]]

  var phase: Phase = _
  sealed trait Phase
  case object FindLoweredRecordType extends Phase
  case object GatherLoweredSymbols extends Phase

  override def analyseProgram[T: TypeRep](node: Block[T]): Unit = {
    phase = FindLoweredRecordType
    traverseBlock(node)
    loweredHashMaps ++= (potentiallyLoweredHashMaps diff invalidLoweredHashMaps)
    phase = GatherLoweredSymbols
    traverseBlock(node)
    System.out.println(flattenedStructValueFields)
  }

  val potentiallyLoweredHashMaps = scala.collection.mutable.Set[Rep[Any]]()
  val invalidLoweredHashMaps = scala.collection.mutable.Set[Rep[Any]]()
  val analysingInputForeach = scala.collection.mutable.Map[Rep[Any], Rep[Any]]()

  /* Phase I: Identifying the types and hashmaps that have the potential to be lowered */

  analysis += rule {
    case node @ HashMapGetOrElseUpdate(nodeself, nodekey, Block(_, struct @ Def(Struct(_, fields, _)))) if phase == FindLoweredRecordType =>
      // TODO maybe can be generalized
      if (nodekey.tp == IntType && fields.exists(_.init == nodekey) && fields.size == 2) {
        potentiallyLoweredHashMaps += nodeself
      }
      ()
  }

  analysis += rule {
    case node @ HashMapForeach(nodeself, Def(Lambda(_, i, o))) if phase == FindLoweredRecordType =>
      analysingInputForeach(i) = nodeself
      traverseBlock(o)
      analysingInputForeach.remove(i)
      ()
  }

  analysis += rule {
    case node @ HashMapRemove(nodeself, nodekey) if phase == FindLoweredRecordType =>
      invalidLoweredHashMaps += nodeself
      ()
  }

  analysis += rule {
    case Tuple2_Field__1(i) if phase == FindLoweredRecordType && analysingInputForeach.contains(i) =>
      invalidLoweredHashMaps += analysingInputForeach(i)
      ()
  }

  /* Phase II: Gathering the symbols that should be lowered */

  val loweredHashMaps = scala.collection.mutable.Set[Rep[Any]]()
  val flattenedStructs = scala.collection.mutable.Set[Rep[Any]]()
  val hashMapElemValue = scala.collection.mutable.Map[Rep[Any], Block[Any]]()
  val flattenedStructKeyField = scala.collection.mutable.Map[TypeRep[Any], String]()
  val flattenedStructValueFields = scala.collection.mutable.Map[TypeRep[Any], List[String]]()

  analysis += rule {
    case node @ HashMapGetOrElseUpdate(nodeself, nodekey, nodeopOutput) if phase == GatherLoweredSymbols && loweredHashMaps.contains(nodeself) =>
      hashMapElemValue(nodeself) = nodeopOutput
      traverseBlock(nodeopOutput)
      ()
  }

  analysis += rule {
    case StructImmutableField(s, _) if phase == GatherLoweredSymbols && s.tp.isLoweredRecordType =>
      flattenedStructs += s
      ()
  }

  analysis += rule {
    case StructFieldGetter(s, _) if phase == GatherLoweredSymbols && s.tp.isLoweredRecordType =>
      flattenedStructs += s
      ()
  }

  analysis += rule {
    case StructFieldSetter(s, _, _) if phase == GatherLoweredSymbols && s.tp.isLoweredRecordType =>
      flattenedStructs += s
      ()
  }

  analysis += statement {
    case sym -> Struct(_, fields, _) if phase == GatherLoweredSymbols && sym.tp.isLoweredRecordType =>
      flattenedStructs += sym
      flattenedStructKeyField += sym.tp -> fields.find(_.init.tp == IntType).get.name
      flattenedStructValueFields += sym.tp -> fields.toList.filter(_ != fields.find(_.init.tp == IntType).get).map(_.name)
      ()
  }

  def mustBeLowered[T](sym: Rep[T]): Boolean =
    loweredHashMaps.contains(sym)

  def isFlattenedStruct[T](sym: Rep[T]): Boolean =
    flattenedStructs.contains(sym)

  def isKeyFieldOfFlattennedStruct[T](sym: Rep[T], name: String): Boolean =
    flattenedStructKeyField(sym.tp.asInstanceOf[TypeRep[Any]]) == name
  def isValueFieldOfFlattennedStruct[T](sym: Rep[T], name: String): Boolean =
    flattenedStructValueFields(sym.tp.asInstanceOf[TypeRep[Any]]).contains(name)

  val hashmapForeachKeyIndex = scala.collection.mutable.Map[Rep[Any], Rep[Int]]()
  val lastIndexMap = scala.collection.mutable.Map[Rep[Any], Var[Any]]()
  val tableMap = scala.collection.mutable.Map[Rep[Any], Rep[Any]]()

  private implicit class HashMapRep1[A, B](self: Rep[HashMap[A, B]]) {
    implicit val typeA = transformType(self.tp.typeArguments(0)).asInstanceOf[TypeRep[A]]
    implicit val typeB = changeType(self.tp.typeArguments(1)).asInstanceOf[TypeRep[B]]
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
      tableMap(sym) = __newArray[B](self.buckets)

      Range.apply(unit(0), self.buckets).foreach[Unit](__lambda(((i: this.Rep[Int]) =>
        self.table(i) = inlineBlock(hashMapElemValue(sym).asInstanceOf[Block[B]]))))

      unit(null.asInstanceOf[HashMap[A, B]])(node.tp.asInstanceOf[TypeRep[HashMap[A, B]]])
  }

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
        list
      }
  }

  rewrite += statement {
    case sym -> Struct(_, fields, _) if isFlattenedStruct(sym) =>
      // TODO can be generalized
      assert(fields.size == 2)
      fields.find(f => f.init.tp != IntType).get.init
  }

  rewrite += rule {
    case node @ StructImmutableField(s, field) if isFlattenedStruct(s) && isValueFieldOfFlattennedStruct(s, field) =>
      apply(s)
  }
  rewrite += rule {
    case node @ StructFieldGetter(s, field) if isFlattenedStruct(s) && isValueFieldOfFlattennedStruct(s, field) =>
      apply(s)
  }
  rewrite += rule {
    case node @ StructFieldSetter(s @ TDef(ArrayApply(arr, i)), field, v) if isFlattenedStruct(s) && isValueFieldOfFlattennedStruct(s, field) =>
      arr(i) = v
  }
  rewrite += rule {
    case node @ StructImmutableField(s, field) if isFlattenedStruct(s) && hashmapForeachKeyIndex.exists(_._1.tp.typeArguments(1) == s.tp) && isKeyFieldOfFlattennedStruct(s, field) =>
      hashmapForeachKeyIndex.find(_._1.tp.typeArguments(1) == s.tp).get._2
  }

  rewrite += rule {
    case node @ HashMapForeach(nodeself, nodef) if mustBeLowered(nodeself) =>
      class C
      val self = nodeself.asInstanceOf[Rep[HashMap[A, B]]]
      val f = nodef.asInstanceOf[Rep[((Tuple2[A, B]) => C)]]
      implicit val typeA = transformType(nodeself.tp.typeArguments(0)).asInstanceOf[TypeRep[A]]
      implicit val typeB = changeType(nodeself.tp.typeArguments(1)).asInstanceOf[TypeRep[B]]
      implicit val typeC = transformType(f.tp.typeArguments(1)).asInstanceOf[TypeRep[C]]

      val index = __newVarNamed[Int](unit(0), "hashmapIndex")
      __whileDo((index: Rep[Int]) < (self.lastIndex + unit(1)), {
        val i = (index: Rep[Int])
        val list: this.Rep[B] = self.table.apply(i);
        hashmapForeachKeyIndex(nodeself) = i
        // TODO rewrite it in a better way
        f match {
          case Def(Lambda(_, i, body)) => {
            val input = body.stmts.head.sym
            substitutionContext(input -> list) {
              body.stmts.tail.foreach(transformStm)
            }
            unit(())
          }
        }
        // hashmapForeachKeyIndex.remove(nodeself)
        __assign(index, i + unit(1))
      })
  }

}

