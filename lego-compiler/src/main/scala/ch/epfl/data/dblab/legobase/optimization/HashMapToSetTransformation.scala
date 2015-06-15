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
import sc.pardis.deep.scalalib._
import sc.pardis.deep.scalalib.collection._

/**
 * A lowering transformation which transforms a HashMap to an Array[Set[_]].
 *
 * @param IR the polymorphic embedding trait which contains the reified program.
 * @param schema the schema information which will be used to estimate the number of buckets
 */
class HashMapToSetTransformation(override val IR: LoweringLegoBase, val schema: Schema) extends HashMapOptimalNoMallocTransformation(IR) with StructCollector[HashMapOps with SetOps with RangeOps with ArrayOps with OptionOps with IntOps with Tuple2Ops] with HashMapBucketAnalyser[HashMapOps with SetOps with RangeOps with ArrayOps with OptionOps with IntOps with Tuple2Ops] {
  import IR._
  override def hashMapExtractKey[A, B](self: Rep[HashMap[A, B]], value: Rep[B])(implicit typeA: TypeRep[A], typeB: TypeRep[B]): Rep[A] = typeB match {
    case t if t.isRecord && t.name.startsWith("AGGRecord") => {
      // class C
      // implicit val typeC = t.asInstanceOf[TypeRep[C]]
      // System.out.println(s"typeRepA: $typeA, record type $t")
      val structDef = getStructDef(t)
      val fieldType = structDef.map(sd => sd.fields.find(_.name == "key").get.tpe.asInstanceOf[TypeRep[A]]).getOrElse(typeA)
      // System.out.println(s"structDef $structDef fieldType $fieldType")
      field[A](value, "key")(fieldType)
      // field[A](value, "key")(value.tp.typeArguments(0).asInstanceOf[TypeRep[A]])
    }
    case t => throw new Exception(s"does not know how to extract key for type $t")
  }

  override def hashMapBuckets[A, B](self: Rep[HashMap[A, B]])(implicit typeA: TypeRep[A], typeB: TypeRep[B]): Rep[Int] = hashMapsStructFieldInfo.get(self) match {
    case Some(fieldInfo) =>
      val numBuckets = schema.stats.getDistinctAttrValues(fieldInfo.field) * 2
      // System.out.println(s"for hashmap type: ${self.tp}, buckets = ${numBuckets}")
      numBuckets
    case _ =>
      // System.out.println(s"for hashmap type: ${self.tp}, couldn't find key")
      unit(1 << 20)
  }
}
