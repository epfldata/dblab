package ch.epfl.data
package dblab.legobase
package optimization

import scala.language.implicitConversions
import schema._
import sc.pardis.ir._
import reflect.runtime.universe.{ TypeTag, Type }
import sc.pardis.optimization._
import deep._
import sc.pardis.types._
import sc.pardis.types.PardisTypeImplicits._
import sc.pardis.deep.scalalib._
import sc.pardis.deep.scalalib.collection._

/**
 * Transforms HashMaps which have no collision in the hash function computation and also
 * in the case that the key has a continuous value into a one dimensional Array.
 *
 * @param LB the polymorphic embedding trait which contains the reified program.
 * @param schema the schema information which will be used to estimate the number of buckets
 */
class HashMapNoCollisionTransformation(val LB: LoweringLegoBase, val schema: Schema) extends sc.pardis.deep.scalalib.collection.HashMapOptimalNoCollisionTransformation(LB) with StructCollector[HashMapOps with RangeOps with ArrayOps with OptionOps with IntOps with Tuple2Ops] with HashMapBucketAnalyser[HashMapOps with RangeOps with ArrayOps with OptionOps with IntOps with Tuple2Ops] {
  import LB._
  override def hashMapExtractKey[A, B](self: Rep[HashMap[A, B]], value: Rep[B])(implicit typeA: TypeRep[A], typeB: TypeRep[B]): Rep[A] = typeB match {
    case t if t.isRecord && t.name.startsWith("AGGRecord") => {
      val structDef = getStructDef(t)
      val fieldType = structDef.map(sd => sd.fields.find(_.name == "key").get.tpe.asInstanceOf[TypeRep[A]]).getOrElse(typeA)
      field[A](value, "key")(fieldType)
    }
    case t => throw new Exception(s"Does not know how to extract key for type $t")
  }

  override def hashMapHash[A, B](self: Rep[HashMap[A, B]], k: Rep[A])(implicit typeA: TypeRep[A], typeB: TypeRep[B]): Rep[Int] = infix_hashCode(k)

  override def hashMapBuckets[A, B](self: Rep[HashMap[A, B]])(implicit typeA: TypeRep[A], typeB: TypeRep[B]): Rep[Int] = hashMapsStructFieldInfo.get(self) match {
    case Some(fieldInfo) =>
      schema.stats.getDistinctAttrValues(fieldInfo.field) * 2
    case _ =>
      unit(1 << 21)
  }
}
