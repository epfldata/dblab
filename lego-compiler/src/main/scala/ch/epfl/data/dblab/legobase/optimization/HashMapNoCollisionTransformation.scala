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
 * The assumptions for this transformation is similar to [[HashMapToSetTransformation]].
 * In addition, we the assumption that there is no collision.
 *
 * Example:
 * {{{
 *    // RecordA { key: Int, fieldA: Int, fieldB: String }
 *    val hm = new HashMap[Int, RecordA]
 *    // Add some element into the HashMap
 *    hm.foreach( { case (key, value) =>
 *      process(key, value)
 *    })
 * }}}
 * is converted to:
 * {{{
 *    val table = new Array[RecordA]
 *    var currentSize = 0
 *    // Add some element into the lowered HashMap
 *    for (i <- 0 until currentSize) {
 *      val value = table(i)
 *      if (value != null) {
 *        val key = extractKey(value) // extracts key from the value
 *        process(value, elem)
 *      }
 *    }
 * }}}
 *
 * Precondition:
 * 1) There should be a functional dependancy between key and value, which means
 * that we can extract key from the value.
 * 2) Furthermore, this lowering is implemented only in the case of using 3 methods
 * of a HashMap: a) getOrElseUpdate b) foreach c) remove.
 * In the other cases this transformation should not be applied.
 * 3) There should be no collision between the hashing function computed using the
 * keys.
 *
 * @param LB the polymorphic embedding trait which contains the reified program.
 * @param schema the schema information which will be used to estimate the number of buckets
 */
class HashMapNoCollisionTransformation(val LB: LoweringLegoBase, val schema: Schema) extends sc.pardis.deep.scalalib.collection.HashMapOptimalNoCollisionTransformation(LB) with StructCollector[HashMapOps with RangeOps with ArrayOps with OptionOps with IntOps with Tuple2Ops] with HashMapBucketAnalyser[HashMapOps with RangeOps with ArrayOps with OptionOps with IntOps with Tuple2Ops] {
  import LB._
  /**
   * Specifies the function that is used for extracting the key from the value.
   * Keep in mind that the assumption for this transformation is that the value
   * contains the value of key. Hence there is no need to store the key together
   * with the value, and storing only value is enough.
   */
  override def hashMapExtractKey[A, B](self: Rep[HashMap[A, B]], value: Rep[B])(implicit typeA: TypeRep[A], typeB: TypeRep[B]): Rep[A] = typeB match {
    case t if t.isRecord && t.name.startsWith("AGGRecord") => {
      val structDef = getStructDef(t)
      val fieldType = structDef.map(sd => sd.fields.find(_.name == "key").get.tpe.asInstanceOf[TypeRep[A]]).getOrElse(typeA)
      field[A](value, "key")(fieldType)
    }
    case t => throw new Exception(s"Does not know how to extract key for type $t")
  }

  /**
   * The hashing method should be used. As we know that here we do not have
   * any collision, and also we know that we have allocated enough amount of
   * buckets, there is no need to compute modulo of the hashCode value.
   */
  override def hashMapHash[A, B](self: Rep[HashMap[A, B]], k: Rep[A])(implicit typeA: TypeRep[A], typeB: TypeRep[B]): Rep[Int] = infix_hashCode(k)

  /**
   * The number of buckets that the generated array should contain.
   */
  override def hashMapBuckets[A, B](self: Rep[HashMap[A, B]])(implicit typeA: TypeRep[A], typeB: TypeRep[B]): Rep[Int] = hashMapsStructFieldInfo.get(self) match {
    case Some(fieldInfo) =>
      schema.stats.getDistinctAttrValues(fieldInfo.field) * 2
    case _ =>
      unit(1 << 21)
  }
}
