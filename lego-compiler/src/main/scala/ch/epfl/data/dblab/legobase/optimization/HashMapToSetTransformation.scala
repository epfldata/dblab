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
 * This transformation is applied whenever in a HashMap the value contains the
 * key. In this case there is no longer any need to store the key together with
 * the value.
 *
 * This transformation extends the already generated transformation, however
 * here we specify the number of buckets and how to extract a key from a value.
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
 *
 * {{{
 *    val table = new Array[Set[RecordA]]
 *    var currentSize = 0
 *    // Add some element into the lowered HashMap which which modifies currentSize
 *    for (i <- 0 until currentSize) {
 *      val list = table(i)
 *      if (list != null) {
 *        list.foreach(value =>
 *          val key = extractKey(value) // extracts key from the value
 *          process(key, value)
 *        )
 *      }
 *    }
 * }}}
 *
 * Precondition:
 * There should be a functional dependancy between key and value, which means
 * that we can extract key from the value.
 * Furthermore, this lowering is implemented only in the case of using 3 methods
 * of a HashMap: 1) getOrElseUpdate 2) foreach 3) remove.
 * In the other cases this transformation should not be applied.
 *
 * @param IR the polymorphic embedding trait which contains the reified program.
 * @param schema the schema information which will be used to estimate the number of buckets
 */
class HashMapToSetTransformation(override val IR: LegoBaseExp, val schema: Schema)
  extends HashMapOptimalNoMallocTransformation(IR)
  with StructCollector[HashMapOps with SetOps with RangeOps with ArrayOps with OptionOps with IntOps with Tuple2Ops]
  with HashMapBucketAnalyser[HashMapOps with SetOps with RangeOps with ArrayOps with OptionOps with IntOps with Tuple2Ops] {
  import IR._
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
    case t => throw new Exception(s"does not know how to extract key for type $t")
  }

  /**
   * The number of buckets that the generated array should contain.
   */
  override def hashMapBuckets[A, B](self: Rep[HashMap[A, B]])(implicit typeA: TypeRep[A], typeB: TypeRep[B]): Rep[Int] =
    hashMapsStructFieldInfo.get(self) match {
      case Some(fieldInfo) =>
        val numBuckets = schema.stats.getDistinctAttrValues(fieldInfo.field) * 2
        // System.out.println(s"for hashmap type: ${self.tp}, buckets = ${numBuckets}")
        numBuckets
      case _ =>
        // System.out.println(s"for hashmap type: ${self.tp}, couldn't find key")
        unit(1 << 20)
    }
}
