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
// import pardis.deep.scalalib.collection._

class HashMapNoCollisionTransformation(val LB: LoweringLegoBase, val queryNumber: Int) extends pardis.deep.scalalib.collection.HashMapOptimalNoCollisionTransformation(LB) with StructCollector[ch.epfl.data.pardis.deep.scalalib.collection.HashMapOps with ch.epfl.data.pardis.deep.scalalib.collection.RangeOps with ch.epfl.data.pardis.deep.scalalib.ArrayOps with ch.epfl.data.pardis.deep.scalalib.OptionOps with ch.epfl.data.pardis.deep.scalalib.IntOps with ch.epfl.data.pardis.deep.scalalib.Tuple2Ops] {
  import LB._
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

  override def hashMapHash[A, B](self: Rep[HashMap[A, B]], k: Rep[A])(implicit typeA: TypeRep[A], typeB: TypeRep[B]): Rep[Int] = infix_hashCode(k) //.$amp(self.table.length.$minus(unit(1)))

  override def hashMapBuckets[A, B](self: Rep[HashMap[A, B]])(implicit typeA: TypeRep[A], typeB: TypeRep[B]): Rep[Int] = queryNumber match {
    case 12      => unit(16)
    case 10 | 13 => unit(1 << 21)
    case 8       => unit(2000)
    // case 18 => unit(49000000)
    case _       => unit(1 << 20)
  }
}
