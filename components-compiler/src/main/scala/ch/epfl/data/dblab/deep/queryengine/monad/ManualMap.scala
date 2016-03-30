package ch.epfl.data
package dblab
package deep.queryengine.monad

import ch.epfl.data.sc.pardis
import pardis.ir._
import pardis.types.PardisTypeImplicits._
import pardis.effects._
import pardis.deep._
import pardis.deep.scalalib._

trait MapOps extends Base {
  // Type representation
  val MapType = MapIRs.MapType
  type MapType[A, B] = MapIRs.MapType[A, B]
  implicit def typeMap[A: TypeRep, B: TypeRep]: TypeRep[Map[A, B]] = MapType(implicitly[TypeRep[A]], implicitly[TypeRep[B]])
}
object MapIRs extends Base {
  // Type representation
  case class MapType[A, B](typeA: TypeRep[A], typeB: TypeRep[B]) extends TypeRep[Map[A, B]] {
    def rebuild(newArguments: TypeRep[_]*): TypeRep[_] = MapType(newArguments(0).asInstanceOf[TypeRep[_]], newArguments(1).asInstanceOf[TypeRep[_]])
    // private implicit val tagA = typeA.typeTag
    // private implicit val tagB = typeB.typeTag
    val name = s"Map[${typeA.name}, ${typeB.name}]"
    val typeArguments = List(typeA, typeB)

    // val typeTag = scala.reflect.runtime.universe.typeTag[Map[A, B]]
  }
  implicit def typeMap[A: TypeRep, B: TypeRep]: TypeRep[Map[A, B]] = MapType(implicitly[TypeRep[A]], implicitly[TypeRep[B]])
}
