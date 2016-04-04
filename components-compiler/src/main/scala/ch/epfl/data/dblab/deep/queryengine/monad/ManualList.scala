package ch.epfl.data
package dblab
package deep.queryengine.monad

import ch.epfl.data.sc.pardis
import pardis.ir._
import pardis.types.PardisTypeImplicits._
import pardis.effects._
import pardis.deep._
import pardis.deep.scalalib._

trait ListOps extends Base {
  // Type representation
  val ListType = ListIRs.ListType
  type ListType[A] = ListIRs.ListType[A]
  implicit def typeList[A: TypeRep]: TypeRep[List[A]] = ListType(implicitly[TypeRep[A]])
}
object ListIRs extends Base {
  // Type representation
  case class ListType[A](typeA: TypeRep[A]) extends TypeRep[List[A]] {
    def rebuild(newArguments: TypeRep[_]*): TypeRep[_] = ListType(newArguments(0).asInstanceOf[TypeRep[_]])
    // private implicit val tagA = typeA.typeTag
    val name = s"List[${typeA.name}]"
    val typeArguments = List(typeA)

    // val typeTag = scala.reflect.runtime.universe.typeTag[List[A]]
  }
  implicit def typeList[A: TypeRep]: TypeRep[List[A]] = ListType(implicitly[TypeRep[A]])
}
