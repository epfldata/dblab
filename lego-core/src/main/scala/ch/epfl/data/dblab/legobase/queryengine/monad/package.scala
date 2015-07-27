package ch.epfl.data
package dblab.legobase
package queryengine

import sc.pardis.shallow.{ Record, DynamicCompositeRecord }
import scala.language.implicitConversions

package object monad {
  implicit def queryToJoinableQuery[T <: Record](q: Query[T]): JoinableQuery[T] =
    new JoinableQuery(q.getList)
}
