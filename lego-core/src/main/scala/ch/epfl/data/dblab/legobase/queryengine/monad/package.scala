package ch.epfl.data
package dblab.legobase
package queryengine

import sc.pardis.shallow.{ Record, DynamicCompositeRecord }
import scala.language.implicitConversions

package object monad {
  implicit def queryToJoinableQuery[T <: Record](q: Query[T]): JoinableQuery[T] =
    new JoinableQuery(q.getList)

  implicit def queryCPSToJoinableQueryCPS[T <: Record](q: QueryCPS[T]): JoinableQueryCPS[T] =
    new JoinableQueryCPS(q)

  implicit def queryIteratorToJoinableQueryIterator[T <: Record, Source](q: QueryIterator[T, Source]): JoinableQueryIterator[T, Source] =
    new JoinableQueryIterator(q)

  implicit def queryUnfoldToJoinableQueryUnfold[T <: Record](q: QueryUnfold[T]): JoinableQueryUnfold[T] =
    new JoinableQueryUnfold(q)

  implicit def queryStreamToJoinableQueryStream[T <: Record](q: QueryStream[T]): JoinableQueryStream[T] =
    new JoinableQueryStream(q)
}
