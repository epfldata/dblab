package ch.epfl.data
package dblab
package queryengine

import sc.pardis.shallow.{ Record, DynamicCompositeRecord }
import scala.language.implicitConversions

package object monad {
  implicit def queryToJoinableQuery[T <: Record](q: Query[T]): JoinableQuery[T] =
    new JoinableQuery(q.getList)

  implicit def queryCPSToJoinableQueryCPS[T <: Record](q: QueryCPS[T]): JoinableQueryCPS[T] =
    new JoinableQueryCPS(q)

  implicit def queryUnfoldToJoinableQueryUnfold[T <: Record, Source](q: QueryUnfold[T, Source]): JoinableQueryUnfold[T, Source] =
    new JoinableQueryUnfold(q)

  implicit def queryIteratorToJoinableQueryIterator[T <: Record](q: QueryIterator[T]): JoinableQueryIterator[T] =
    new JoinableQueryIterator(q)

  implicit def queryStreamToJoinableQueryStream[T <: Record](q: QueryStream[T]): JoinableQueryStream[T] =
    new JoinableQueryStream(q)
}
