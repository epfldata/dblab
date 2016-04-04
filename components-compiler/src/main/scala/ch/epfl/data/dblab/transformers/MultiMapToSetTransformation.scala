package ch.epfl.data
package dblab
package transformers

import schema._
import scala.language.implicitConversions
import sc.pardis.ir._
import deep.dsls.QueryEngineExp
import reflect.runtime.universe.{ TypeTag, Type }
import sc.pardis.optimization._
import deep._
import sc.pardis.types._
import sc.pardis.types.PardisTypeImplicits._
import sc.pardis.deep.scalalib._
import sc.pardis.deep.scalalib.collection._

class MultiMapToSetTransformation(override val IR: QueryEngineExp, val schema: Schema)
  extends MultiMapOptimalTransformation(IR) {
  import IR._

  override def multiMapHash[A, B](self: Rep[MultiMap[A, B]], k: Rep[A])(implicit typeA: TypeRep[A], typeB: TypeRep[B]): Rep[Int] = {
    k match {
      case Def(StructImmutableField(s, field)) =>
        for (conflicts <- schema.stats.conflicts(field)) {
          schema.stats.conflicts(s.tp.name) = conflicts
        }
      case _ =>
    }
    super.multiMapHash(self, k)
  }
}
