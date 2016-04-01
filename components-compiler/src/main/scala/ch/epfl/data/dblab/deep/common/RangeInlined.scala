package ch.epfl.data
package dblab
package deep
package common

import scala.language.implicitConversions
import sc.pardis.ir._
import sc.pardis.types.PardisTypeImplicits._
import sc.pardis.deep.scalalib.collection.RangeComponent

/** A polymorphic embedding cake for performing loop unrolling in the case of using var args. */
trait RangeInlined extends RangeComponent {
  // TODO should be handled by Purgatory by adding more annotations
  override def __newRange(start: Rep[Int], end: Rep[Int], step: Rep[Int]): Rep[Range] = step match {
    case Constant(1) => Range(start, end)
    case _           => super.__newRange(start, end, step)
  }

}
