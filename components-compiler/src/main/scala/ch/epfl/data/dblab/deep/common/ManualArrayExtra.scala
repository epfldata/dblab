package ch.epfl.data
package dblab
package deep
package common

import ch.epfl.data.sc.pardis
import pardis.ir._
import pardis.types.PardisTypeImplicits._
import pardis.effects._
import pardis.deep.scalalib.ArrayComponent
import pardis.deep.scalalib.collection.RangeComponent

/** A polymorphic embedding cake containing additional manually lifted methods for Array */
trait ArrayExtraOps extends RangeComponent with ArrayComponent {

  // TODO may be should be moved to SC?
  def array_foreach[T: TypeRep](arr: Rep[Array[T]], f: Rep[T] => Rep[Unit]): Rep[Unit] = {
    Range(unit(0), arr.length).foreach {
      __lambda { i =>
        val e = arr(i)
        f(e)
      }
    }
  }

  def byteArrayOps(arr: Rep[Array[Byte]]): Rep[Array[Byte]] = arr
}
