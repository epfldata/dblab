package ch.epfl.data
package dblab
package deep
package common

import ch.epfl.data.sc.pardis
import pardis.ir._
import pardis.types.PardisTypeImplicits._
import pardis.effects._
import pardis.deep.scalalib.CharComponent

// TODO should be removed by adding toInt to Char in the standard library
/** A polymorphic embedding cake containing additional manually lifted methods for Char */
trait CharExtraOps extends CharComponent {
  implicit class CharOps2(c: Rep[Char]) {
    def toInt: Rep[Int] = c.asInstanceOf[Rep[Int]]
  }
}
