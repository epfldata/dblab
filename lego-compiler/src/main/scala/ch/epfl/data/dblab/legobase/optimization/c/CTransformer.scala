package ch.epfl.data
package dblab.legobase
package optimization
package c

import scala.language.existentials
import sc.pardis.shallow.OptimalString
import sc.pardis.ir._
import sc.pardis.types._
import sc.pardis.types.PardisTypeImplicits._
import sc.pardis.deep.scalalib._
import sc.pardis.deep.scalalib.collection._
import dblab.legobase.deep._
import sc.pardis.optimization._
import scala.language.implicitConversions
import sc.pardis.utils.Utils._
import scala.reflect.runtime.universe
import sc.pardis.ir.StructTags._
import sc.cscala.CLangTypesDeep._
import sc.cscala.GLibTypes._

/**
 * Common trait for all C transformers.
 */
trait CTransformer extends TopDownTransformerTraverser[LegoBaseExp] {
  val IR: LegoBaseExp
  import IR._
  import CNodes._

  implicit class PointerTypeOps[T](tp: TypeRep[T]) {
    def isPointerType: Boolean = tp match {
      case x: CTypes.PointerType[_] => true
      case _                        => false
    }
  }

  override def transformExp[T: TypeRep, S: TypeRep](exp: Rep[T]): Rep[S] = exp match {
    case t: typeOf[_] => typeOf()(apply(t.tp)).asInstanceOf[Rep[S]]
    case _            => super.transformExp[T, S](exp)
  }
}
