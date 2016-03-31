package ch.epfl.data
package dblab
package deep
package dsls

import sc.pardis.quasi.anf.BaseQuasiExp
import sc.pardis.ir._
import sc.pardis.types.PardisTypeImplicits._
import sc.pardis.deep._
import sc.pardis.language._
import sc.pardis.deep.scalalib._
import sc.pardis.deep.scalalib.collection._
import sc.pardis.deep.scalalib.io._
import sc.pardis.quasi.anf.BaseQuasiExt
import dblab.deep.queryengine._
import dblab.deep.storagemanager._

/** A polymophic embedding cake which chains all other cakes together */
abstract class QueryEngineExp
  extends CScalaDSL
  with QPlanDSL
  with QMonadDSL
  with BaseQuasiExp {
  /**
   * Keeps the link between the lowered symbols and the original (higher level) symbol node definition
   *
   * For example, in the case of lowering a LeftOuterJoinOpNew node to a MultiMapNew node, bound to the symbol `x`,
   * this map keeps the link `x` -> LeftOuterJoinOpNew
   *
   */
  val loweredSymbolToOriginalDef = scala.collection.mutable.Map[Rep[Any], Def[Any]]()

  def addLoweredSymbolOriginalDef[T, S](sym: Rep[T], originalDef: Def[S]): Unit = {
    loweredSymbolToOriginalDef += sym.asInstanceOf[Rep[Any]] -> originalDef.asInstanceOf[Def[Any]]
  }

  def getLoweredSymbolOriginalDef[T](sym: Rep[T]): Option[Def[Any]] = {
    loweredSymbolToOriginalDef.get(sym.asInstanceOf[Rep[Any]])
  }
}

abstract class QueryEngineExt extends BaseQuasiExt
  with NumericExtOps
  with ByteExtOps
  with SetExtOps
  with PairExtOps
  with ArrayBufferExtOps
  with ArrayExtOps
  with RangeExtOps
  with BooleanExtOps
  with MultiMapExtOps
  with OptionExtOps
  with HashMapExtOps
