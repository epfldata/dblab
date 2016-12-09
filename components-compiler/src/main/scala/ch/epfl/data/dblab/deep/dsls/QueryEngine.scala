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
import squid.scback.PardisBinding

/** A polymophic embedding cake which chains all other cakes together */
abstract class QueryEngineExp
  extends CScalaDSL
  with ScalaCoreDSLInlined
  with QPlanDSLInlined
  with QMonadDSLInlined
  with BaseQuasiExp
  with PardisBinding.DefaultPardisMixin {
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

  // Tells Squid's AutoBinder not to try and generate bindings for these (they have problems):
  protected type `ignore java.lang.Exception.<init>` = Nothing
  protected type `ignore ch.epfl.data.dblab.deep.queryengine.push.LeftOuterJoinOpOps.LeftOuterJoinOpRep` = Nothing
  protected type `ignore ch.epfl.data.dblab.deep.queryengine.push.ViewOpOps.ViewOpRep` = Nothing
  protected type `ignore ch.epfl.data.dblab.deep.queryengine.push.HashJoinAntiOps.HashJoinAntiRep` = Nothing
  protected type `ignore ch.epfl.data.dblab.deep.queryengine.push.MergeJoinOpOps.MergeJoinOpRep` = Nothing
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
