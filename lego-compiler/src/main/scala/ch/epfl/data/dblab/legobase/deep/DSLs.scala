package ch.epfl.data
package dblab.legobase
package deep

import sc.pardis.quasi.anf.BaseQuasiExp
import sc.pardis.ir._
import sc.pardis.types.PardisTypeImplicits._
import sc.pardis.deep._
import sc.pardis.deep.scalalib._
import sc.pardis.deep.scalalib.collection._
import sc.pardis.deep.scalalib.io._
import dblab.legobase.deep.queryengine._
import dblab.legobase.deep.storagemanager._
import dblab.legobase.deep.tpch._

/** A polymophic embedding cake which chains all other cakes together */
class LegoBaseExp
  extends InliningLegoBase
  with LegoBaseCLang
  with ch.epfl.data.sc.cscala.deep.DeepCScala
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

trait ScalaCoreDSL extends CharComponent
  with DoubleComponent
  with IntComponent
  with LongComponent
  with BooleanComponent
  // with DoublePartialEvaluation
  // with IntPartialEvaluation
  // with LongPartialEvaluation
  // with BooleanPartialEvaluation
  with ArrayComponent
  with SeqComponent
  with PrintStreamComponent
  // with Q1GRPRecordComponent
  // with Q3GRPRecordComponent
  // with Q7GRPRecordComponent
  // with Q9GRPRecordComponent
  // with Q10GRPRecordComponent
  // with Q13IntRecordComponent
  // with Q16GRPRecord1Component
  // with Q16GRPRecord2Component
  // with Q18GRPRecordComponent
  // with Q20GRPRecordComponent
  // with GenericEngineComponent
  // with LINEITEMRecordComponent
  // with SUPPLIERRecordComponent
  // with PARTSUPPRecordComponent
  // with REGIONRecordComponent
  // with NATIONRecordComponent
  // with PARTRecordComponent
  // with CUSTOMERRecordComponent
  // with ORDERSRecordComponent
  with OptimalStringComponent
  // with LoaderComponent
  with K2DBScannerComponent
  with IntegerComponent
  with HashMapComponent
  with SetComponent
  with TreeSetComponent
  with ArrayBufferComponent
  // with ManualLiftedLegoBase
  // with QueryComponent
  with Tuple2Component
  with Tuple3Component
  with Tuple4Component
  with Tuple9Component
  with MultiMapComponent
  with OptionComponent
  with StringComponent
// with SynthesizedQueriesComponent
// with TPCHLoaderComponent
// with monad.GroupedQueryComponent
// with monad.QueryComponent
// with monad.JoinableQueryComponent

trait ScalaCoreDSLPartialEvaluation extends ScalaCoreDSL
  with DoublePartialEvaluation
  with IntPartialEvaluation
  with LongPartialEvaluation
  with BooleanPartialEvaluation

