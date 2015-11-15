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
  with CScalaDSL
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

trait CScalaDSL extends ScalaCoreDSL
  with LegoBaseCLang
  with ch.epfl.data.sc.cscala.deep.DeepCScala

trait ScalaCoreDSL extends CharComponent
  with DoubleComponent
  with IntComponent
  with LongComponent
  with BooleanComponent
  with ArrayComponent
  with SeqComponent
  with PrintStreamComponent
  with OptimalStringComponent
  with K2DBScannerComponent
  with IntegerComponent
  with HashMapComponent
  with SetComponent
  with TreeSetComponent
  with ArrayBufferComponent
  with Tuple2Component
  with Tuple3Component
  with Tuple4Component
  with Tuple9Component
  with MultiMapComponent
  with OptionComponent
  with StringComponent

trait ScalaCoreDSLPartialEvaluation extends ScalaCoreDSL
  with DoublePartialEvaluation
  with IntPartialEvaluation
  with LongPartialEvaluation
  with BooleanPartialEvaluation

trait QOpDSL extends queryengine.push.OperatorsComponent
  with AGGRecordComponent
  with WindowRecordComponent
  with GenericEngineComponent
  with LoaderComponent { this: DeepDSL =>
}

// TODO remove the dependacy of Loader on ManualLifted so that there is no more
// need for the self type.
trait QMonadDSL extends GenericEngineComponent
  with LoaderComponent
  with monad.GroupedQueryComponent
  with monad.QueryComponent
  with monad.JoinableQueryComponent { this: DeepDSL =>
}

trait TPCHQueries extends QueryComponent
  with TPCHLoaderComponent
  with TPCHRecords
  with SynthesizedQueriesComponent { this: DeepDSL =>
}

/** A polymorphic embedding cake which chains all components needed for TPCH queries */
trait DeepDSL extends QOpDSL
  with QMonadDSL
  with ManualLiftedLegoBase
  with TPCHQueries
  with ScalaCoreDSLPartialEvaluation
