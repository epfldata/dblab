package ch.epfl.data
package dblab.legobase
package deep

import sc.pardis.quasi.anf.BaseQuasiExp

/** A polymophic embedding cake which chains all other cakes together */
trait LoweringLegoBase
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

  // TODO should be moved to SC
  /**
   * Reifies the expression comming from a different polymorphic embedding context
   * into the current context
   */
  // def reify[T](exp: => Rep[T]): Rep[T] = {
  //   val expValue = exp
  //   expValue match {
  //     case expSymbol: Sym[Any] if expSymbol.context != this =>
  //       expSymbol.context.scopeDefs.foreach(x => reflectStm(x.asInstanceOf[Stm[Any]]))
  //       expSymbol.context.scopeDefs.last.sym.asInstanceOf[Rep[T]]
  //     case _ => expValue
  //   }
  // }
}
