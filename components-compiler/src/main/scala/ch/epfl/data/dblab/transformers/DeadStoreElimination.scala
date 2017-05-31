package ch.epfl.data
package dblab
package transformers

import utils.Logger
import schema._
import sc.pardis
import scala.language.implicitConversions
import sc.pardis.ir._
import deep.dsls.QueryEngineExp
import reflect.runtime.universe.{ TypeTag, Type }
import sc.pardis.optimization._
import deep._
import sc.pardis.types._
import sc.pardis.types.PardisTypeImplicits._
import sc.pardis.shallow.utils.DefaultValue
import sc.pardis.quasi.anf._
import quasi._
import scala.collection.mutable

class DeadStoreElimination(override val IR: QueryEngineExp)
  extends RuleBasedTransformer[QueryEngineExp](IR) {
  import IR._

  val dsAnalysis = new DeadStoreAnalysis(IR)

  override def analyseProgram[T: TypeRep](node: Block[T]): Unit = {
    dsAnalysis.traverse(node)
  }

  rewrite += remove {
    case Assign(v, value) if !dsAnalysis.isUsed(v) =>
      ()
  }

  rewrite += remove {
    case ReadVar(v) if !dsAnalysis.isUsed(v) =>
      ()
  }

  rewrite += removeStatement {
    case sym -> NewVar(init) if !dsAnalysis.isUsed(Var(sym.asInstanceOf[Rep[Var[Any]]])) =>
      ()
  }
}

class DeadStoreAnalysis(override val IR: QueryEngineExp) extends BackwardAnalysis[QueryEngineExp] {
  import IR._

  val logger = Logger[DeadStoreAnalysis]

  // val assignedVars = new mutable.ArrayBuffer[Var[Any]]
  val usedSymbols = new mutable.HashSet[Sym[_]]
  val usedVars = new mutable.HashSet[Var[_]]
  val allVars = new mutable.HashSet[Var[_]]

  def isUsed(sym: Sym[_]): Boolean = usedSymbols.contains(sym)
  def isUsed(v: Var[_]): Boolean = usedVars.contains(v)

  override def traverse(block: Block[_]): Unit = {
    var usedVarsSize = 0
    var usedSymsSize = 0
    do {
      usedVarsSize = usedVars.size
      usedSymsSize = usedSymbols.size
      traverseBlock(block)
    } while (usedVarsSize != usedVars.size || usedSymsSize != usedSymbols.size)
    postprocess(block)
  }

  override def postprocess(block: Block[_]): Unit = {
    super.postprocess(block)
    logger.debug(s"allVars: ${allVars.size}, usedVars: ${usedVars.size}")
    logger.debug(s"removedVars: ${(allVars diff usedVars).map(_.e)}")
  }

  override protected[dblab] def traverseStm(stm: Stm[_]): Unit = stm match {
    case Stm(sym, NewVar(init)) =>
      val v = Var(sym.asInstanceOf[Rep[Var[Any]]])
      allVars += v
      if (isUsed(v))
        traverseExp(init)
    case Stm(sym, Assign(v, value)) =>
      if (isUsed(v))
        // assignedVars += v
        traverseExp(value)
    case Stm(sym, ReadVar(v)) =>
      if (isUsed(sym))
        usedVars += v
    case _ => super.traverseStm(stm)
  }

  override protected[dblab] def traverseExp(exp: Rep[_]): Unit = exp match {
    case sym: Sym[Any] => usedSymbols += sym
    case _             => super.traverseExp(exp)
  }

}

trait BackwardAnalysis[Lang <: Base] {
  val IR: Lang
  import IR._

  def traverse(block: Block[_]): Unit = {
    traverseBlock(block)
    postprocess(block)
  }

  def postprocess(block: Block[_]): Unit = {

  }

  protected[dblab] def traverseBlock(block: Block[_]): Unit = block match {
    case Block(stmts, res) => {
      traverseExp(res)
      stmts.reverse foreach (s =>
        traverseStm(s))
    }
  }

  protected[dblab] def traverseStm(stm: Stm[_]): Unit = stm match {
    case Stm(sym, rhs) => {
      traverseDef(rhs)
    }
  }

  protected[dblab] def traverseDef(node: Def[_]): Unit = node match {
    case b @ PardisBlock(stmt, res) => traverseBlock(b)
    case _ => for (a <- node.funArgs) {
      traverseFunArg(a)
    }
  }

  protected[dblab] def traverseFunArg(funArg: PardisFunArg): Unit = funArg match {
    case d: Def[_]       => traverseDef(d)
    case e: Rep[_]       => traverseExp(e)
    case PardisVarArg(v) => traverseFunArg(v)
  }

  protected[dblab] def traverseExp(exp: Rep[_]): Unit = exp match {
    case sy: Sym[_]         => ()
    case cons @ Constant(c) => ()
    case _                  => ()
  }
}