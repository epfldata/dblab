package ch.epfl.data
package legobase
package deep

import scala.language.implicitConversions
import pardis.ir._

trait Traverser[Lang <: Base] {
  val IR: Lang
  import IR._

  def traverseBlock(block: Block[_]): Unit = block match {
    case Block(stmts, res) => {
      stmts foreach (s =>
        traverseStm(s))
      traverseExp(res)
    }
  }

  def traverseStm(stm: Stm[_]): Unit = stm match {
    case Stm(sym, rhs) => {
      traverseDef(rhs)
    }
  }

  def traverseDef(node: Def[_]): Unit = node match {
    case b @ PardisBlock(stmt, res) => traverseBlock(b)
    case _ => for (a <- node.funArgs) {
      traverseFunArg(a)
    }
  }

  def traverseFunArg(funArg: PardisFunArg): Unit = funArg match {
    case d: Def[_]       => traverseDef(d)
    case e: Rep[_]       => traverseExp(e)
    case PardisVarArg(v) => traverseFunArg(v)
  }

  def traverseExp(exp: Rep[_]): Unit = exp match {
    case sy @ Sym(s)        => ()
    case cons @ Constant(c) => ()
  }
}
