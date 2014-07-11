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
    case ifte @ PardisIfThenElse(cond, thenp, elsep) => {
      traverseExp(cond)
      traverseBlock(thenp)
      traverseBlock(elsep)
    }
    case w @ PardisWhile(cond, block) => {
      traverseBlock(cond)
      traverseBlock(block)
    }
    case _ => ()
  }

  def traverseExp(exp: Rep[_]): Unit = exp match {
    case sy @ Sym(s)        => ()
    case cons @ Constant(c) => ()
  }
}
