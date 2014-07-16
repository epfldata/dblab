package ch.epfl.data
package legobase
package deep

import scala.language.implicitConversions
import pardis.ir._
import reflect.runtime.universe.{ TypeTag, Type }

// trait ZCE extends TopDownTransformer[Base, Base] with Traverser[Base] {
trait DCE extends TopDownTransformerTraverser[Base] {
  import IR._

  def optimize[T: Manifest](node: Block[T]): Block[T] = {
    Predef.print(s"optimizing: $node")
    traverseBlock(node)
    traverseWorkList()
    transformProgram(node)
  }

  val marks = collection.mutable.Set[Sym[Any]]()
  val workList = collection.mutable.Stack[Def[Any]]()

  def addToWorkList[T](d: Def[T]) {
    workList.push(d.asInstanceOf[Def[Any]])
  }

  def mark[T](s: Sym[T]) {
    marks += s.asInstanceOf[Sym[Any]]
  }

  def isMarked[T](s: Sym[T]): Boolean = marks.contains(s.asInstanceOf[Sym[Any]])

  def checkAndAdd[T](exp: Rep[T]) {
    exp match {
      case sym @ Sym(_) => {
        // TODO HACK
        val d = sym.correspondingNode
        if (!isMarked(sym)) {
          addToWorkList(d)
          mark(sym)
        }
      }
      case _ => ()
    }
  }

  override def traverseStm(stm: Stm[_]): Unit = stm match {
    case Stm(sym, rhs) => {
      if (!rhs.isPure) {
        addToWorkList(rhs)
        mark(sym)
      }
      super.traverseStm(stm)
    }
  }

  def processArg(p: PardisFunArg): Unit = p match {
    case s @ Sym(_) => checkAndAdd(s)
    case _          => ()
  }

  def traverseWorkList() {
    while (!workList.isEmpty) {
      val d = workList.pop()
      // d match {
      //   case fd: FunctionDef[_] => {
      //     fd.caller.foreach { x =>
      //       checkAndAdd(x)
      //     }
      //     fd.argss.foreach { l =>
      //       l.foreach { x =>
      //         processArg(x)
      //       }
      //     }
      //   }
      //   case _ => ()
      // }
      for (a <- d.funArgs) {
        processArg(a)
      }
    }
  }

  override def traverseBlock(block: Block[_]): Unit = block match {
    case b @ Block(stmts, res) => {
      checkAndAdd(res)
      super.traverseBlock(block)
    }
  }

  override def transformStmToMultiple(stm: Stm[_]): List[to.Stm[_]] = stm match {
    case Stm(s, d) if marks.contains(s) => super.transformStmToMultiple(stm)
    case _                              => Nil
  }
}
