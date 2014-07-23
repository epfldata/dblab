package ch.epfl.data
package legobase
package deep

import scala.language.implicitConversions
import pardis.ir._
import reflect.runtime.universe.{ TypeTag, Type }

trait DCE[Lang <: Base] extends TopDownTransformerTraverser[Lang] {
  import IR._

  def optimize[T: Manifest](node: Block[T]): Block[T] = {
    // Predef.print(s"optimizing: $node")
    traverseBlock(node)
    // println(s"marks after one iteration: ${marks.toList.sortBy(x => x.id).map(_.id)}")
    traverseWorkList()
    transformProgram(node)
  }

  val marks = collection.mutable.Set[Sym[Any]]()
  val workList = collection.mutable.Stack[Def[Any]]()

  def addToWorkList[T](d: Def[T]) {
    workList.push(d.asInstanceOf[Def[Any]])
  }

  def mark[T](s: Sym[T]) {
    // Predef.println(s.id + "\tadded")
    marks += s.asInstanceOf[Sym[Any]]
  }

  def isMarked[T](s: Sym[T]): Boolean = marks.contains(s.asInstanceOf[Sym[Any]])

  def checkAndAdd[T](exp: Rep[T]) {
    exp match {
      case Def(d) => {
        val sym = exp.asInstanceOf[Sym[T]]
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
    case s @ Sym(_)       => checkAndAdd(s)
    case PardisVarArg(va) => processArg(va)
    case _                => ()
  }

  def traverseWorkList() {
    while (!workList.isEmpty) {
      val d = workList.pop()
      for (a <- d.funArgs) {
        // Predef.println(a + "\tprocessed")
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

trait DCELegoBase extends DCE[DeepDSL] {
  import from._
  // TODO hack!
  override def transformDef[T: Manifest](node: Def[T]): to.Def[T] = node match {
    case RunQuery(b) => to.RunQuery(transformBlock(b))
    case _           => super.transformDef(node)
  }
}
