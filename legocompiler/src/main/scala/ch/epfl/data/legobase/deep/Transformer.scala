package ch.epfl.data
package legobase
package deep

import scala.language.implicitConversions
import pardis.ir._

trait TopDownTransformer[FromLang <: Base, ToLang <: Base] {
  val from: FromLang
  val to: ToLang
  import from._

  implicit val context: FromLang = from

  def transformBlockTyped[T: Manifest, S: Manifest](block: Block[T]): to.Block[S] = block match {
    case Block(stmts, res) => {
      // println(s"processing block with ${stmts.size} and result $res")
      to.reifyBlock[S] {
        stmts.foreach(transformStmToMultiple)
        transformExp[T, S](res)
      }
    }
  }

  def transformProgram[T: Manifest](block: Block[T]): to.Block[T] = transformBlock[T](block)
  def transformBlock[T: Manifest](block: Block[T]): to.Block[T] = transformBlockTyped[T, T](block)

  val subst = collection.mutable.Map.empty[Rep[Any], to.Rep[Any]]

  def transformStmToMultiple(stm: Stm[_]): List[to.Stm[_]] = List(transformStm(stm))

  def transformStm(stm: Stm[_]): to.Stm[_] = stm match {
    case Stm(sym, rhs) => {
      val newSym = to.fresh(sym.tp).copyFrom(sym)
      // val newSym = sym
      subst += sym -> newSym
      val newdef = transformDef(rhs)

      val stmt = to.Stm(newSym, newdef)
      to.reflectStm(stmt)
      stmt
    }
  }

  def newSym[T: Manifest](sym: Rep[T]): to.Sym[_] = to.fresh(sym.tp).copyFrom(sym.asInstanceOf[Sym[T]])

  def transformDef[T: Manifest](node: Def[T]): to.Def[T] = node match {
    case b @ PardisBlock(stmt, res)                  => transformBlockTyped[T, T](b)
    case ifte @ PardisIfThenElse(cond, thenp, elsep) => to.IfThenElse(transformExp[Boolean, Boolean](cond), transformBlockTyped[T, T](thenp), transformBlockTyped[T, T](elsep))
    case w @ PardisWhile(cond, block)                => to.While(transformBlockTyped[Boolean, Boolean](cond), transformBlockTyped[Unit, Unit](block)).asInstanceOf[to.Def[T]]
    case PardisLambda0(f, o)                         => to.Lambda0(f, transformBlockTyped(o).asInstanceOf[Block[Any]])
    case PardisLambda(f, i, o) => {
      subst += i -> newSym(i)
      val newO = transformBlockTyped(o).asInstanceOf[Block[Any]]
      to.Lambda(f, i, newO)
    }
    case PardisLambda2(f, i1, i2, o) => {
      subst += i1 -> newSym(i1)
      subst += i2 -> newSym(i2)
      to.Lambda2(f, i1, i2, transformBlockTyped(o).asInstanceOf[Block[Any]])
    }
    case _ => node.rebuild(node.funArgs.map(transformFunArg): _*).asInstanceOf[to.Def[T]]
  }

  def transformFunArg(funArg: PardisFunArg): PardisFunArg = funArg match {
    case d: Def[_]       => transformDef(d)(d.tp).asInstanceOf[PardisFunArg]
    case e: Rep[_]       => transformExp(e)(e.tp, e.tp)
    case PardisVarArg(v) => transformFunArg(v)
  }

  def transformExp[T: Manifest, S: Manifest](exp: Rep[T]): to.Rep[S] = exp match {
    case sy @ Sym(s)        => subst(sy).asInstanceOf[to.Rep[S]]
    case cons @ Constant(c) => Constant[S](c.asInstanceOf[S]) // TODO think more about it
  }
}

trait TopDownTransformerTraverser[Lang <: Base] extends Traverser[Lang] with TopDownTransformer[Lang, Lang] {
  val from = IR
  val to = IR
}
