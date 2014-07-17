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

  def transformDef[T: Manifest](node: Def[T]): to.Def[T] = node match {
    case b @ PardisBlock(stmt, res) => transformBlockTyped[T, T](b)
    case ifte @ PardisIfThenElse(cond, thenp, elsep) => to.IfThenElse(transformExp[Boolean, Boolean](cond), transformBlockTyped[T, T](thenp), transformBlockTyped[T, T](elsep))
    case w @ PardisWhile(cond, block) => to.While(transformBlockTyped[Boolean, Boolean](cond), transformBlockTyped[Unit, Unit](block)).asInstanceOf[to.Def[T]]
    case _ => node.asInstanceOf[to.Def[T]]
    // case _                          => node.recreate(node.funArgs.map(transformFunArg): _*)
  }

  // implicit def expToDef[T](exp: to.Rep[T]): to.Def[T] = exp.correspondingNode

  // def transformFunArg(funArg: PardisFunArg): PardisFunArg = funArg match {
  //   case d: Def[_]       => transformDef(d)(d.tp).asInstanceOf[PardisFunArg]
  //   case e: Rep[_]       => transformExp(e)(e.tp, e.tp)
  //   case PardisVarArg(v) => transformFunArg(v)
  // }

  def transformExp[T: Manifest, S: Manifest](exp: Rep[T]): to.Rep[S] = exp match {
    case sy @ Sym(s)        => subst(sy).asInstanceOf[to.Rep[S]]
    case cons @ Constant(c) => Constant[S](c.asInstanceOf[S]) // TODO think more about it
  }
}

trait TopDownTransformerTraverser[Lang <: Base] extends Traverser[Lang] with TopDownTransformer[Lang, Lang] {
  val from = IR
  val to = IR
}
