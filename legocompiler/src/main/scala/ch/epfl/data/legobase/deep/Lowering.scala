package ch.epfl.data
package legobase
package deep

import scala.language.implicitConversions
import pardis.ir._

trait Lowering {
  val transformer: TopDownTransformer[DeepDSL, DeepDSL]
  val from: DeepDSL
  val to: DeepDSL
  import from._

  def transform(program: Block[Any]): Block[Any] = {
    transformer.transformBlock(program)
  }
}

trait TopDownTransformer[FromLang <: Base, ToLang <: Base] {
  val from: FromLang
  val to: ToLang
  import from._
  def transformBlock[T: Manifest, S: Manifest](block: Block[T]): to.Block[S] = block match {
    case Block(stmts, res) => to.Block[S](
      stmts map (s => transformStm[Any, Any](s.asInstanceOf[Stm[Any]])(s.manifest.asInstanceOf[Manifest[Any]], s.manifest.asInstanceOf[Manifest[Any]]).asInstanceOf[Stm[Any]]),
      transformExp[T, S](res))
  }

  val subst = collection.mutable.Map.empty[Rep[Any], to.Rep[Any]]

  def transformStm[T: Manifest, S: Manifest](stm: Stm[T]): to.Stm[S] = stm match {
    case Stm(sym, rhs) => {
      val newSym = to.fresh[S]
      subst += sym -> newSym
      to.Stm[S](newSym, transformDef[T, S](rhs))
    }
  }

  def transformDef[T: Manifest, S: Manifest](node: Def[T]): to.Def[S] = node match {
    case b @ PardisBlock(stmt, res) => transformBlock[T, S](b)
    case ifte @ PardisIfThenElse(cond, thenp, elsep) => to.IfThenElse(transformExp[Boolean, Boolean](cond), transformBlock[T, S](thenp), transformBlock[T, S](elsep))
    case w @ PardisWhile(cond, block) => to.While(transformBlock[Boolean, Boolean](cond), transformBlock[Unit, Unit](block)).asInstanceOf[to.Def[S]]
    case _ => ???
  }

  def transformExp[T: Manifest, S: Manifest](exp: Rep[T]): to.Rep[S] = exp match {
    case sy @ Sym(s)        => subst(sy).asInstanceOf[to.Rep[S]]
    case cons @ Constant(c) => Constant[S](c.asInstanceOf[S]) // TODO think more about it
  }
}