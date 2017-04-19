package ch.epfl.data
package dblab
package deep
package dsls

import sc.pardis.ir._
import sc.pardis.types._
import dblab.deep._

import sc.pardis.types.PardisTypeImplicits._

/**
 * A polymorphic embedding cake containing information related to C language
 */
trait CScalaExtra extends ScalaCoreDSL with CFunctions {
  import CNodes._
  import CTypes._
  override def structName[T](m: PardisType[T]): String = {
    m match {
      case CArrayType(args) => "ArrayOf" + structName(args)
      case ArrayType(args) => structName(args)
      case PointerType(args) => structName(args)
      case IntType => "int"
      case ByteType | CharType | CharacterType => "char"
      case DoubleType => "double"
      case OptimalStringType => "OptimalString"
      // TODO why we need it here?
      case c if c.isRecord && config.Config.specializeEngine => m.name.replaceAll("struct ", "")
      case _ => super.structName(m)
    }
  }
  def papi_start(): Rep[Unit] = PAPIStart()
  def papi_end(): Rep[Unit] = PAPIEnd()
}

case class PAPIStart() extends FunctionNode[Unit](None, "???", Nil) {
  override def rebuild(children: PardisFunArg*) = PAPIStart()
}
case class PAPIEnd() extends FunctionNode[Unit](None, "???", Nil) {
  override def rebuild(children: PardisFunArg*) = PAPIEnd()
}
