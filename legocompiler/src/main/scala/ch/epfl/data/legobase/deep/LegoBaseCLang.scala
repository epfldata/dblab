package ch.epfl.data
package legobase
package deep

import pardis.ir._
import pardis.types._
import legobase.deep._

trait LegoBaseCLang extends DeepDSL with CFunctions { this: Base =>
  import CNodes._
  import CTypes._
  override def structName[T](m: PardisType[T]): String = {
    m match {
      case CArrayType(args)                    => "ArrayOf" + structName(args)
      case ArrayType(args)                     => structName(args)
      case PointerType(args)                   => structName(args)
      case IntType                             => "int"
      case ByteType | CharType | CharacterType => "char"
      case DoubleType                          => "double"
      case OptimalStringType                   => "OptimalString"
      case c if c.isRecord                     => m.name.replaceAll("struct ", "")
      case _                                   => super.structName(m)
    }
  }
}
