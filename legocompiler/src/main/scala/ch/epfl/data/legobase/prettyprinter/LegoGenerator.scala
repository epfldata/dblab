package ch.epfl.data
package legobase
package prettyprinter

import ocas.utils.Document
import pardis.ir._
import pardis.prettyprinter._
import scala.language.implicitConversions

object LegoGenerator extends ScalaCodeGenerator {

  override def getHeader: Document = """
import storagemanager.TPCHRelations.LINEITEMRecord
import queryengine.AGGRecord
import scala.collection.mutable.Set

object SetVarArg {
  def apply[T](v: Seq[T]): Set[T] = Set(v : _*)
}
"""

  def apply(program: PardisBlock[_]) {
    /*    val header = {
      """package ch.epfl.data
package legobase

import queryengine.volcano._
import storagemanager.TPCHRelations.LINEITEMRecord
import queryengine.AGGRecord
"""
    }
    val traitSignature = "trait Q1Generated extends Q1"*/
    /*:/: Document.nest(2, "def q1 = " :: doc) :/: "}" :/: ""*/
    generate(program /*, header, traitSignature*/ )
  }
}
