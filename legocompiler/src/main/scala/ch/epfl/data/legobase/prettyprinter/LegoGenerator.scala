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
import scala.collection.mutable.HashMap
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.TreeSet

object OrderingFactory {
  def apply[T](fun: (T, T) => Int): Ordering[T] = new Ordering[T] {
    def compare(o1: T, o2: T) = fun(o1, o2)
  }
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

  def apply(program: PardisProgram) {
    generate(program)
  }
}
