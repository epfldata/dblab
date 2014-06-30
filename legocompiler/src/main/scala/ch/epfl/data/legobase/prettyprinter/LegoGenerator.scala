package ch.epfl.data
package legobase
package prettyprinter

import ocas.utils.Document
import pardis.ir._
import pardis.prettyprinter._
import scala.language.implicitConversions

object LegoGenerator extends CodeGenerator {
  def generate(program: PardisBlock[_]) {
    val docBlock = blockToDocument(program)
    val doc = putInside(docBlock)
    val pw = new java.io.PrintWriter(System.out)
    doc.format(40, pw)
    pw.flush()
  }

  def putInside(doc: Document): Document = {
    """package ch.epfl.data
package legobase

import queryengine.volcano._
import storagemanager.TPCHRelations.LINEITEMRecord
import queryengine.AGGRecord

trait Q1Generated extends Q1 {""" :/: Document.nest(2, "def q1 = " :: doc) :/: "}" :/: ""
  }
}
