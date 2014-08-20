package ch.epfl.data
package legobase
package prettyprinter

import ocas.utils.Document
import pardis.ir._
import pardis.prettyprinter._
import scala.language.implicitConversions

class LegoScalaGenerator(val query: Int, val shallow: Boolean = false) extends ScalaCodeGenerator {

  def getShallowHeader: String = if (shallow) """
import queryengine._
import queryengine.volcano._
import queryengine.TPCHRelations._
import pardis.shallow._
  """
  else
    ""

  override def getHeader: Document = s"""package ch.epfl.data
package legobase

$getShallowHeader
import scala.collection.mutable.Set
import scala.collection.mutable.HashMap
import scala.collection.mutable.TreeSet
import scala.collection.mutable.ArrayBuffer
import storagemanager.K2DBScanner
import storagemanager.Loader._
import queryengine.GenericEngine._
import pardis.shallow.OptimalString

object OrderingFactory {
  def apply[T](fun: (T, T) => Int): Ordering[T] = new Ordering[T] {
    def compare(o1: T, o2: T) = fun(o1, o2)
  }
}
"""

  override def getTraitSignature(): Document = s"""object LEGO_QUERY extends LegoRunner {
  def executeQuery(query: String): Unit = main()
  def main(args: Array[String]) {
    run(args)
  }
  def main() = 
  """
  //Temporary fix for def main(), check if generated code for Scala runs

  def apply(program: PardisProgram) {
    generate(program)
  }
}

class LegoCGenerator(val query: Int, val shallow: Boolean = false) extends CCodeGenerator {
  def apply(program: PardisProgram) {
    generate(program)
  }
}
