package ch.epfl.data
package legobase
package prettyprinter

import ocas.utils.Document
import pardis.ir._
import pardis.prettyprinter._
import scala.language.implicitConversions

class LegoGenerator(val query: Int, val shallow: Boolean = false) extends ScalaCodeGenerator {

  def getShallowHeader: String = if (shallow) """
import queryengine._
import queryengine.volcano._
import storagemanager.TPCHRelations._
import pardis.shallow._
  """
  else
    ""

  override def getHeader: Document = s"""package ch.epfl.data
package legobase

$getShallowHeader
import queryengine.AGGRecord
import scala.collection.mutable.Set
import scala.collection.mutable.HashMap
import scala.collection.mutable.TreeSet
import scala.collection.mutable.ArrayBuffer
import storagemanager.K2DBScanner

object OrderingFactory {
  def apply[T](fun: (T, T) => Int): Ordering[T] = new Ordering[T] {
    def compare(o1: T, o2: T) = fun(o1, o2)
  }
}
"""

  override def getTraitSignature(): Document = s"""object LEGO_QUERY extends LegoRunner with GenericQuery {
  def main(args: Array[String]) {
    val query = (x: Int) => main()
    val list = ${if (query == 1) "List(query)" else 1.to(query).mkString("List(", ", ", ", ") + "query)"}
    run(args, list)
  }
  """

  def apply(program: PardisProgram) {
    generate(program)
  }
}

object LegoGeneratorShallow extends ScalaCodeGenerator {

  override def getHeader: Document = """package ch.epfl.data
package legobase

import scala.collection.mutable.Set
import scala.collection.mutable.HashMap
import scala.collection.mutable.TreeSet
import scala.collection.mutable.ArrayBuffer
import storagemanager.K2DBScanner
import storagemanager.TPCHRelations._
import pardis.shallow._

object OrderingFactory {
  def apply[T](fun: (T, T) => Int): Ordering[T] = new Ordering[T] {
    def compare(o1: T, o2: T) = fun(o1, o2)
  }
}
"""

  override def getTraitSignature(): Document = """object LEGO_QUERY extends LegoRunner with GenericQuery {
  def main(args: Array[String]) {
    val q1 = (x: Int) => main()
    run(args, List(q1))
  }
  """

  def apply(program: PardisProgram) {
    generate(program)
  }
}

// object LegoGenerator extends CCodeGenerator {
//   def apply(program: PardisProgram) {
//     generate(program)
//   }
// }
