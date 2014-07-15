package ch.epfl.data
package legobase
package prettyprinter

import ocas.utils.Document
import pardis.ir._
import pardis.prettyprinter._
import scala.language.implicitConversions

object LegoGenerator extends ScalaCodeGenerator {

  override def getHeader: Document = """package ch.epfl.data
package legobase

import queryengine.AGGRecord
import scala.collection.mutable.Set
import scala.collection.mutable.HashMap
import scala.collection.mutable.TreeSet
import storagemanager.K2DBScanner

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
