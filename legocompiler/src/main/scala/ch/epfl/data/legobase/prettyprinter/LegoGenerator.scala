package ch.epfl.data
package legobase
package prettyprinter

import pardis.utils.Document
import pardis.ir._
import pardis.prettyprinter._
import scala.language.implicitConversions
import pardis.deep.scalalib._

class LegoScalaASTGenerator(val IR: Base, override val shallow: Boolean = false, override val outputFileName: String = "generatedProgram") extends LegoScalaGenerator(shallow, outputFileName) with ASTCodeGenerator[Base]

class LegoScalaGenerator(val shallow: Boolean = false, val outputFileName: String = "generatedProgram") extends ScalaCodeGenerator {

  def getShallowHeader: String = if (shallow) """
import queryengine._
import queryengine.push._
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
import storagemanager.Loader
import queryengine.GenericEngine
import pardis.shallow.OptimalString
import pardis.shallow.scalalib.collection.Cont

class MultiMap[T, S] extends HashMap[T, Set[S]] with scala.collection.mutable.MultiMap[T, S]

object OrderingFactory {
  def apply[T](fun: (T, T) => Int): Ordering[T] = new Ordering[T] {
    def compare(o1: T, o2: T) = fun(o1, o2)
  }
}
"""

  override def getTraitSignature(): Document = s"""object $outputFileName extends LegoRunner {
  def executeQuery(query: String): Unit = main()
  def main(args: Array[String]) {
    run(args)
  }
  def main() = 
  """
  //Temporary fix for def main(), check if generated code for Scala runs

  def apply(program: PardisProgram) {
    generate(program, outputFileName)
  }
}

class LegoCASTGenerator(val IR: Base, override val shallow: Boolean = false, override val outputFileName: String = "generatedProgram", override val verbose: Boolean = true) extends LegoCGenerator(shallow, outputFileName, verbose) with CASTCodeGenerator[Base]

class LegoCGenerator(val shallow: Boolean = false, val outputFileName: String = "generatedProgram", override val verbose: Boolean = true) extends CCodeGenerator /* with BooleanCCodeGen */ {
  def apply(program: PardisProgram) {
    generate(program, outputFileName)
  }
}
