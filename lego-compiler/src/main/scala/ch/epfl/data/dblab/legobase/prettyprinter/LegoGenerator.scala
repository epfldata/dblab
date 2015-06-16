package ch.epfl.data
package dblab.legobase
package prettyprinter

import sc.pardis.utils.document._
import sc.pardis.ir._
import sc.pardis.prettyprinter._
import scala.language.implicitConversions
import sc.pardis.deep.scalalib._

/**
 * The class responsible for Scala code generation in ANF.
 *
 * This class unparses the IR nodes into their corresponding Scala syntax. However, the generated
 * code still remains in the administrative-normal form (ANF).
 *
 * @param shallow specifies whether the generated code should use the shallow interface classes of
 * LegoBase or not.
 * @param outputFileName the name of output file
 */
class LegoScalaGenerator(val shallow: Boolean = false,
                         val outputFileName: String,
                         val runnerClassName: String) extends ScalaCodeGenerator {

  /**
   * Returns the generated code of the necessary import for the shallow libraries
   * to be put in the header
   */
  def getShallowHeader: String = if (shallow) """
import queryengine._
import queryengine.push._
import sc.pardis.shallow._
  """
  else
    ""
  /**
   * Returns the generated code that is put in the header
   */
  override def getHeader: Document = doc"""package ch.epfl.data
package dblab.legobase

$getShallowHeader
import scala.collection.mutable.Set
import scala.collection.mutable.HashMap
import scala.collection.mutable.TreeSet
import scala.collection.mutable.ArrayBuffer
import storagemanager.K2DBScanner
import storagemanager.Loader
import queryengine.GenericEngine
import sc.pardis.shallow.OptimalString
import sc.pardis.shallow.scalalib.collection.Cont

class MultiMap[T, S] extends HashMap[T, Set[S]] with scala.collection.mutable.MultiMap[T, S]

object OrderingFactory {
  def apply[T](fun: (T, T) => Int): Ordering[T] = new Ordering[T] {
    def compare(o1: T, o2: T) = fun(o1, o2)
  }
}
"""

  /**
   * Returns the class/module signature code that the generated query is put inside that.
   */
  override def getTraitSignature(): Document = doc"""object $outputFileName extends $runnerClassName {
  def executeQuery(query: String, schema: ch.epfl.data.dblab.legobase.schema.Schema): Unit = main()
  def main(args: Array[String]) {
    run(args)
  }
  def main() = 
  """

  /**
   * Generates the code for the IR of the given program
   *
   * @param program the input program for which the code is generated
   */
  def apply(program: PardisProgram) {
    generate(program, outputFileName)
  }
}

/**
 * The class responsible for Scala code generation in a way more readable than ANF.
 *
 * This class unparses the IR nodes into their corresponding Scala syntax. The generated code
 * is no longer in ANF, but in a more similar format to a code written by human.
 *
 * @param IR the polymorphic embedding trait which contains the reified program.
 * @param shallow specifies whether the generated code should use the shallow interface classes of
 * LegoBase or not.
 * @param outputFileName the name of output file
 */
class LegoScalaASTGenerator(val IR: Base, override val shallow: Boolean = false,
                            override val outputFileName: String,
                            override val runnerClassName: String) extends LegoScalaGenerator(shallow, outputFileName, runnerClassName) with ASTCodeGenerator[Base]

/**
 * The class responsible for C code generation in ANF.
 *
 * This class unparses the IR nodes into their corresponding C syntax. However, the generated
 * code still remains in the administrative-normal form (ANF).
 *
 * @param outputFileName the name of output file
 * @param verbose outputs comments inside the code to provide more information about specific variable
 * definitions. For example, in the case of defining mutable variables an appropriate comment in front
 * of that variable definition.
 */
class LegoCGenerator(val outputFileName: String, override val verbose: Boolean = true) extends CCodeGenerator /* with BooleanCCodeGen */ {
  /**
   * Generates the code for the IR of the given program
   *
   * @param program the input program for which the code is generated
   */
  def apply(program: PardisProgram) {
    generate(program, outputFileName)
  }

  import sc.cscala.deep.GArrayHeaderIRs._

  /**
   * Generates the code for the given function definition node
   *
   * @param fun the input function definition node
   * @returns the corresponding generated code
   */
  override def functionNodeToDocument(fun: FunctionNode[_]) = fun match {
    case GArrayHeaderG_array_indexObject(array, i) =>
      "g_array_index(" :: expToDocument(array) :: ", " :: CUtils.pardisTypeToString(fun.tp) :: ", " :: expToDocument(i) :: ")"
    case _ => super.functionNodeToDocument(fun)
  }
}

/**
 * The class responsible for C code generation in a way more readable than ANF.
 *
 * This class unparses the IR nodes into their corresponding C syntax. The generated code
 * is no longer in ANF, but in a more similar format to a code written by human.
 *
 * @param IR the polymorphic embedding trait which contains the reified program.
 * @param outputFileName the name of output file
 * @param verbose outputs comments inside the code to provide more information about specific variable
 * definitions. For example, in the case of defining mutable variables an appropriate comment in front
 * of that variable definition.
 */
class LegoCASTGenerator(val IR: Base,
                        override val outputFileName: String,
                        override val verbose: Boolean = true) extends LegoCGenerator(outputFileName, verbose) with CASTCodeGenerator[Base]
