package ch.epfl.data
package dblab.legobase
package prettyprinter

import sc.pardis.utils.document._
import sc.pardis.ir._
import sc.pardis.prettyprinter._
import scala.language.implicitConversions
import sc.pardis.deep.scalalib._
import compiler.Settings
import deep.{ PAPIStart, PAPIEnd }

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
class LegoCGenerator(val outputFileName: String, val settings: Settings, override val verbose: Boolean = true) extends CCodeGenerator with ScalaCoreCCodeGen /* with BooleanCCodeGen */ {
  /**
   * Generates the code for the IR of the given program
   *
   * @param program the input program for which the code is generated
   */
  def apply(program: PardisProgram) {
    generate(program, outputFileName)
  }

  val branch_mis_pred = true

  override def header: Document = super.header :/: doc"""#include "pardis_clib.h" """ ::
    {
      if (settings.profile)
        Document.break :: doc"""#include <papi.h>""" :/: {
          if (branch_mis_pred)
            doc"""#define NUM_EVENTS 7
int event[NUM_EVENTS] = {PAPI_TOT_INS, PAPI_TOT_CYC, PAPI_BR_MSP, 
  PAPI_L1_DCM, PAPI_L2_DCA, PAPI_BR_INS,
  PAPI_REF_CYC
   };
"""
          else
            doc"""#define NUM_EVENTS 5
int event[NUM_EVENTS] = {PAPI_L1_DCM, PAPI_L2_DCM, PAPI_L2_DCA, 
  PAPI_STL_ICY, PAPI_REF_CYC};
"""
        } :/: doc"long long values[NUM_EVENTS];"
      else
        Document.empty
    }

  import sc.cscala.deep.GArrayHeaderIRs.GArrayHeaderG_array_indexObject

  val BN = "\\n"

  /**
   * Generates the code for the given function definition node
   *
   * @param fun the input function definition node
   * @returns the corresponding generated code
   */
  override def functionNodeToDocument(fun: FunctionNode[_]) = fun match {
    case GArrayHeaderG_array_indexObject(array, i) =>
      doc"g_array_index($array, ${fun.tp}, $i)"
    case PAPIStart() =>
      doc"""
/* Start counting events */
if (PAPI_start_counters(event, NUM_EVENTS) != PAPI_OK) {
    fprintf(stderr, "PAPI_start_counters - FAILED$BN");
    exit(1);
}"""
    case PAPIEnd() =>
      doc"""
/* Read the counters */
if (PAPI_read_counters(values, NUM_EVENTS) != PAPI_OK) {
    fprintf(stderr, "PAPI_read_counters - FAILED$BN");
    exit(1);
}""" :/: {
        if (branch_mis_pred)
          doc"""printf("Total instructions: %lld$BN", values[0]);
printf("Total cycles: %lld$BN", values[1]);
printf("Instr per cycle: %2.3f$BN", (double)values[0] / (double) values[1]);
printf("Branches mispredicted: %lld$BN", values[2]);
printf("L1 data cache misses: %lld$BN", values[3]);
printf("L2 data cache access: %lld$BN", values[4]);
printf("Branch instructions: %lld$BN", values[5]);
printf("Branch missprediction rate: %.6f$BN", (double)values[2] / (double)values[5]);
printf("Total ref cycles: %lld$BN", values[6]);"""
        else
          doc"""printf("Total ref cycles: %lld$BN", values[4]);
printf("L1 data cache misses: %lld$BN", values[0]);
printf("L2 data cache misses: %lld$BN", values[1]);
printf("Stalled cycles: %lld$BN", values[3]);
printf("L2 data cache accesses: %lld$BN", values[2]);
printf("L2 data cache miss rate: %.6f$BN", (double)values[1]/(double)values[2]);
printf("Stalled/Ref cycles: %.6f$BN", (double)values[3]/(double)values[4]);"""
      } :/: doc"""/* Stop counting events */
if (PAPI_stop_counters(values, NUM_EVENTS) != PAPI_OK) {
    fprintf(stderr, "PAPI_stoped_counters - FAILED$BN");
    exit(1);
}"""
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
                        override val settings: Settings,
                        override val verbose: Boolean = true) extends LegoCGenerator(outputFileName, settings, verbose) with CASTCodeGenerator[Base]
