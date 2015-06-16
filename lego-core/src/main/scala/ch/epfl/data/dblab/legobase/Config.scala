package ch.epfl.data
package dblab.legobase

/**
 * A module containing configurations for running queries.
 */
object Config {
  /** Specifies if the correctness of the results should be checked or not. */
  var checkResults = true
  /** Specifies the location of data files. */
  var datapath: java.lang.String = null
  /** Number of the repetitions of running a query */
  val numRuns: scala.Int = 1
  /* Code generation info */
  var codeGenLang: CodeGenerationLang = CCodeGeneration
}

// TODO move to sc
sealed trait CodeGenerationLang
case object CCodeGeneration extends CodeGenerationLang
case object ScalaCodeGeneration extends CodeGenerationLang
