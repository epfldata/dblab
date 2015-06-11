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
  /** Specifies the scaling factor for TPCH queries (TODO should be removed) */
  var sf: Double = _
  /** Number of the repetitions of running a query */
  val numRuns: scala.Int = 1

  /* Code generation info */
  sealed trait CodeGenerationLang
  case object CCodeGeneration extends CodeGenerationLang
  case object ScalaCodeGeneration extends CodeGenerationLang
  var codeGenLang: CodeGenerationLang = CCodeGeneration
}
