package ch.epfl.data
package dblab.legobase

import sc.pardis.language._

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
  var codeGenLang: Language = CCoreLanguage
}
