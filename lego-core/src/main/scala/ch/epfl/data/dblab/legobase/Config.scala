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
  /** Cache the loaded tables so that the same table is not loaded several times */
  val cacheLoading: Boolean = true
  /** Number of the repetitions of running a query */
  val numRuns: scala.Int = 5
  /* Code generation info */
  var codeGenLang: Language = CCoreLanguage
}
