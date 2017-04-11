package ch.epfl.data
package dblab
package config

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
  val numRuns: scala.Int = 1
  /** Specifies if the output should be printed or not */
  val printResult = false
  /** Code generation info */
  var codeGenLang: Language = CCoreLanguage
  /** Specifies whether to obtain statistics during schema definition */
  val gatherStats: Boolean = false
  /** Specifies whether to show information about the query plan generation during execution */
  val debugQueryPlan: Boolean = true
  /** Specifies whether to specialize the loader or not */
  var specializeLoader: Boolean = true
}
