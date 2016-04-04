package ch.epfl.data
package dblab
package legobase
package deep

import dblab.deep.dsls.QueryEngineExp
import dblab.deep.benchmarks._
import dblab.deep.benchmarks.tpch._

/** A polymophic embedding cake which chains all cakes needed for the LegoBase query engine */
abstract class LegoBaseQueryEngineExp extends QueryEngineExp
  with SynthesizedQueriesComponent
  with QueriesImplementations
  with TPCHLoaderInlined
