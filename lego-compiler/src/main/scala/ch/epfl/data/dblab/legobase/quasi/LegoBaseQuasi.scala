package ch.epfl.data
package dblab.legobase
package quasi

import deep.LoweringLegoBase
import sc.pardis.deep.{ DSLExpOps, DSLExtOps }
import sc.pardis.deep.scalalib._
import sc.pardis.deep.scalalib.collection._

trait LegoBaseQuasiExp extends LoweringLegoBase with DSLExpOps {

}

trait LegoBaseQuasiExt extends DSLExtOps with ArrayExtOps with RangeExtOps with BooleanExtOps
