package ch.epfl.data
package dblab.legobase
package quasi

import deep.LoweringLegoBase
import sc.pardis.deep.scalalib._
import sc.pardis.deep.scalalib.collection._
import sc.pardis.quasi.anf.{ BaseYY, BaseExt }

trait LegoBaseQuasiExp extends LoweringLegoBase
  with BaseYY

trait LegoBaseQuasiExt extends BaseExt
  with NumericExtOps
  with ByteExtOps
  with SetExtOps
  with PairExtOps
  with ArrayBufferExtOps
  with ArrayExtOps
  with RangeExtOps
  with BooleanExtOps
  with MultiMapExtOps
  with OptionExtOps
