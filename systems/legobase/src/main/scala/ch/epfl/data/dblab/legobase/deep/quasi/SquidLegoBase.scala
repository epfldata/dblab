package ch.epfl.data
package dblab
package legobase
package deep
package quasi

import sc.pardis._
import squid.scback._

class SquidLegoBase[SC <: LegoBaseQueryEngineExp](val SC: SC) {

  object Sqd extends AutoboundPardisIR[SC.type](SC) with PardisBinding.DefaultRedirections[SC.type]
  val base: Sqd.type = Sqd

  Sqd.ab = AutoBinder(SC, Sqd) // this is going to generate a big binding structure; it's in a separate class/file so it's not always recomputed and recompiled!

}

object SquidLegoBase extends SquidLegoBase(new LegoBaseQueryEngineExp {})
