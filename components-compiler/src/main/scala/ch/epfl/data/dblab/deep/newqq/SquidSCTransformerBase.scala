package ch.epfl.data
package dblab
package deep
package newqq

import sc.pardis._
import squid.scback._

class SquidSCTransformerBase[SC <: dsls.QueryEngineExp](val SC: SC) {

  object Sqd extends AutoboundPardisIR[SC.type](SC) with PardisBinding.DefaultRedirections[SC.type]
  val base: Sqd.type = Sqd

  Sqd.ab = AutoBinder(SC, Sqd) // this is going to generate a big binding structure; it's in a separate class/file so it's not always recomputed and recompiled!

}

object SquidSCTransformerBase extends SquidSCTransformerBase(new dsls.QueryEngineExp {})
