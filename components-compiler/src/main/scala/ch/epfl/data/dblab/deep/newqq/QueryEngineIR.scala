package ch.epfl.data
package dblab
package deep
package newqq

import sc.pardis._
import deep.scalalib._
import deep.scalalib.collection._
import squid.scback._

import scala.collection.mutable.ArrayBuffer

object QueryEngineIR {

  object SC extends dsls.QueryEngineExp with PardisBinding.DefaultPardisMixin {
    type `ignore java.lang.Exception.<init>` = Nothing
    type `ignore ch.epfl.data.dblab.queryengine.push.LeftOuterJoinOp.<init>` = Nothing
    type `ignore ch.epfl.data.dblab.queryengine.push.LeftOuterJoinOp.init` = Nothing
    type `ignore ch.epfl.data.dblab.queryengine.push.LeftOuterJoinOpRep` = Nothing
    type `ignore ch.epfl.data.dblab.queryengine.push.LeftOuterJoinOp` = Nothing
    type `ignore ch.epfl.data.dblab.queryengine.push.LeftOuterJoinOp.child` = Nothing

    //type `ignore ch.epfl.data.dblab.deep.queryengine.push.LeftOuterJoinOpRep` = Nothing
    type `ignore ch.epfl.data.dblab.deep.queryengine.push.LeftOuterJoinOpOps.LeftOuterJoinOpRep` = Nothing
    type `ignore ch.epfl.data.dblab.deep.queryengine.push.ViewOpOps.ViewOpRep` = Nothing
    type `ignore ch.epfl.data.dblab.deep.queryengine.push.HashJoinAntiOps.HashJoinAntiRep` = Nothing
  }

  //object Sqd extends AutoboundPardisIR(SC)
  object Sqd extends AutoboundPardisIR(SC) with PardisBinding.DefaultRedirections[SC.type]
  Sqd.ab = AutoBinder(SC, Sqd) // this is going to generate a big binding structure; put it in a separate file so it's not always recomputed and recompiled!
  //Sqd.ab = AutoBinder.dbg(SC, Sqd) // this is going to generate a big binding structure; put it in a separate file so it's not always recomputed and recompiled!
}
