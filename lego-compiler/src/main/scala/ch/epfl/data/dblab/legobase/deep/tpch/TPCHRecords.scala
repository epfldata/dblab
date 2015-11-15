package ch.epfl.data
package dblab.legobase
package deep
package tpch

import sc.pardis.ir._
import sc.pardis.types.PardisTypeImplicits._
import sc.pardis.deep._
import sc.pardis.deep.scalalib._
import sc.pardis.deep.scalalib.collection._
import sc.pardis.deep.scalalib.io._
import dblab.legobase.deep.queryengine._
import dblab.legobase.deep.storagemanager._
import dblab.legobase.deep.tpch._

/** A polymorphic embedding cake which chains all components needed for TPCH queries */
trait TPCHRecords extends Q1GRPRecordComponent
  with Q3GRPRecordComponent
  with Q7GRPRecordComponent
  with Q9GRPRecordComponent
  with Q10GRPRecordComponent
  with Q13IntRecordComponent
  with Q16GRPRecord1Component
  with Q16GRPRecord2Component
  with Q18GRPRecordComponent
  with Q20GRPRecordComponent
  with LINEITEMRecordComponent
  with SUPPLIERRecordComponent
  with PARTSUPPRecordComponent
  with REGIONRecordComponent
  with NATIONRecordComponent
  with PARTRecordComponent
  with CUSTOMERRecordComponent
  with ORDERSRecordComponent
