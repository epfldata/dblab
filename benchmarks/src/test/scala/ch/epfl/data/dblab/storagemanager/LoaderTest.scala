package ch.epfl.data
package dblab
package storagemanager

import benchmarks.tpch.{ TPCHSchema, PARTSUPPRecord }
import org.scalatest._
import sc.pardis.shallow.OptimalString
import Matchers._

case class PARTRecord(
  val P_PARTKEY: Int,
  val P_NAME: OptimalString,
  val P_MFGR: OptimalString,
  val P_BRAND: OptimalString,
  val P_TYPE: OptimalString,
  val P_SIZE: Int,
  val P_CONTAINER: OptimalString,
  val P_RETAILPRICE: Double,
  val P_COMMENT: OptimalString)

class LoaderTest extends FlatSpec {
  TPCHData.runOnData(datapath => {
    "Loader" should "load PART table with SF0.1 correctly" in {
      val partTable = TPCHSchema.getSchema(datapath, 0.1).tables.find(t => t.name == "PART").get
      val records = Loader.loadTable[PARTRecord](partTable)

      records.size should be(20000)
      records(0).P_PARTKEY should be(1)
      records(19999).P_RETAILPRICE should be(920.00)
    }

    "Loader" should "load PARTSUPP with SF0.1 sorted on PS_PARTKEY and PS_SUPPKEY" in {
      val partsuppTable = TPCHSchema.getSchema(datapath, 0.1).tables.find(t => t.name == "PARTSUPP").get
      val records = Loader.loadTable[PARTSUPPRecord](partsuppTable)

      records.size should be(80000)
    }
  })
}
