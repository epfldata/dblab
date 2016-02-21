package ch.epfl.data
package dblab.legobase
package storagemanager

import tpch.TPCHSchema
import org.scalatest._
import Matchers._

case class PARTRecord(
  val P_PARTKEY: Int,
  val P_NAME: LBString,
  val P_MFGR: LBString,
  val P_BRAND: LBString,
  val P_TYPE: LBString,
  val P_SIZE: Int,
  val P_CONTAINER: LBString,
  val P_RETAILPRICE: Double,
  val P_COMMENT: LBString)

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
      import tpch.PARTSUPPRecord
      val partsuppTable = TPCHSchema.getSchema(datapath, 0.1).tables.find(t => t.name == "PARTSUPP").get
      val records = Loader.loadTable[PARTSUPPRecord](partsuppTable)

      records.size should be(80000)
    }
  })
}
