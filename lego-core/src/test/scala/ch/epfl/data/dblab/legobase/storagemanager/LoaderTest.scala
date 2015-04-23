import ch.epfl.data.dblab.legobase.storagemanager._
import ch.epfl.data.dblab.legobase.LBString
import ch.epfl.data.dblab.legobase.tpch.TPCHSchema
import org.scalatest._

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
  val DATAPATH = System.getenv("LEGO_DATA_FOLDER")
  if (DATAPATH != null) {
    "Loader" should "load PART table with SF0.1 correctly" in {
      // Change path to test
      val partTable = TPCHSchema.getSchema(s"$DATAPATH/sf0.1", 0.1).tables.find(t => t.name == "PART").get
      val records = Loader.loadTable[PARTRecord](partTable)

      assert(records.size == 20000)
      assert(records(0).P_PARTKEY == 1)
      assert(records(19999).P_RETAILPRICE == 920.00)
    }
  } else {
    println("Tests could not run because the environment variable `LEGO_DATA_FOLDER` does not exist.")
  }

}
