package ch.epfl.data
package dblab.legobase
package tpch

import utils.Utilities._
import sc.pardis.annotations.{ deep, metadeep, dontLift, dontInline, needs }
import queryengine._
import tpch._
import schema._
import sc.pardis.shallow.OptimalString
import sc.pardis.types._
import scala.reflect.runtime.universe._
import scala.reflect.runtime.currentMirror

// TODO it should be generalized to not be only TPCH-specific

// @metadeep(
//   folder = "",
//   header = """import ch.epfl.data.dblab.legobase.deep._
// import ch.epfl.data.dblab.legobase.deep.queryengine._
// import ch.epfl.data.dblab.legobase.deep.tpch._""",
//   component = "",
//   thisComponent = "ch.epfl.data.dblab.legobase.deep.DeepDSL")
// class MetaInfo

// @needs[(K2DBScanner, Array[_], REGIONRecord, PARTSUPPRecord, PARTRecord, NATIONRecord, SUPPLIERRecord, LINEITEMRecord, ORDERSRecord, CUSTOMERRecord, OptimalString)]
// @deep
// trait TPCHLoaderLoader

/**
 * A module that defines loaders for relations.
 *
 * (TODO for now it's specific to TPCH, but should be generalized)
 */
object TPCHLoader {

  val tpchSchema: Schema = TPCHSchema.getSchema(Config.datapath, getScalingFactor)
  def getTable(name: String): Table = tpchSchema.tables.find(t => t.name == name).get
  def getScalingFactor: Double = Config.datapath.slice(Config.datapath.lastIndexOfSlice("sf") + 2, Config.datapath.length - 1).toDouble //TODO Pass SF to Config

  import storagemanager.Loader.loadTable

  def loadRegion() = loadTable[REGIONRecord](getTable("REGION"))

  def loadPartsupp() = loadTable[PARTSUPPRecord](getTable("PARTSUPP"))

  def loadPart() = loadTable[PARTRecord](getTable("PART"))

  def loadNation() = loadTable[NATIONRecord](getTable("NATION"))

  def loadSupplier() = loadTable[SUPPLIERRecord](getTable("SUPPLIER"))

  def loadLineitem() = loadTable[LINEITEMRecord](getTable("LINEITEM"))

  def loadOrders() = loadTable[ORDERSRecord](getTable("ORDERS"))

  def loadCustomer() = loadTable[CUSTOMERRecord](getTable("CUSTOMER"))
}