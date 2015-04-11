package ch.epfl.data
package dblab.legobase
package storagemanager

import utils.Utilities._
import sc.pardis.annotations.{ deep, metadeep, dontLift, dontInline, needs }
import queryengine._
import tpch._
import sc.pardis.shallow.OptimalString

// TODO it should be generalized to not be only TPCH-specific

@metadeep(
  folder = "",
  header = """import ch.epfl.data.dblab.legobase.deep._
import ch.epfl.data.dblab.legobase.deep.queryengine._
import ch.epfl.data.dblab.legobase.deep.tpch._""",
  component = "",
  thisComponent = "ch.epfl.data.dblab.legobase.deep.DeepDSL")
class MetaInfo

@needs[(K2DBScanner, Array[_], REGIONRecord, PARTSUPPRecord, PARTRecord, NATIONRecord, SUPPLIERRecord, LINEITEMRecord, ORDERSRecord, CUSTOMERRecord, OptimalString)]
@deep
trait Loader

/**
 * A module that defines loaders for relations.
 *
 * (TODO for now it's specific to TPCH, but should be generalized)
 */
object Loader {
  @dontInline
  def getFullPath(fileName: String): String = Config.datapath + fileName
  def loadString(size: Int, s: K2DBScanner) = {
    val NAME = new Array[Byte](size + 1)
    s.next(NAME)
    new OptimalString(NAME.filter(y => y != 0))
  }

  @dontInline
  def fileLineCount(file: String) = {
    import scala.sys.process._;
    Integer.parseInt(((("wc -l " + file) #| "awk {print($1)}").!!).replaceAll("\\s+$", ""))
  }

  def loadRegion() = {
    val file = getFullPath("region.tbl")
    val size = fileLineCount(file)
    /* Load Relation */
    val s = new K2DBScanner(file)
    val hm = new Array[REGIONRecord](size)
    var i = 0
    while (s.hasNext()) {
      val newEntry = new REGIONRecord(s.next_int, loadString(25, s), loadString(152, s))
      hm(i) = newEntry
      i += 1
    }
    hm
  }

  def loadPartsupp() = {
    val file = getFullPath("partsupp.tbl")
    val size = fileLineCount(file)
    /* Load Relation */
    val s = new K2DBScanner(file)
    val hm = new Array[PARTSUPPRecord](size)
    var i = 0
    while (s.hasNext()) {
      val newEntry = new PARTSUPPRecord(s.next_int, s.next_int, s.next_int, s.next_double, loadString(199, s))
      hm(i) = newEntry
      i += 1
    }
    hm
  }

  def loadPart() = {
    val file = getFullPath("part.tbl")
    val size = fileLineCount(file)
    /* Load Relation */
    val s = new K2DBScanner(file)
    val hm = new Array[PARTRecord](size)
    var i = 0
    while (s.hasNext()) {
      val newEntry = new PARTRecord(s.next_int, loadString(55, s), loadString(25, s), loadString(10, s), loadString(25, s),
        s.next_int, loadString(10, s), s.next_double, loadString(23, s))
      hm(i) = newEntry
      i += 1
    }
    hm
  }

  def loadNation() = {
    val file = getFullPath("nation.tbl")
    val size = fileLineCount(file)
    /* Load Relation */
    val s = new K2DBScanner(file)
    val hm = new Array[NATIONRecord](size)
    var i = 0
    while (s.hasNext()) {
      val newEntry = new NATIONRecord(s.next_int, loadString(25, s), s.next_int, loadString(152, s))
      hm(i) = newEntry
      i += 1
    }
    hm
  }

  def loadSupplier() = {
    val file = getFullPath("supplier.tbl")
    val size = fileLineCount(file)
    /* Load Relation */
    val s = new K2DBScanner(file)
    val hm = new Array[SUPPLIERRecord](size)
    var i = 0
    while (s.hasNext()) {
      val newEntry = new SUPPLIERRecord(s.next_int, loadString(25, s), loadString(40, s), s.next_int, loadString(15, s), s.next_double, loadString(101, s))
      hm(i) = newEntry
      i += 1
    }
    hm
  }

  def loadLineitem() = {
    val file = getFullPath("lineitem.tbl")
    val size = fileLineCount(file)
    // Load Relation 
    val s = new K2DBScanner(file)
    val hm = new Array[LINEITEMRecord](size)
    var i = 0
    while (s.hasNext()) {
      val newEntry = new LINEITEMRecord(s.next_int, s.next_int, s.next_int, s.next_int,
        s.next_int, s.next_double, s.next_double, s.next_double,
        s.next_char, s.next_char, s.next_date, s.next_date, s.next_date,
        loadString(25, s), loadString(10, s), loadString(44, s))
      hm(i) = newEntry
      i += 1
    }
    hm
  }

  def loadOrders() = {
    val file = getFullPath("orders.tbl")
    val size = fileLineCount(file)
    // Load Relation 
    val s = new K2DBScanner(file)
    val hm = new Array[ORDERSRecord](size)
    var i = 0
    while (s.hasNext()) {
      val newEntry = new ORDERSRecord(s.next_int, s.next_int, s.next_char, s.next_double, s.next_date,
        loadString(15, s), loadString(15, s), s.next_int, loadString(79, s))
      hm(i) = newEntry
      i += 1
    }
    hm
  }

  def loadCustomer() = {
    val file = getFullPath("customer.tbl")
    val size = fileLineCount(file)
    // Load Relation 
    val s = new K2DBScanner(file)
    val hm = new Array[CUSTOMERRecord](size)
    var i = 0
    while (s.hasNext()) {
      val newEntry = new CUSTOMERRecord(s.next_int, loadString(25, s), loadString(40, s), s.next_int, loadString(15, s), s.next_double, loadString(10, s), loadString(117, s))
      hm(i) = newEntry
      i += 1
    }
    hm
  }
}