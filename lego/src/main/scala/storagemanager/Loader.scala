package ch.epfl.data
package legobase
package storagemanager

import utils.Utilities._

// FIXME just to cheat on auto-lifter
trait Loader {

}

object Loader {
  import queryengine.TPCHRelations._
  def loadString(size: Int, s: K2DBScanner) = {
    val NAME = new Array[Byte](size)
    s.next(NAME)
    LBString(NAME.filter(y => y != 0))
  }

  def fileLineCount(file: String) = {
    import scala.sys.process._;
    Integer.parseInt(((("wc -l " + file) #| "awk {print($1)}").!!).replaceAll("\\s+$", ""))
  }

  def loadRegion() = {
    val file = REGIONTABLE
    val size = fileLineCount(file)
    /* Load Relation */
    val s = new K2DBScanner(file)
    val hm = new Array[REGIONRecord](size)
    var i = 0
    while (s.hasNext()) {
      val newEntry = newREGIONRecord(s.next_int, loadString(25, s), loadString(152, s))
      hm(i) = newEntry
      i += 1
    }
    hm
  }

  def loadPartsupp() = {
    val file = PARTSUPPTABLE
    val size = fileLineCount(file)
    /* Load Relation */
    val s = new K2DBScanner(file)
    val hm = new Array[PARTSUPPRecord](size)
    var i = 0
    while (s.hasNext()) {
      val newEntry = newPARTSUPPRecord(s.next_int, s.next_int, s.next_int, s.next_double, loadString(199, s))
      hm(i) = newEntry
      i += 1
    }
    hm
  }

  def loadPart() = {
    val file = PARTTABLE
    val size = fileLineCount(file)
    /* Load Relation */
    val s = new K2DBScanner(file)
    val hm = new Array[PARTRecord](size)
    var i = 0
    while (s.hasNext()) {
      val newEntry = newPARTRecord(s.next_int, loadString(55, s), loadString(25, s), loadString(10, s), loadString(25, s),
        s.next_int, loadString(10, s), s.next_double, loadString(23, s))
      hm(i) = newEntry
      i += 1
    }
    hm
  }

  def loadNation() = {
    val file = NATIONTABLE
    val size = fileLineCount(file)
    /* Load Relation */
    val s = new K2DBScanner(file)
    val hm = new Array[NATIONRecord](size)
    var i = 0
    while (s.hasNext()) {
      val newEntry = newNATIONRecord(s.next_int, loadString(25, s), s.next_int, loadString(152, s))
      hm(i) = newEntry
      i += 1
    }
    hm
  }

  def loadSupplier() = {
    val file = SUPPLIERTABLE
    val size = fileLineCount(file)
    /* Load Relation */
    val s = new K2DBScanner(file)
    val hm = new Array[SUPPLIERRecord](size)
    var i = 0
    while (s.hasNext()) {
      val newEntry = newSUPPLIERRecord(s.next_int, loadString(25, s), loadString(40, s), s.next_int, loadString(15, s), s.next_double, loadString(101, s))
      hm(i) = newEntry
      i += 1
    }
    hm
  }

  def loadLineitem() = {
    val file = LINEITEMTABLE
    val size = fileLineCount(file)
    // Load Relation 
    val s = new K2DBScanner(file)
    val hm = new Array[LINEITEMRecord](size)
    var i = 0
    while (s.hasNext()) {
      val newEntry = newLINEITEMRecord(s.next_int, s.next_int, s.next_int, s.next_int,
        s.next_double, s.next_double, s.next_double, s.next_double,
        s.next_char, s.next_char, s.next_date, s.next_date, s.next_date,
        loadString(25, s), loadString(10, s), loadString(44, s))
      hm(i) = newEntry
      i += 1
    }
    hm
  }

  def loadOrders() = {
    val file = ORDERSTABLE
    val size = fileLineCount(file)
    // Load Relation 
    val s = new K2DBScanner(file)
    val hm = new Array[ORDERSRecord](size)
    var i = 0
    while (s.hasNext()) {
      val newEntry = newORDERSRecord(s.next_int, s.next_int, s.next_char, s.next_double, s.next_date,
        loadString(15, s), loadString(15, s), s.next_int, loadString(79, s))
      hm(i) = newEntry
      i += 1
    }
    hm
  }

  def loadCustomer() = {
    val file = CUSTOMERTABLE
    val size = fileLineCount(file)
    // Load Relation 
    val s = new K2DBScanner(file)
    val hm = new Array[CUSTOMERRecord](size)
    var i = 0
    while (s.hasNext()) {
      val newEntry = newCUSTOMERRecord(s.next_int, loadString(25, s), loadString(40, s), s.next_int, loadString(15, s), s.next_double, loadString(10, s), loadString(117, s))
      hm(i) = newEntry
      i += 1
    }
    hm
  }
}
