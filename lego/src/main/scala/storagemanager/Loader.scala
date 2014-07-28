package ch.epfl.data
package legobase
package storagemanager

import utils.Utilities._
import ch.epfl.data.pardis.shallow.{ CaseClassRecord }

object TPCHRelations {
  lazy val PARTTABLE = Config.datapath + "part.tbl"
  lazy val PARTSUPPTABLE = Config.datapath + "partsupp.tbl"
  lazy val NATIONTABLE = Config.datapath + "nation.tbl"
  lazy val REGIONTABLE = Config.datapath + "region.tbl"
  lazy val SUPPLIERTABLE = Config.datapath + "supplier.tbl"
  lazy val LINEITEMTABLE = Config.datapath + "lineitem.tbl"

  case class LINEITEMRecord(
    val L_ORDERKEY: Int,
    val L_PARTKEY: Int,
    val L_SUPPKEY: Int,
    val L_LINENUMBER: Int,
    val L_QUANTITY: Double,
    val L_EXTENDEDPRICE: Double,
    val L_DISCOUNT: Double,
    val L_TAX: Double,
    val L_RETURNFLAG: Character,
    val L_LINESTATUS: Character,
    val L_SHIPDATE: Long,
    val L_COMMITDATE: Long,
    val L_RECEIPTDATE: Long,
    val L_SHIPINSTRUCT: Array[Byte],
    val L_SHIPMODE: Array[Byte],
    val L_COMMENT: Array[Byte]) extends CaseClassRecord {
    def getField(key: String): Option[Any] = key match {
      case "L_ORDERKEY"      => Some(L_ORDERKEY)
      case "L_PARTKEY"       => Some(L_PARTKEY)
      case "L_SUPPKEY"       => Some(L_SUPPKEY)
      case "L_LINENUMBER"    => Some(L_LINENUMBER)
      case "L_QUANTITY"      => Some(L_QUANTITY)
      case "L_EXTENDEDPRICE" => Some(L_EXTENDEDPRICE)
      case "L_DISCOUNT"      => Some(L_DISCOUNT)
      case "L_TAX"           => Some(L_TAX)
      case "L_RETURNFLAG"    => Some(L_RETURNFLAG)
      case "L_LINESTATUS"    => Some(L_LINESTATUS)
      case "L_SHIPDATE"      => Some(L_SHIPDATE)
      case "L_COMMITDATE"    => Some(L_COMMITDATE)
      case "L_RECEIPTDATE"   => Some(L_RECEIPTDATE)
      case "L_SHIPINSTRUCT"  => Some(L_SHIPINSTRUCT)
      case "L_SHIPMODE"      => Some(L_SHIPMODE)
      case "L_COMMENT"       => Some(L_COMMENT)
      case _                 => None
    }
  }
  def newLINEITEMRecord(ORDERKEY: Int, PARTKEY: Int, SUPPKEY: Int, LINENUMBER: Int,
                        QUANTITY: Double, EXTENDEDPRICE: Double, DISCOUNT: Double, TAX: Double,
                        RETURNFLAG: Character, LINESTATUS: Character, SHIPDATE: Long, COMMITDATE: Long,
                        RECEIPTDATE: Long, SHIPINSTRUCT: Array[Byte], SHIPMODE: Array[Byte],
                        COMMENT: Array[Byte]): LINEITEMRecord = {
    new LINEITEMRecord(ORDERKEY, PARTKEY, SUPPKEY, LINENUMBER, QUANTITY, EXTENDEDPRICE, DISCOUNT, TAX,
      RETURNFLAG, LINESTATUS, SHIPDATE, COMMITDATE, RECEIPTDATE, SHIPINSTRUCT, SHIPMODE,
      COMMENT);
  }

  case class SUPPLIERRecord(
    val S_SUPPKEY: Int,
    val S_NAME: Array[Byte],
    val S_ADDRESS: Array[Byte],
    val S_NATIONKEY: Int,
    val S_PHONE: Array[Byte],
    val S_ACCTBAL: Double,
    val S_COMMENT: Array[Byte]) extends CaseClassRecord {
    def getField(key: String): Option[Any] = key match {
      case "S_SUPPKEY"   => Some(S_SUPPKEY)
      case "S_NAME"      => Some(S_NAME)
      case "S_ADDRESS"   => Some(S_ADDRESS)
      case "S_NATIONKEY" => Some(S_NATIONKEY)
      case "S_PHONE"     => Some(S_PHONE)
      case "S_ACCTBAL"   => Some(S_ACCTBAL)
      case "S_COMMENT"   => Some(S_COMMENT)
      case _             => None
    }
  }
  def newSUPPLIERRecord(SUPPKEY: Int, NAME: Array[Byte], ADDRESS: Array[Byte], NATIONKEY: Int, PHONE: Array[Byte], ACCTBAL: Double, COMMENT: Array[Byte]): SUPPLIERRecord = {
    new SUPPLIERRecord(SUPPKEY, NAME, ADDRESS, NATIONKEY, PHONE, ACCTBAL, COMMENT)
  }

  case class PARTSUPPRecord(
    val PS_PARTKEY: Int,
    val PS_SUPPKEY: Int,
    val PS_AVAILQTY: Int,
    val PS_SUPPLYCOST: Double,
    val PS_COMMENT: Array[Byte]) extends CaseClassRecord {
    def getField(key: String): Option[Any] = key match {
      case "PS_PARTKEY"    => Some(PS_PARTKEY)
      case "PS_SUPPKEY"    => Some(PS_SUPPKEY)
      case "PS_AVAILQTY"   => Some(PS_AVAILQTY)
      case "PS_SUPPLYCOST" => Some(PS_SUPPLYCOST)
      case "PS_COMMENT"    => Some(PS_COMMENT)
      case _               => None
    }
  }
  def newPARTSUPPRecord(PARTKEY: Int, SUPPKEY: Int, AVAILQTY: Int, SUPPLYCOST: Double, COMMENT: Array[Byte]): PARTSUPPRecord = {
    new PARTSUPPRecord(PARTKEY, SUPPKEY, AVAILQTY, SUPPLYCOST, COMMENT)
  }

  case class REGIONRecord(
    val R_REGIONKEY: Int,
    val R_NAME: Array[Byte],
    val R_COMMENT: Array[Byte]) extends CaseClassRecord {
    def getField(key: String): Option[Any] = key match {
      case "R_REGIONKEY" => Some(R_REGIONKEY)
      case "R_NAME"      => Some(R_NAME)
      case "R_COMMENT"   => Some(R_COMMENT)
      case _             => None
    }
  }
  def newREGIONRecord(REGIONKEY: Int, NAME: Array[Byte], COMMENT: Array[Byte]): REGIONRecord = {
    new REGIONRecord(REGIONKEY, NAME, COMMENT)
  }

  case class NATIONRecord(
    val N_NATIONKEY: Int,
    val N_NAME: Array[Byte],
    val N_REGIONKEY: Int,
    val N_COMMENT: Array[Byte]) extends CaseClassRecord {
    def getField(key: String): Option[Any] = key match {
      case "N_NATIONKEY" => Some(N_NATIONKEY)
      case "N_NAME"      => Some(N_NAME)
      case "N_REGIONKEY" => Some(N_REGIONKEY)
      case "N_COMMENT"   => Some(N_COMMENT)
      case _             => None
    }
  }
  def newNATIONRecord(NATIONKEY: Int, NAME: Array[Byte], REGIONKEY: Int, COMMENT: Array[Byte]): NATIONRecord = {
    new NATIONRecord(NATIONKEY, NAME, REGIONKEY, COMMENT)
  }

  case class PARTRecord(
    val P_PARTKEY: Int,
    val P_NAME: Array[Byte],
    val P_MFGR: Array[Byte],
    val P_BRAND: Array[Byte],
    val P_TYPE: Array[Byte],
    val P_SIZE: Int,
    val P_CONTAINER: Array[Byte],
    val P_RETAILPRICE: Double,
    val P_COMMENT: Array[Byte]) extends CaseClassRecord {
    def getField(key: String): Option[Any] = key match {
      case "P_PARTKEY"     => Some(P_PARTKEY)
      case "P_NAME"        => Some(P_NAME)
      case "P_MFGR"        => Some(P_MFGR)
      case "P_BRAND"       => Some(P_BRAND)
      case "P_TYPE"        => Some(P_TYPE)
      case "P_SIZE"        => Some(P_SIZE)
      case "P_CONTAINER"   => Some(P_CONTAINER)
      case "P_RETAILPRICE" => Some(P_RETAILPRICE)
      case "P_COMMENT"     => Some(P_COMMENT)
      case _               => None
    }
  }
  def newPARTRecord(PARTKEY: Int, NAME: Array[Byte], MFGR: Array[Byte], BRAND: Array[Byte], TYPE: Array[Byte], SIZE: Int, CONTAINER: Array[Byte], RETAILPRICE: Double, COMMENT: Array[Byte]): PARTRecord = {
    new PARTRecord(PARTKEY, NAME, MFGR, BRAND, TYPE, SIZE, CONTAINER, RETAILPRICE, COMMENT)
  }
}

trait Loader {
  import TPCHRelations._
  def loadString(size: Int, s: K2DBScanner) = {
    val NAME = new Array[Byte](size)
    s.next(NAME)
    NAME.filter(y => y != 0)
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
}
