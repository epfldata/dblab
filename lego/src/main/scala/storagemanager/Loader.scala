package ch.epfl.data
package legobase
package storagemanager

import utils.Utilities._

object TPCHRelations {
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
    val L_COMMENT: Array[Byte]);
  def newLINEITEMRecord(ORDERKEY: Int, PARTKEY: Int, SUPPKEY: Int, LINENUMBER: Int,
                        QUANTITY: Double, EXTENDEDPRICE: Double, DISCOUNT: Double, TAX: Double,
                        RETURNFLAG: Character, LINESTATUS: Character, SHIPDATE: Long, COMMITDATE: Long,
                        RECEIPTDATE: Long, SHIPINSTRUCT: Array[Byte], SHIPMODE: Array[Byte],
                        COMMENT: Array[Byte]): LINEITEMRecord = {
    new LINEITEMRecord(ORDERKEY, PARTKEY, SUPPKEY, LINENUMBER, QUANTITY, EXTENDEDPRICE, DISCOUNT, TAX,
      RETURNFLAG, LINESTATUS, SHIPDATE, COMMITDATE, RECEIPTDATE, SHIPINSTRUCT, SHIPMODE,
      COMMENT);
  }
}

trait Loader {
  import TPCHRelations._
  def loadString(size: Int, s: K2DBScanner) = {
    val NAME = new Array[Byte](size)
    s.next(NAME)
    NAME.filter(y => y != 0)
  }
  def loadLineitem() = {
    val file = LINEITEMTABLE
    val size = {
      import scala.sys.process._;
      Integer.parseInt((("wc -l " + file) #| "awk {print($1)}" !!).replaceAll("\\s+$", ""))
    }
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
