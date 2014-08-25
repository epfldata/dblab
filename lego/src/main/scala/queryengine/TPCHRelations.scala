package ch.epfl.data
package legobase
package queryengine

import utils.Utilities._
import ch.epfl.data.pardis.shallow.{ CaseClassRecord }

object TPCHRelations {
  lazy val PARTTABLE = Config.datapath + "part.tbl"
  lazy val PARTSUPPTABLE = Config.datapath + "partsupp.tbl"
  lazy val NATIONTABLE = Config.datapath + "nation.tbl"
  lazy val REGIONTABLE = Config.datapath + "region.tbl"
  lazy val SUPPLIERTABLE = Config.datapath + "supplier.tbl"
  lazy val LINEITEMTABLE = Config.datapath + "lineitem.tbl"
  lazy val CUSTOMERTABLE = Config.datapath + "customer.tbl"
  lazy val ORDERSTABLE = Config.datapath + "orders.tbl"

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
    val L_SHIPINSTRUCT: LBString,
    val L_SHIPMODE: LBString,
    val L_COMMENT: LBString) extends CaseClassRecord {
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
                        RECEIPTDATE: Long, SHIPINSTRUCT: LBString, SHIPMODE: LBString,
                        COMMENT: LBString): LINEITEMRecord = {
    new LINEITEMRecord(ORDERKEY, PARTKEY, SUPPKEY, LINENUMBER, QUANTITY, EXTENDEDPRICE, DISCOUNT, TAX,
      RETURNFLAG, LINESTATUS, SHIPDATE, COMMITDATE, RECEIPTDATE, SHIPINSTRUCT, SHIPMODE,
      COMMENT);
  }

  case class ORDERSRecord(
    val O_ORDERKEY: Int,
    val O_CUSTKEY: Int,
    val O_ORDERSTATUS: Character,
    val O_TOTALPRICE: Double,
    val O_ORDERDATE: Long,
    val O_ORDERPRIORITY: LBString,
    val O_CLERK: LBString,
    val O_SHIPPRIORITY: Int,
    val O_COMMENT: LBString) extends CaseClassRecord {
    def getField(key: String): Option[Any] = key match {
      case "O_ORDERKEY"      => Some(O_ORDERKEY)
      case "O_CUSTKEY"       => Some(O_CUSTKEY)
      case "O_ORDERSTATUS"   => Some(O_ORDERSTATUS)
      case "O_TOTALPRICE"    => Some(O_TOTALPRICE)
      case "O_ORDERDATE"     => Some(O_ORDERDATE)
      case "O_CLERK"         => Some(O_CLERK)
      case "O_ORDERPRIORITY" => Some(O_ORDERPRIORITY)
      case "O_SHIPPRIORITY"  => Some(O_SHIPPRIORITY)
      case "O_COMMENT"       => Some(O_COMMENT)
      case _                 => None
    }
  }

  def newORDERSRecord(ORDERKEY: Int, CUSTKEY: Int, ORDERSTATUS: Character, TOTALPRICE: Double, ORDERDATE: Long, ORDERPRIORITY: LBString, CLERK: LBString, SHIPPRIORITY: Int, COMMENT: LBString): ORDERSRecord = {
    new ORDERSRecord(ORDERKEY, CUSTKEY, ORDERSTATUS, TOTALPRICE, ORDERDATE, ORDERPRIORITY, CLERK, SHIPPRIORITY, COMMENT)
  }

  case class CUSTOMERRecord(
    val C_CUSTKEY: Int,
    val C_NAME: LBString,
    val C_ADDRESS: LBString,
    val C_NATIONKEY: Int,
    val C_PHONE: LBString,
    val C_ACCTBAL: Double,
    val C_MKTSEGMENT: LBString,
    val C_COMMENT: LBString) extends CaseClassRecord {
    def getField(key: String): Option[Any] = key match {
      case "C_CUSTKEY"    => Some(C_CUSTKEY)
      case "C_NAME"       => Some(C_NAME)
      case "C_ADDRESS"    => Some(C_ADDRESS)
      case "C_NATIONKEY"  => Some(C_NATIONKEY)
      case "C_PHONE"      => Some(C_PHONE)
      case "C_ACCTBAL"    => Some(C_ACCTBAL)
      case "C_MKTSEGMENT" => Some(C_MKTSEGMENT)
      case "C_COMMENT"    => Some(C_COMMENT)
      case _              => None
    }
  }
  def newCUSTOMERRecord(CUSTKEY: Int, NAME: LBString, ADDRESS: LBString, NATIONKEY: Int, PHONE: LBString, ACCTBAL: Double, MKTSEGMENT: LBString, COMMENT: LBString): CUSTOMERRecord = {
    new CUSTOMERRecord(CUSTKEY, NAME, ADDRESS, NATIONKEY, PHONE, ACCTBAL, MKTSEGMENT, COMMENT)
  }

  case class SUPPLIERRecord(
    val S_SUPPKEY: Int,
    val S_NAME: LBString,
    val S_ADDRESS: LBString,
    val S_NATIONKEY: Int,
    val S_PHONE: LBString,
    val S_ACCTBAL: Double,
    val S_COMMENT: LBString) extends CaseClassRecord {
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
  def newSUPPLIERRecord(SUPPKEY: Int, NAME: LBString, ADDRESS: LBString, NATIONKEY: Int, PHONE: LBString, ACCTBAL: Double, COMMENT: LBString): SUPPLIERRecord = {
    new SUPPLIERRecord(SUPPKEY, NAME, ADDRESS, NATIONKEY, PHONE, ACCTBAL, COMMENT)
  }

  case class PARTSUPPRecord(
    val PS_PARTKEY: Int,
    val PS_SUPPKEY: Int,
    val PS_AVAILQTY: Int,
    val PS_SUPPLYCOST: Double,
    val PS_COMMENT: LBString) extends CaseClassRecord {
    def getField(key: String): Option[Any] = key match {
      case "PS_PARTKEY"    => Some(PS_PARTKEY)
      case "PS_SUPPKEY"    => Some(PS_SUPPKEY)
      case "PS_AVAILQTY"   => Some(PS_AVAILQTY)
      case "PS_SUPPLYCOST" => Some(PS_SUPPLYCOST)
      case "PS_COMMENT"    => Some(PS_COMMENT)
      case _               => None
    }
  }
  def newPARTSUPPRecord(PARTKEY: Int, SUPPKEY: Int, AVAILQTY: Int, SUPPLYCOST: Double, COMMENT: LBString): PARTSUPPRecord = {
    new PARTSUPPRecord(PARTKEY, SUPPKEY, AVAILQTY, SUPPLYCOST, COMMENT)
  }

  case class REGIONRecord(
    val R_REGIONKEY: Int,
    val R_NAME: LBString,
    val R_COMMENT: LBString) extends CaseClassRecord {
    def getField(key: String): Option[Any] = key match {
      case "R_REGIONKEY" => Some(R_REGIONKEY)
      case "R_NAME"      => Some(R_NAME)
      case "R_COMMENT"   => Some(R_COMMENT)
      case _             => None
    }
  }
  def newREGIONRecord(REGIONKEY: Int, NAME: LBString, COMMENT: LBString): REGIONRecord = {
    new REGIONRecord(REGIONKEY, NAME, COMMENT)
  }

  case class NATIONRecord(
    val N_NATIONKEY: Int,
    val N_NAME: LBString,
    val N_REGIONKEY: Int,
    val N_COMMENT: LBString) extends CaseClassRecord {
    def getField(key: String): Option[Any] = key match {
      case "N_NATIONKEY" => Some(N_NATIONKEY)
      case "N_NAME"      => Some(N_NAME)
      case "N_REGIONKEY" => Some(N_REGIONKEY)
      case "N_COMMENT"   => Some(N_COMMENT)
      case _             => None
    }
  }
  def newNATIONRecord(NATIONKEY: Int, NAME: LBString, REGIONKEY: Int, COMMENT: LBString): NATIONRecord = {
    new NATIONRecord(NATIONKEY, NAME, REGIONKEY, COMMENT)
  }

  case class PARTRecord(
    val P_PARTKEY: Int,
    val P_NAME: LBString,
    val P_MFGR: LBString,
    val P_BRAND: LBString,
    val P_TYPE: LBString,
    val P_SIZE: Int,
    val P_CONTAINER: LBString,
    val P_RETAILPRICE: Double,
    val P_COMMENT: LBString) extends CaseClassRecord {
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
  def newPARTRecord(PARTKEY: Int, NAME: LBString, MFGR: LBString, BRAND: LBString, TYPE: LBString, SIZE: Int, CONTAINER: LBString, RETAILPRICE: Double, COMMENT: LBString): PARTRecord = {
    new PARTRecord(PARTKEY, NAME, MFGR, BRAND, TYPE, SIZE, CONTAINER, RETAILPRICE, COMMENT)
  }
}