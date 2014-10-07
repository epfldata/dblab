package ch.epfl.data
package legobase
package queryengine

import utils.Utilities._
import ch.epfl.data.pardis.shallow.{ CaseClassRecord }

object TPCHRelations {
  case class LINEITEMRecord(
    val L_ORDERKEY: Int,
    val L_PARTKEY: Int,
    val L_SUPPKEY: Int,
    val L_LINENUMBER: Int,
    val L_QUANTITY: Double,
    val L_EXTENDEDPRICE: Double,
    val L_DISCOUNT: Double,
    val L_TAX: Double,
    val L_RETURNFLAG: Char,
    val L_LINESTATUS: Char,
    val L_SHIPDATE: Int,
    val L_COMMITDATE: Int,
    val L_RECEIPTDATE: Int,
    val L_SHIPINSTRUCT: LBString,
    val L_SHIPMODE: LBString,
    val L_COMMENT: LBString) extends CaseClassRecord

  case class ORDERSRecord(
    val O_ORDERKEY: Int,
    val O_CUSTKEY: Int,
    val O_ORDERSTATUS: Char,
    val O_TOTALPRICE: Double,
    val O_ORDERDATE: Int,
    val O_ORDERPRIORITY: LBString,
    val O_CLERK: LBString,
    val O_SHIPPRIORITY: Int,
    val O_COMMENT: LBString) extends CaseClassRecord

  case class CUSTOMERRecord(
    val C_CUSTKEY: Int,
    val C_NAME: LBString,
    val C_ADDRESS: LBString,
    val C_NATIONKEY: Int,
    val C_PHONE: LBString,
    val C_ACCTBAL: Double,
    val C_MKTSEGMENT: LBString,
    val C_COMMENT: LBString) extends CaseClassRecord

  case class SUPPLIERRecord(
    val S_SUPPKEY: Int,
    val S_NAME: LBString,
    val S_ADDRESS: LBString,
    val S_NATIONKEY: Int,
    val S_PHONE: LBString,
    val S_ACCTBAL: Double,
    val S_COMMENT: LBString) extends CaseClassRecord

  case class PARTSUPPRecord(
    val PS_PARTKEY: Int,
    val PS_SUPPKEY: Int,
    val PS_AVAILQTY: Int,
    val PS_SUPPLYCOST: Double,
    val PS_COMMENT: LBString) extends CaseClassRecord

  case class REGIONRecord(
    val R_REGIONKEY: Int,
    val R_NAME: LBString,
    val R_COMMENT: LBString) extends CaseClassRecord

  case class NATIONRecord(
    val N_NATIONKEY: Int,
    val N_NAME: LBString,
    val N_REGIONKEY: Int,
    val N_COMMENT: LBString) extends CaseClassRecord

  case class PARTRecord(
    val P_PARTKEY: Int,
    val P_NAME: LBString,
    val P_MFGR: LBString,
    val P_BRAND: LBString,
    val P_TYPE: LBString,
    val P_SIZE: Int,
    val P_CONTAINER: LBString,
    val P_RETAILPRICE: Double,
    val P_COMMENT: LBString) extends CaseClassRecord
}
