package ch.epfl.data
package legobase
package queryengine

import ch.epfl.data.pardis.shallow.{ CaseClassRecord }

case class AGGRecord[B](
  val key: B,
  val aggs: Array[Double]) extends CaseClassRecord {
  def getField(key: String): Option[Any] = key match {
    case "key"  => Some(key)
    case "aggs" => Some(aggs)
    case _      => None
  }
}

case class WindowRecord[B, C](
  val key: B,
  val wnd: C) extends CaseClassRecord {
  def getField(key: String): Option[Any] = key match {
    case "key"  => Some(key)
    case "aggs" => Some(wnd)
    case _      => None
  }
}

case class GroupByClass(val L_RETURNFLAG: java.lang.Character, val L_LINESTATUS: java.lang.Character) extends CaseClassRecord {
  def getField(key: String): Option[Any] = key match {
    case "L_RETURNFLAG" => Some(L_RETURNFLAG)
    case "L_LINESTATUS" => Some(L_LINESTATUS)
    case _              => None
  }
}

case class Q3GRPRecord(
  val L_ORDERKEY: Int,
  val O_ORDERDATE: Long,
  val O_SHIPPRIORITY: Int) extends CaseClassRecord {
  def getField(key: String): Option[Any] = key match {
    case "L_ORDERKEY"     => Some(L_ORDERKEY)
    case "O_ORDERDATE"    => Some(O_ORDERDATE)
    case "O_SHIPPRIORITY" => Some(O_SHIPPRIORITY)
    case _                => None
  }
}

case class Q7GRPRecord(
  val SUPP_NATION: LBString,
  val CUST_NATION: LBString,
  val L_YEAR: Long) extends CaseClassRecord {
  def getField(key: String): Option[Any] = key match {
    case "SUPP_NATION" => Some(SUPP_NATION)
    case "CUST_NATION" => Some(CUST_NATION)
    case "L_YEAR"      => Some(L_YEAR)
    case _             => None
  }
}

case class Q9GRPRecord(
  val NATION: LBString,
  val O_YEAR: Long) extends CaseClassRecord {
  def getField(key: String): Option[Any] = key match {
    case "NATION" => Some(NATION)
    case "O_YEAR" => Some(O_YEAR)
    case _        => None
  }
}

case class Q10GRPRecord(
  val C_CUSTKEY: Int,
  val C_NAME: LBString,
  val C_ACCTBAL: Double,
  val C_PHONE: LBString,
  val N_NAME: LBString,
  val C_ADDRESS: LBString,
  val C_COMMENT: LBString) extends CaseClassRecord {
  def getField(key: String): Option[Any] = key match {
    case "C_CUSTKEY" => Some(C_CUSTKEY)
    case "C_NAME"    => Some(C_NAME)
    case "C_ACCTBAL" => Some(C_ACCTBAL)
    case "C_PHONE"   => Some(C_PHONE)
    case "N_NAME"    => Some(N_NAME)
    case "C_ADDRESS" => Some(C_ADDRESS)
    case "C_COMMENT" => Some(C_COMMENT)
    case _           => None
  }
}

case class Q16GRPRecord1(
  val P_BRAND: LBString,
  val P_TYPE: LBString,
  val P_SIZE: Int,
  val PS_SUPPKEY: Int) extends CaseClassRecord {
  def getField(key: String): Option[Any] = key match {
    case "P_BRAND"    => Some(P_BRAND)
    case "P_TYPE"     => Some(P_TYPE)
    case "P_SIZE"     => Some(P_SIZE)
    case "PS_SUPPKEY" => Some(PS_SUPPKEY)
    case _            => None
  }
}
case class Q16GRPRecord2(
  val P_BRAND: LBString,
  val P_TYPE: LBString,
  val P_SIZE: Int) extends CaseClassRecord {
  def getField(key: String): Option[Any] = key match {
    case "P_BRAND" => Some(P_BRAND)
    case "P_TYPE"  => Some(P_TYPE)
    case "P_SIZE"  => Some(P_SIZE)
    case _         => None
  }
}

case class Q18GRPRecord(
  val C_NAME: LBString,
  val C_CUSTKEY: Int,
  val O_ORDERKEY: Int,
  val O_ORDERDATE: Long,
  val O_TOTALPRICE: Double) extends CaseClassRecord {
  def getField(key: String): Option[Any] = key match {
    case "C_NAME"       => Some(C_NAME)
    case "C_CUSTKEY"    => Some(C_CUSTKEY)
    case "O_ORDERKEY"   => Some(O_ORDERKEY)
    case "O_ORDERDATE"  => Some(O_ORDERDATE)
    case "O_TOTALPRICE" => Some(O_TOTALPRICE)
    case _              => None
  }
}

case class Q20GRPRecord(
  val PS_PARTKEY: Int,
  val PS_SUPPKEY: Int,
  val PS_AVAILQTY: Int) extends CaseClassRecord {
  def getField(key: String): Option[Any] = key match {
    case "PS_PARTKEY"  => Some(PS_PARTKEY)
    case "PS_SUPPKEY"  => Some(PS_SUPPKEY)
    case "PS_AVAILQTY" => Some(PS_AVAILQTY)
    case _             => None
  }
}