package ch.epfl.data
package dblab
package benchmarks
package tpch

import sc.pardis.annotations.{ deep, needs, noDeepExt }
import sc.pardis.shallow.{ CaseClassRecord, OptimalString }

@deep case class Q1GRPRecord(
  val L_RETURNFLAG: Char,
  val L_LINESTATUS: Char) extends CaseClassRecord

@deep case class Q3GRPRecord(
  val L_ORDERKEY: Int,
  val O_ORDERDATE: Int,
  val O_SHIPPRIORITY: Int) extends CaseClassRecord

@needs[OptimalString] @deep case class Q7GRPRecord(
  val SUPP_NATION: OptimalString,
  val CUST_NATION: OptimalString,
  val L_YEAR: Int) extends CaseClassRecord

@needs[OptimalString] @deep case class Q9GRPRecord(
  val NATION: OptimalString,
  val O_YEAR: Int) extends CaseClassRecord

@needs[OptimalString] @deep case class Q10GRPRecord(
  val C_CUSTKEY: Int,
  val C_NAME: OptimalString,
  val C_ACCTBAL: Double,
  val C_PHONE: OptimalString,
  val N_NAME: OptimalString,
  val C_ADDRESS: OptimalString,
  val C_COMMENT: OptimalString) extends CaseClassRecord

@deep @noDeepExt case class Q13IntRecord(
  var count: Int) extends CaseClassRecord

@needs[OptimalString] @deep case class Q16GRPRecord1(
  val P_BRAND: OptimalString,
  val P_TYPE: OptimalString,
  val P_SIZE: Int,
  val PS_SUPPKEY: Int) extends CaseClassRecord

@needs[OptimalString] @deep case class Q16GRPRecord2(
  val P_BRAND: OptimalString,
  val P_TYPE: OptimalString,
  val P_SIZE: Int) extends CaseClassRecord

@needs[OptimalString] @deep case class Q18GRPRecord(
  val C_NAME: OptimalString,
  val C_CUSTKEY: Int,
  val O_ORDERKEY: Int,
  val O_ORDERDATE: Int,
  val O_TOTALPRICE: Double) extends CaseClassRecord

@deep case class Q20GRPRecord(
  val PS_PARTKEY: Int,
  val PS_SUPPKEY: Int,
  val PS_AVAILQTY: Int) extends CaseClassRecord