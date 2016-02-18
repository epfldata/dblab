package ch.epfl.data
package dblab.legobase
package tpch

import sc.pardis.types._
import schema._
import utils.Utilities._

object TPCHSchema {
  def getSchema(folderLocation: String, scalingFactor: Double): Schema = {
    val lineItemTable = {
      val L_ORDERKEY: Attribute = "L_ORDERKEY" -> IntType
      val L_LINENUMBER: Attribute = "L_LINENUMBER" -> IntType
      val L_SHIPMODE = Attribute("L_SHIPMODE", VarCharType(10), List(Compressed))
      val L_SHIPINSTRUCT = Attribute("L_SHIPINSTRUCT", VarCharType(25), List(Compressed))

      new Table("LINEITEM", List(
        L_ORDERKEY,
        "L_PARTKEY" -> IntType,
        "L_SUPPKEY" -> IntType,
        L_LINENUMBER,
        "L_QUANTITY" -> DoubleType,
        "L_EXTENDEDPRICE" -> DoubleType,
        "L_DISCOUNT" -> DoubleType,
        "L_TAX" -> DoubleType,
        "L_RETURNFLAG" -> CharType,
        "L_LINESTATUS" -> CharType,
        "L_SHIPDATE" -> DateType,
        "L_COMMITDATE" -> DateType,
        "L_RECEIPTDATE" -> DateType,
        L_SHIPINSTRUCT,
        L_SHIPMODE,
        ("L_COMMENT" -> VarCharType(44))),
        List(
          PrimaryKey(List(L_ORDERKEY, L_LINENUMBER)),
          ForeignKey("LINEITEM", "ORDERS", List(("L_ORDERKEY", "O_ORDERKEY"))),
          ForeignKey("LINEITEM", "PARTSUPP", List(("L_PARTKEY", "PS_PARTKEY"), ("L_SUPPKEY", "PS_SUPPKEY")))),
        folderLocation + "lineitem.tbl", (scalingFactor * 6000000).toLong)
    }

    val regionTable = {
      val R_REGIONKEY: Attribute = "R_REGIONKEY" -> IntType
      val R_NAME = Attribute("R_NAME", VarCharType(25), List(Compressed))

      new Table("REGION", List(
        R_REGIONKEY,
        R_NAME,
        ("R_COMMENT" -> VarCharType(152))),
        List(PrimaryKey(List(R_REGIONKEY)),
          Continuous(R_REGIONKEY, 0)),
        folderLocation + "region.tbl", 5)
    }

    val nationTable = {
      val N_NATIONKEY: Attribute = "N_NATIONKEY" -> IntType
      val N_NAME: Attribute = Attribute("N_NAME", VarCharType(25), List(Compressed))

      new Table("NATION", List(
        N_NATIONKEY,
        N_NAME,
        "N_REGIONKEY" -> IntType,
        ("N_COMMENT" -> VarCharType(152))),
        List(
          PrimaryKey(List(N_NATIONKEY)),
          Continuous(N_NATIONKEY, 0),
          ForeignKey("NATION", "REGION", List(("N_REGIONKEY", "R_REGIONKEY")))),
        folderLocation + "nation.tbl", 25)
    }

    val supplierTable = {
      val sk: Attribute = "S_SUPPKEY" -> IntType

      new Table("SUPPLIER", List(
        sk,
        ("S_NAME" -> VarCharType(25)),
        ("S_ADDRESS" -> VarCharType(40)),
        "S_NATIONKEY" -> IntType,
        ("S_PHONE" -> VarCharType(15)),
        "S_ACCTBAL" -> DoubleType,
        ("S_COMMENT" -> VarCharType(101))),
        List(
          PrimaryKey(List(sk)),
          Continuous(sk, 1),
          ForeignKey("SUPPLIER", "NATION", List(("S_NATIONKEY", "N_NATIONKEY")))),
        folderLocation + "supplier.tbl", (scalingFactor * 10000).toLong)
    }

    val partTable = {
      val P_PARTKEY: Attribute = "P_PARTKEY" -> IntType
      val P_BRAND = Attribute("P_BRAND", VarCharType(10), List(Compressed))
      val P_TYPE = Attribute("P_TYPE", VarCharType(25), List(Compressed))
      val P_MFGR = Attribute("P_MFGR", VarCharType(25), List(Compressed))
      val P_CONTAINER = Attribute("P_CONTAINER", VarCharType(10), List(Compressed))

      new Table("PART", List(
        P_PARTKEY,
        ("P_NAME" -> VarCharType(55)),
        P_MFGR,
        P_BRAND,
        P_TYPE,
        "P_SIZE" -> IntType,
        P_CONTAINER,
        "P_RETAILPRICE" -> DoubleType,
        ("P_COMMENT" -> VarCharType(23))),
        List(
          PrimaryKey(List(P_PARTKEY)),
          Continuous(P_PARTKEY, 1)),
        folderLocation + "part.tbl", (scalingFactor * 200000).toLong)
    }

    val partsuppTable = {
      val pk: Attribute = "PS_PARTKEY" -> IntType
      val sk: Attribute = "PS_SUPPKEY" -> IntType

      new Table("PARTSUPP", List(
        pk,
        sk,
        "PS_AVAILQTY" -> IntType,
        "PS_SUPPLYCOST" -> DoubleType,
        ("PS_COMMENT" -> VarCharType(199))),
        List(
          PrimaryKey(List(pk, sk)),
          ForeignKey("PARTSUPP", "PART", List(("PS_PARTKEY", "P_PARTKEY"))),
          ForeignKey("PARTSUPP", "SUPPLIER", List(("PS_SUPPKEY", "S_SUPPKEY")))),
        folderLocation + "partsupp.tbl", (scalingFactor * 800000).toLong)
    }

    val customerTable = {
      val ck: Attribute = "C_CUSTKEY" -> IntType
      val C_MKTSEGMENT = Attribute("C_MKTSEGMENT", VarCharType(10), List(Compressed))

      new Table("CUSTOMER", List(
        ck,
        ("C_NAME" -> VarCharType(25)),
        ("C_ADDRESS" -> VarCharType(40)),
        "C_NATIONKEY" -> IntType,
        ("C_PHONE" -> VarCharType(15)),
        "C_ACCTBAL" -> DoubleType,
        C_MKTSEGMENT,
        ("C_COMMENT" -> VarCharType(117))),
        List(
          PrimaryKey(List(ck)),
          Continuous(ck, 1),
          ForeignKey("CUSTOMER", "NATION", List(("C_NATIONKEY", "N_NATIONKEY")))),
        folderLocation + "customer.tbl", (scalingFactor * 150000).toLong)
    }

    val ordersTable = {
      val O_ORDERKEY: Attribute = "O_ORDERKEY" -> IntType
      val O_COMMENT = Attribute("O_COMMENT", VarCharType(79), List(Compressed))
      val O_ORDERPRIORITY = Attribute("O_ORDERPRIORITY", VarCharType(15))

      new Table("ORDERS", List(
        O_ORDERKEY,
        "O_CUSTKEY" -> IntType,
        "O_ORDERSTATUS" -> CharType,
        "O_TOTALPRICE" -> DoubleType,
        "O_ORDERDATE" -> DateType,
        O_ORDERPRIORITY,
        ("O_CLERK" -> VarCharType(15)),
        "O_SHIPPRIORITY" -> IntType,
        O_COMMENT),
        List(
          PrimaryKey(List(O_ORDERKEY)),
          ForeignKey("ORDERS", "CUSTOMER", List(("O_CUSTKEY", "C_CUSTKEY")))),
        folderLocation + "orders.tbl", (scalingFactor * 1500000).toLong)
    }

    val tpchSchema = new Schema(List(lineItemTable, regionTable, nationTable, supplierTable, partTable, partsuppTable, customerTable, ordersTable))

    val YEARS = 7

    // Register all statistics here
    tpchSchema.stats += "CARDINALITY_ORDERS" -> ordersTable.rowCount
    tpchSchema.stats += "CARDINALITY_CUSTOMER" -> customerTable.rowCount
    tpchSchema.stats += "CARDINALITY_LINEITEM" -> lineItemTable.rowCount
    tpchSchema.stats += "CARDINALITY_SUPPLIER" -> supplierTable.rowCount
    tpchSchema.stats += "CARDINALITY_PARTSUPP" -> partsuppTable.rowCount
    tpchSchema.stats += "CARDINALITY_PART" -> partTable.rowCount
    tpchSchema.stats += "CARDINALITY_NATION" -> nationTable.rowCount
    tpchSchema.stats += "CARDINALITY_REGION" -> regionTable.rowCount

    tpchSchema.stats += "CARDINALITY_Q1GRP" -> 4
    tpchSchema.stats += "CARDINALITY_Q3GRP" -> ordersTable.rowCount / 100
    tpchSchema.stats += "CARDINALITY_Q9GRP" -> nationTable.rowCount * YEARS
    tpchSchema.stats += "CARDINALITY_Q10GRP" -> customerTable.rowCount

    tpchSchema.stats += "DISTINCT_L_SHIPMODE" -> YEARS
    tpchSchema.stats += "DISTINCT_L_RETURNFLAG" -> 3
    tpchSchema.stats += "DISTINCT_L_LINESTATUS" -> 2
    tpchSchema.stats += "DISTINCT_L_ORDERKEY" -> ordersTable.rowCount * 5
    tpchSchema.stats += "DISTINCT_L_PARTKEY" -> partTable.rowCount
    tpchSchema.stats += "DISTINCT_L_SUPPKEY" -> supplierTable.rowCount
    tpchSchema.stats += "DISTINCT_N_NAME" -> nationTable.rowCount
    tpchSchema.stats += "DISTINCT_N_NATIONKEY" -> nationTable.rowCount
    tpchSchema.stats += "DISTINCT_O_SHIPPRIORITY" -> 1
    tpchSchema.stats += "DISTINCT_O_ORDERDATE" -> 365 * YEARS
    tpchSchema.stats += "DISTINCT_O_ORDERPRIORITY" -> 5
    tpchSchema.stats += "DISTINCT_O_ORDERKEY" -> ordersTable.rowCount * 5
    tpchSchema.stats += "DISTINCT_O_CUSTKEY" -> customerTable.rowCount
    tpchSchema.stats += "DISTINCT_P_PARTKEY" -> partTable.rowCount
    tpchSchema.stats += "DISTINCT_P_BRAND" -> 25
    tpchSchema.stats += "DISTINCT_P_SIZE" -> 50
    tpchSchema.stats += "DISTINCT_P_TYPE" -> 150
    tpchSchema.stats += "DISTINCT_PS_PARTKEY" -> partTable.rowCount
    tpchSchema.stats += "DISTINCT_PS_SUPPKEY" -> supplierTable.rowCount
    tpchSchema.stats += "DISTINCT_PS_AVAILQTY" -> 9999
    tpchSchema.stats += "DISTINCT_S_NAME" -> supplierTable.rowCount
    tpchSchema.stats += "DISTINCT_S_NATIONKEY" -> nationTable.rowCount
    tpchSchema.stats += "DISTINCT_S_SUPPKEY" -> supplierTable.rowCount
    tpchSchema.stats += "DISTINCT_C_CUSTKEY" -> customerTable.rowCount
    tpchSchema.stats += "DISTINCT_C_NAME" -> customerTable.rowCount
    tpchSchema.stats += "DISTINCT_C_NATIONKEY" -> nationTable.rowCount
    tpchSchema.stats += "DISTINCT_R_REGIONKEY" -> 5

    tpchSchema.stats.conflicts("P_PARTKEY") = 4
    tpchSchema.stats.conflicts("L_ORDERKEY") = 16
    tpchSchema.stats.conflicts("C_NATIONKEY") = customerTable.rowCount / 20
    tpchSchema.stats.conflicts("S_NATIONKEY") = supplierTable.rowCount / 20

    tpchSchema.stats += "NUM_YEARS_ALL_DATES" -> YEARS

    tpchSchema
  }
}
