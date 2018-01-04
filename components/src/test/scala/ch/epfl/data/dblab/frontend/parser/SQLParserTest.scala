package ch.epfl.data
package dblab
package frontend
package parser

import org.scalatest._
import Matchers._

object TPCHQueries {
  val q1 = "SELECT l_returnflag, l_linestatus, SUM(l_quantity) AS sum_qty, SUM(l_extendedprice) AS sum_base_price, SUM(l_extendedprice * (1 - l_discount)) AS sum_disc_price, SUM(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge, AVG(l_quantity) AS avg_qty, AVG(l_extendedprice) AS avg_price, AVG(l_discount) AS avg_disc, COUNT(*) AS count_order FROM lineitem WHERE l_shipdate <= DATE '1998-09-02' GROUP BY l_returnflzag, l_linestatus ORDER BY l_returnflag, l_linestatus"
  val q2 = "SELECT s_acctbal, s_name, n_name, p_partkey, p_mfgr, s_address, s_phone, s_comment FROM part, supplier, partsupp, nation, region WHERE p_partkey = ps_partkey AND s_suppkey = ps_suppkey AND p_size = 43 AND p_type LIKE '%TIN' AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey AND r_name = 'AFRICA' AND ps_supplycost = (SELECT MIN(ps_supplycost) FROM partsupp, supplier, nation, region WHERE p_partkey = ps_partkey AND s_suppkey = ps_suppkey AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey AND r_name = 'AFRICA') LIMIT 100"
  val q3 = "SELECT l_orderkey, SUM(l_extendedprice * (1 - l_discount)) AS revenue, o_orderdate, o_shippriority FROM customer, orders, lineitem WHERE c_mktsegment = 'HOUSEHOLD' AND c_custkey = o_custkey AND l_orderkey = o_orderkey AND o_orderdate < DATE '1995-03-04' AND l_shipdate > DATE '1995-03-04' GROUP BY l_orderkey, o_orderdate, o_shippriority ORDER BY revenue DESC, o_orderdate LIMIT 10"
  val q4 = "SELECT o_orderpriority, COUNT(*) AS order_count FROM orders WHERE o_orderdate >= DATE '1993-08-01' AND o_orderdate < DATE '1993-11-01' AND EXISTS (SELECT * FROM lineitem WHERE l_orderkey = o_orderkey AND l_commitdate < l_receiptdate) GROUP BY o_orderpriority ORDER BY o_orderpriority"
  val q5 = "SELECT n_name, SUM(l_extendedprice * (1 - l_discount)) AS revenue FROM customer, orders, lineitem, supplier, nation, region WHERE c_custkey = o_custkey AND l_orderkey = o_orderkey AND l_suppkey = s_suppkey AND c_nationkey = s_nationkey AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey AND r_name = 'ASIA' AND o_orderdate >= DATE '1996-01-01' AND o_orderdate < DATE '1997-01-01' GROUP BY n_name ORDER BY revenue DESC"
  val q6 = "SELECT SUM(l_extendedprice * l_discount) AS revenue FROM lineitem WHERE l_shipdate >= DATE '1996-01-01' AND l_shipdate < DATE '1997-01-01' AND l_discount BETWEEN 0.09 - 0.01 AND 0.09 + 0.01 AND l_quantity < 24"
}

class SQLParserTest extends FlatSpec {

  "SQLParser" should "parse Q1 correctly" in {
    val parser = SQLParser
    val r = parser.parse(TPCHQueries.q1)
    r should not be None
  }

  "SQLParser" should "parse Q2 correctly" in {
    val parser = SQLParser
    val r = parser.parse(TPCHQueries.q2)
    r should not be None
  }

  "SQLParser" should "parse Q3 correctly" in {
    val parser = SQLParser
    val r = parser.parse(TPCHQueries.q3)
    r should not be None
  }

  "SQLParser" should "parse Q4 correctly" in {
    val parser = SQLParser
    val r = parser.parse(TPCHQueries.q4)
    r should not be None
  }

  "SQLParser" should "parse Q5 correctly" in {
    val parser = SQLParser
    val r = parser.parse(TPCHQueries.q5)
    r should not be None
  }

  "SQLParser" should "parse Q6 correctly" in {
    val parser = SQLParser
    val r = parser.parse(TPCHQueries.q6)
    r should not be None
  }

  "SQLParser" should "parse simple SELECT/FROM correctly" in {
    val parser = SQLParser
    val r = parser.parse("SELECT * FROM tbl t")
    r should not be None
  }

  // "SQLParser" should "parse simple SELECT/FROM (case insensitive) correctly" in {
  //   val parser = SQLParser
  //   val r = parser.parse("select * FROM table t")
  //   r should not be None
  // }

  "SQLParser" should "parse single expression correctly" in {
    val parser = SQLParser
    val r = parser.parse("SELECT column1 FROM tbl")
    r should not be None
  }

  "SQLParser" should "parse explicit columns correctly" in {
    val parser = SQLParser
    val r = parser.parse("SELECT column1, ident.column2 FROM tbl")
    r should not be None
  }

  "SQLParser" should "parse aggregate functions correctly" in {
    val parser = SQLParser
    val r = parser.parse("SELECT SUM(column1), MIN(ident.column2) FROM tbl")
    r should not be None
  }

  "SQLParser" should "parse aliased expression correctly" in {
    val parser = SQLParser
    val r = parser.parse("SELECT SUM(column1) AS theSum, MIN(ident.column2) AS minimum FROM tbl")
    r should not be None
  }

  "SQLParser" should "parse arithmetic operations correctly" in {
    val parser = SQLParser
    val r = parser.parse("SELECT SUM(column1) / 10, MIN(ident.column2) + 125.50 FROM tbl")
    r should not be None
  }

  "SQLParser" should "parse GROUP BY with HAVING correctly" in {
    val parser = SQLParser
    val r = parser.parse("SELECT column1 FROM tbl GROUP BY column1, tbl.column2 HAVING SUM(tbl.column3) > 12345")
    r should not be None
  }

  "SQLParser" should "parse WHERE correctly" in {
    val parser = SQLParser
    val r = parser.parse("SELECT column1 FROM tbl WHERE column2 > 2.51 AND 1 != 0 AND tbl.column3 BETWEEN 5 AND 10")
    r should not be None
  }

  "SQLParser" should "parse dates correctly" in {
    val parser = SQLParser
    val r = parser.parse("SELECT * FROM lineitem WHERE DATE '1998-09-02'")
    r should not be None
  }

  "SQLParser" should "parse LIMIT correctly" in {
    val parser = SQLParser
    val r = parser.parse("SELECT * FROM tbl LIMIT 20")
    r should not be None
  }

  "SQLParser" should "parse csv adaptor" in {
    val parser = SQLParser
    val r = parser.parseStream(
      """CREATE STREAM FOO (ok INT) FROM FILE 'foo.csv'
        LINE DELIMITED CSV (delimiter := '|'); SELECT * FROM FOO;""")
    r should not be None
  }

  "SQLParser" should "parse IN LIST" in {
    val parser = SQLParser
    val r = parser.parseStream("SELECT * FROM R WHERE A IN LIST (1, 2, 3);")
    r should not be None
  }

  "SQLParser" should "parse schemas" in {
    val parser = SQLParser
    val s = "CREATE STREAM LINEITEM (\n        orderkey       INT,\n        partkey        INT,\n        suppkey        INT,\n        linenumber     INT,\n        quantity       DECIMAL,\n        extendedprice  DECIMAL,\n        discount       DECIMAL,\n        tax            DECIMAL,\n        returnflag     CHAR(1),\n        linestatus     CHAR(1),\n        shipdate       DATE,\n        commitdate     DATE,\n        receiptdate    DATE,\n        shipinstruct   CHAR(25),\n        shipmode       CHAR(10),\n        comment        VARCHAR(44)\n    )\n  FROM FILE '../../experiments/data/tpch/standard/lineitem.csv'\n  LINE DELIMITED CSV (delimiter := '|');\n\n\nCREATE STREAM ORDERS (\n        orderkey       INT,\n        custkey        INT,\n        orderstatus    CHAR(1),\n        totalprice     DECIMAL,\n        orderdate      DATE,\n        orderpriority  CHAR(15),\n        clerk          CHAR(15),\n        shippriority   INT,\n        comment        VARCHAR(79)\n    )\n  FROM FILE '../../experiments/data/tpch/standard/orders.csv'\n  LINE DELIMITED CSV (delimiter := '|');\n\nCREATE STREAM PART (\n        partkey      INT,\n        name         VARCHAR(55),\n        mfgr         CHAR(25),\n        brand        CHAR(10),\n        type         VARCHAR(25),\n        size         INT,\n        container    CHAR(10),\n        retailprice  DECIMAL,\n        comment      VARCHAR(23)\n    )\n  FROM FILE '../../experiments/data/tpch/standard/part.csv'\n  LINE DELIMITED CSV (delimiter := '|');\n\n\nCREATE STREAM CUSTOMER (\n        custkey      INT,\n        name         VARCHAR(25),\n        address      VARCHAR(40),\n        nationkey    INT,\n        phone        CHAR(15),\n        acctbal      DECIMAL,\n        mktsegment   CHAR(10),\n        comment      VARCHAR(117)\n    )\n  FROM FILE '../../experiments/data/tpch/standard/customer.csv'\n  LINE DELIMITED CSV (delimiter := '|');\n\nCREATE STREAM SUPPLIER (\n        suppkey      INT,\n        name         CHAR(25),\n        address      VARCHAR(40),\n        nationkey    INT,\n        phone        CHAR(15),\n        acctbal      DECIMAL,\n        comment      VARCHAR(101)\n    )\n  FROM FILE '../../experiments/data/tpch/standard/supplier.csv'\n  LINE DELIMITED CSV (delimiter := '|');\n\nCREATE STREAM PARTSUPP (\n        partkey      INT,\n        suppkey      INT,\n        availqty     INT,\n        supplycost   DECIMAL,\n        comment      VARCHAR(199)\n    )\n  FROM FILE '../../experiments/data/tpch/standard/partsupp.csv'\n  LINE DELIMITED CSV (delimiter := '|');\n\nCREATE TABLE NATION (\n        nationkey    INT,\n        name         CHAR(25),\n        regionkey    INT,\n        comment      VARCHAR(152)\n    )\n  FROM FILE '../../experiments/data/tpch/standard/nation.csv'\n  LINE DELIMITED CSV (delimiter := '|');\n\nCREATE TABLE REGION (\n        regionkey    INT,\n        name         CHAR(25),\n        comment      VARCHAR(152)\n    )\n  FROM FILE '../../experiments/data/tpch/standard/region.csv'\n  LINE DELIMITED CSV (delimiter := '|');\n\nSELECT * FROM nation;"
    val r = parser.parseStream(s)
    r should not be None
  }
  //  "SQLParser" should "parse TPCH queries" in {
  //    val parser = SQLParser
  //    val folder = "experimentation/tpch-sql"
  //    val f = new java.io.File(folder)
  //    val files = if (!f.exists) {
  //      throw new Exception(s"$f does not exist!")
  //    } else
  //      f.listFiles.filter(x => x.getName.startsWith("Q")).map(folder + "/" + _.getName).toList
  //    for (file <- files) {
  //      //      println(s"parsing $file")
  //      val r = parser.parseStream(scala.io.Source.fromFile(file).mkString)
  //      r should not be None
  //    }
  //  }
  //
  //  "SQLParser" should "parse DBToaster simple queries" in {
  //    val parser = SQLParser
  //    val folder = "experimentation/dbtoaster/queries/simple"
  //    val f = new java.io.File(folder)
  //    val files = if (!f.exists) {
  //      throw new Exception(s"$f does not exist!")
  //    } else
  //      f.listFiles.map(folder + "/" + _.getName).toList
  //    for (file <- files) {
  //      println(s"parsing $file")
  //      val r = parser.parseStream(scala.io.Source.fromFile(file).mkString)
  //      r should not be None
  //    }
  //  }
  //
  //
  //
  //  "SQLParser" should "parse DBToaster tpch queries" in {
  //    val parser = SQLParser
  //    val folder = "experimentation/dbtoaster/queries/tpch"
  //    val f = new java.io.File(folder)
  //    val files = if (!f.exists) {
  //      throw new Exception(s"$f does not exist!")
  //    } else
  //      f.listFiles.filter(x => x.getName.endsWith(".sql")).map(folder + "/" + _.getName).toList
  //    for (file <- files) {
  //      println(s"parsing $file")
  //      val r = parser.parseStream(scala.io.Source.fromFile(file).mkString)
  //      r should not be None
  //    }
  //  }

}
