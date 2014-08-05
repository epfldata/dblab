package ch.epfl.data
package legobase

import queryengine._
import queryengine.volcano._
import ch.epfl.data.pardis.shallow.{ CaseClassRecord }

trait Queries extends Q1 with Q2 with Q3 with Q4 with Q5 with Q6 with Q7 with Q8 with Q9
trait GenericQuery extends ScalaImpl with storagemanager.Loader {
  var profile = true

  def runQuery[T](query: => T): T = {
    if (profile) {
      utils.Utilities.time(query, "finish")
    } else {
      query
    }
  }
}

trait Q1 extends GenericQuery {
  case class GroupByClass(val L_RETURNFLAG: java.lang.Character, val L_LINESTATUS: java.lang.Character);
  def Q1(numRuns: Int) {
    val lineitemTable = loadLineitem()
    for (i <- 0 until numRuns) {
      runQuery {
        val constantDate: Long = parseDate("1998-08-11")
        val lineitemScan = SelectOp(ScanOp(lineitemTable))(x => x.L_SHIPDATE <= constantDate)
        val aggOp = AggOp(lineitemScan, 9)(x => new GroupByClass(
          x.L_RETURNFLAG, x.L_LINESTATUS))((t, currAgg) => { t.L_DISCOUNT + currAgg },
          (t, currAgg) => { t.L_QUANTITY + currAgg },
          (t, currAgg) => { t.L_EXTENDEDPRICE + currAgg },
          (t, currAgg) => { (t.L_EXTENDEDPRICE * (1.0 - t.L_DISCOUNT)) + currAgg },
          (t, currAgg) => { (t.L_EXTENDEDPRICE * (1.0 - t.L_DISCOUNT) * (1.0 + t.L_TAX)) + currAgg },
          (t, currAgg) => { currAgg + 1 })
        val mapOp = MapOp(aggOp)(kv => kv.aggs(6) = kv.aggs(1) / kv.aggs(5), // AVG(L_QUANTITY)
          kv => kv.aggs(7) = kv.aggs(2) / kv.aggs(5), // AVG(L_EXTENDEDPRICE)
          kv => kv.aggs(8) = kv.aggs(0) / kv.aggs(5)) // AVG(L_DISCOUNT)
        val sortOp = SortOp(mapOp)((kv1, kv2) => {
          var res = kv1.key.L_RETURNFLAG - kv2.key.L_RETURNFLAG
          if (res == 0)
            res = kv1.key.L_LINESTATUS - kv2.key.L_LINESTATUS
          res
        })
        val po = PrintOp(sortOp)(kv => printf("%c|%c|%.2f|%.2f|%.2f|%.2f|%.2f|%.2f|%.6f|%.0f\n",
          kv.key.L_RETURNFLAG, kv.key.L_LINESTATUS, kv.aggs(1), kv.aggs(2), kv.aggs(3), kv.aggs(4),
          kv.aggs(6), kv.aggs(7), kv.aggs(8), kv.aggs(5)))
        po.open
        po.next
        printf("(%d rows)\n", po.numRows)
        ()
      }
    }
  }
}

trait Q2 extends GenericQuery {
  def Q2(numRuns: Int) {
    val partTable = loadPart()
    val partsuppTable = loadPartsupp()
    val nationTable = loadNation()
    val regionTable = loadRegion()
    val supplierTable = loadSupplier()
    for (i <- 0 until numRuns) {
      runQuery {
        val africa = parseString("AFRICA")
        val tin = parseString("TIN")
        val partsuppScan = ScanOp(partsuppTable)
        val supplierScan = ScanOp(supplierTable)
        val jo1 = HashJoinOp(supplierScan, partsuppScan)((x, y) => x.S_SUPPKEY == y.PS_SUPPKEY)(x => x.S_SUPPKEY)(x => x.PS_SUPPKEY)
        val nationScan = ScanOp(nationTable)
        val jo2 = HashJoinOp(nationScan, jo1)((x, y) => x.N_NATIONKEY == y.S_NATIONKEY[Int])(x => x.N_NATIONKEY)(x => x.S_NATIONKEY[Int])
        val partScan = SelectOp(ScanOp(partTable))(x => x.P_SIZE == 43 && x.P_TYPE.endsWith(tin))
        val jo3 = HashJoinOp(partScan, jo2)((x, y) => x.P_PARTKEY == y.PS_PARTKEY[Int])(x => x.P_PARTKEY)(x => x.PS_PARTKEY[Int])
        val regionScan = SelectOp(ScanOp(regionTable))(x => x.R_NAME === africa) // for comparing equality of Array[Byte] we should use === instead of ==
        val jo4 = HashJoinOp(regionScan, jo3)((x, y) => x.R_REGIONKEY == y.N_REGIONKEY[Int])(x => x.R_REGIONKEY)(x => x.N_REGIONKEY[Int])
        val wo = WindowOp(jo4)(x => x.P_PARTKEY[Int])(x => x.minBy(y => y.PS_SUPPLYCOST[Double]))
        val so = SortOp(wo)((x, y) => {
          if (x.wnd.S_ACCTBAL[Double] < y.wnd.S_ACCTBAL[Double]) 1
          else if (x.wnd.S_ACCTBAL[Double] > y.wnd.S_ACCTBAL[Double]) -1
          else {
            var res = x.wnd.N_NAME[Array[Byte]] compare y.wnd.N_NAME[Array[Byte]]
            if (res == 0) {
              res = x.wnd.S_NAME[Array[Byte]] compare y.wnd.S_NAME[Array[Byte]]
              if (res == 0) res = x.wnd.P_PARTKEY[Int] - y.wnd.P_PARTKEY[Int]
            }
            res
          }
        })
        var j = 0
        val po = PrintOp(so)(e => {
          val kv = e.wnd
          printf("%.2f|%s|%s|%d|%s|%s|%s|%s\n", kv.S_ACCTBAL, (kv.S_NAME[Array[Byte]]).string, (kv.N_NAME[Array[Byte]]).string, kv.P_PARTKEY, (kv.P_MFGR[Array[Byte]]).string, (kv.S_ADDRESS[Array[Byte]]).string, (kv.S_PHONE[Array[Byte]]).string, (kv.S_COMMENT[Array[Byte]]).string)
          j += 1
        }, () => j < 100)
        po.open
        po.next
        printf("(%d rows)\n", po.numRows)
        ()
      }
    }
  }
}

trait Q3 extends GenericQuery {
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
  def newQ3GRPRecord(ORDERKEY: Int, ORDERDATE: Long, SHIPPRIORITY: Int) = {
    new Q3GRPRecord(ORDERKEY, ORDERDATE, SHIPPRIORITY)
  }

  def Q3(numRuns: Int) {
    val lineitemTable = loadLineitem()
    val ordersTable = loadOrders()
    val customerTable = loadCustomer()
    for (i <- 0 until numRuns) {
      runQuery({
        val constantDate = parseDate("1995-03-04")
        val scanCustomer = SelectOp(ScanOp(customerTable))(x => x.C_MKTSEGMENT === parseString("HOUSEHOLD"))
        val scanOrders = SelectOp(ScanOp(ordersTable))(x => x.O_ORDERDATE < constantDate)
        val scanLineitem = SelectOp(ScanOp(lineitemTable))(x => x.L_SHIPDATE > constantDate)
        val jo1 = HashJoinOp(scanCustomer, scanOrders)((x, y) => x.C_CUSTKEY == y.O_CUSTKEY)(x => x.C_CUSTKEY)(x => x.O_CUSTKEY)
        val jo2 = HashJoinOp(jo1, scanLineitem)((x, y) => x.O_ORDERKEY == y.L_ORDERKEY)(x => x.O_ORDERKEY[Int])(x => x.L_ORDERKEY)
        val aggOp = AggOp(jo2, 1)(x => newQ3GRPRecord(x.L_ORDERKEY[Int], x.O_ORDERDATE[Long], x.O_SHIPPRIORITY[Int]))((t, currAgg) => { currAgg + (t.L_EXTENDEDPRICE[Double] * (1.0 - t.L_DISCOUNT[Double])) })
        val sortOp = SortOp(aggOp)((kv1, kv2) => {
          val agg1 = kv1.aggs(0); val agg2 = kv2.aggs(0)
          if (agg1 < agg2) 1
          else if (agg1 > agg2) -1
          else {
            val k1 = kv1.key.O_ORDERDATE
            val k2 = kv2.key.O_ORDERDATE
            if (k1 < k2) -1
            else if (k1 > k2) 1
            else 0
          }
        })
        var i = 0
        val po = PrintOp(sortOp)(kv => {
          // TODO: The date is not printed properly (but is correct), and fails 
          // in the comparison with result file. Rest of fields OK.
          printf("%d|%.4f|%s|%d\n", kv.key.L_ORDERKEY, kv.aggs(0), new java.util.Date(kv.key.O_ORDERDATE).toString, kv.key.O_SHIPPRIORITY)
          i += 1
        }, () => i < 10)
        po.open
        po.next
        printf("(%d rows)\n", po.numRows)
        ()
      })
    }
  }
}

trait Q4 extends GenericQuery {
  def Q4(numRuns: Int) {
    val lineitemTable = loadLineitem()
    val ordersTable = loadOrders()
    for (i <- 0 until numRuns) {
      runQuery({
        val constantDate1: Long = parseDate("1993-11-01")
        val constantDate2: Long = parseDate("1993-08-01")
        val scanOrders = SelectOp(ScanOp(ordersTable))(x => x.O_ORDERDATE < constantDate1 && x.O_ORDERDATE >= constantDate2)
        val scanLineitem = SelectOp(ScanOp(lineitemTable))(x => x.L_COMMITDATE < x.L_RECEIPTDATE)
        val hj = LeftHashSemiJoinOp(scanOrders, scanLineitem)((x, y) => x.O_ORDERKEY == y.L_ORDERKEY)(x => x.O_ORDERKEY)(x => x.L_ORDERKEY)
        val aggOp = AggOp(hj, 1)(x => x.O_ORDERPRIORITY)(
          (t, currAgg) => { currAgg + 1 })
        val sortOp = SortOp(aggOp)((kv1, kv2) => {
          val k1 = kv1.key; val k2 = kv2.key
          k1 diff k2
        })
        val po = PrintOp(sortOp)(kv => printf("%s|%.0f\n", kv.key.string, kv.aggs(0)))
        po.open
        po.next
        printf("(%d rows)\n", po.numRows)
        ()
      })
    }
  }
}

trait Q5 extends GenericQuery {
  def Q5(numRuns: Int) {
    val lineitemTable = loadLineitem()
    val nationTable = loadNation()
    val customerTable = loadCustomer()
    val supplierTable = loadSupplier()
    val regionTable = loadRegion()
    val ordersTable = loadOrders()
    for (i <- 0 until numRuns) {
      runQuery({
        val constantDate1 = parseDate("1996-01-01")
        val constantDate2 = parseDate("1997-01-01")
        val scanRegion = SelectOp(ScanOp(regionTable))(x => x.R_NAME === parseString("ASIA"))
        val scanNation = ScanOp(nationTable)
        val scanSupplier = ScanOp(supplierTable)
        val scanCustomer = ScanOp(customerTable)
        val scanLineitem = ScanOp(lineitemTable)
        val scanOrders = SelectOp(ScanOp(ordersTable))(x => x.O_ORDERDATE >= constantDate1 && x.O_ORDERDATE < constantDate2)
        val jo1 = HashJoinOp(scanRegion, scanNation)((x, y) => x.R_REGIONKEY == y.N_REGIONKEY)(x => x.R_REGIONKEY)(x => x.N_REGIONKEY)
        val jo2 = HashJoinOp(jo1, scanSupplier)((x, y) => x.N_NATIONKEY == y.S_NATIONKEY)(x => x.N_NATIONKEY[Int])(x => x.S_NATIONKEY)
        val jo3 = HashJoinOp(jo2, scanCustomer)((x, y) => x.N_NATIONKEY == y.C_NATIONKEY)(x => x.S_NATIONKEY[Int])(x => x.C_NATIONKEY)
        val jo4 = HashJoinOp(jo3, scanOrders)((x, y) => x.C_CUSTKEY == y.O_CUSTKEY)(x => x.C_CUSTKEY[Int])(x => x.O_CUSTKEY)
        val jo5 = SelectOp(HashJoinOp(jo4, scanLineitem)((x, y) => x.O_ORDERKEY == y.L_ORDERKEY)(x => x.O_ORDERKEY[Int])(x => x.L_ORDERKEY))(x => x.S_SUPPKEY == x.L_SUPPKEY)
        val aggOp = AggOp(jo5, 1)(x => x.N_NAME[Array[Byte]])(
          (t, currAgg) => { currAgg + t.L_EXTENDEDPRICE[Double] * (1.0 - t.L_DISCOUNT[Double]) })
        val sortOp = SortOp(aggOp)((x, y) => {
          if (x.aggs(0) < y.aggs(0)) 1
          else if (x.aggs(0) > y.aggs(0)) -1
          else 0
        })
        val po = PrintOp(sortOp)(kv => printf("%s|%.4f\n", kv.key.string, kv.aggs(0)))
        po.open
        po.next
        printf("(%d rows)\n", po.numRows)
        ()
      })
    }
  }
}

trait Q6 extends GenericQuery {
  def Q6(numRuns: Int) {
    val lineitemTable = loadLineitem()
    for (i <- 0 until numRuns) {
      runQuery({
        val constantDate1: Long = parseDate("1996-01-01")
        val constantDate2: Long = parseDate("1997-01-01")
        val lineitemScan = SelectOp(ScanOp(lineitemTable))(x => x.L_SHIPDATE >= constantDate1 && x.L_SHIPDATE < constantDate2 && x.L_DISCOUNT >= 0.08 && x.L_DISCOUNT <= 0.1 && x.L_QUANTITY < 24)
        val aggOp = AggOp(lineitemScan, 1)(x => "Total")((t, currAgg) => { (t.L_EXTENDEDPRICE * t.L_DISCOUNT) + currAgg })
        val po = PrintOp(aggOp)(kv => printf("%.4f\n", kv.aggs(0)))
        po.open
        po.next
        printf("(%d rows)\n", po.numRows)
        ()
      })
    }
  }
}
trait Q7 extends GenericQuery {
  case class Q7GRPRecord(
    val SUPP_NATION: Array[Byte],
    val CUST_NATION: Array[Byte],
    val L_YEAR: Long) extends CaseClassRecord {
    def getField(key: String): Option[Any] = key match {
      case "SUPP_NATION" => Some(SUPP_NATION)
      case "CUST_NATION" => Some(CUST_NATION)
      case "L_YEAR"      => Some(L_YEAR)
      case _             => None
    }
  }
  def newQ7GRPRecord(SUPP_NATION: Array[Byte], CUST_NATION: Array[Byte], L_YEAR: Long) = {
    new Q7GRPRecord(SUPP_NATION, CUST_NATION, L_YEAR)
  }

  def Q7(numRuns: Int) {
    val nationTable = loadNation()
    val ordersTable = loadOrders()
    val lineitemTable = loadLineitem()
    val customerTable = loadCustomer()
    val supplierTable = loadSupplier()
    for (i <- 0 until numRuns) {
      runQuery({
        val usa = parseString("UNITED STATES")
        val indonesia = parseString("INDONESIA")
        val scanNation1 = ScanOp(nationTable)
        val scanNation2 = ScanOp(nationTable)
        val jo1 = NestedLoopsJoinOp(scanNation1, scanNation2, "N1_", "N2_")((x, y) => ((x.N_NAME === usa && y.N_NAME === indonesia) || (x.N_NAME === indonesia && y.N_NAME === usa)))
        val scanSupplier = ScanOp(supplierTable)
        val jo2 = HashJoinOp(jo1, scanSupplier)((x, y) => x.N1_N_NATIONKEY == y.S_NATIONKEY)(x => x.N1_N_NATIONKEY[Int])(x => x.S_NATIONKEY)
        val scanLineitem = SelectOp(ScanOp(lineitemTable))(x => x.L_SHIPDATE >= parseDate("1995-01-01") && x.L_SHIPDATE <= parseDate("1996-12-31"))
        val jo3 = HashJoinOp(jo2, scanLineitem)((x, y) => x.S_SUPPKEY == y.L_SUPPKEY)(x => x.S_SUPPKEY[Int])(x => x.L_SUPPKEY)
        val scanOrders = ScanOp(ordersTable)
        val jo4 = HashJoinOp(jo3, scanOrders)((x, y) => x.L_ORDERKEY == y.O_ORDERKEY)(x => x.L_ORDERKEY[Int])(x => x.O_ORDERKEY)
        val scanCustomer = ScanOp(customerTable)
        val jo5 = HashJoinOp(scanCustomer, jo4)((x, y) => x.C_CUSTKEY == y.O_CUSTKEY[Int] && x.C_NATIONKEY == y.N2_N_NATIONKEY[Int])(x => x.C_CUSTKEY)(x => x.O_CUSTKEY[Int])
        val gb = AggOp(jo5, 1)(x => newQ7GRPRecord(
          x.N1_N_NAME[Array[Byte]],
          x.N2_N_NAME[Array[Byte]],
          new java.util.Date(x.L_SHIPDATE[Long]).getYear + 1900))((t, currAgg) => { currAgg + (t.L_EXTENDEDPRICE[Double] * (1.0 - t.L_DISCOUNT[Double])) })
        val so = SortOp(gb)((kv1, kv2) => {
          val k1 = kv1.key; val k2 = kv2.key
          val r1 = k1.SUPP_NATION diff k2.SUPP_NATION
          if (r1 != 0) r1
          else {
            val r2 = k1.CUST_NATION diff k2.CUST_NATION
            if (r2 != 0) r2
            else {
              if (k1.L_YEAR < k2.L_YEAR) -1
              else if (k1.L_YEAR > k2.L_YEAR) 1
              else 0
            }
          }
        })
        val po = PrintOp(so)(kv => printf("%s|%s|%d|%.4f\n", kv.key.SUPP_NATION.string, kv.key.CUST_NATION.string, kv.key.L_YEAR, kv.aggs(0)))
        po.open
        po.next
        printf("(%d rows)\n", po.numRows)
        ()
      })
    }
  }
}

trait Q8 extends GenericQuery {
  def Q8(numRuns: Int) {
    val nationTable = loadNation()
    val regionTable = loadRegion()
    val partTable = loadPart()
    val ordersTable = loadOrders()
    val lineitemTable = loadLineitem()
    val customerTable = loadCustomer()
    val supplierTable = loadSupplier()
    for (i <- 0 until numRuns) {
      runQuery({
        val constantDate1 = parseDate("1995-01-01")
        val constantDate2 = parseDate("1996-12-31")
        val asia = parseString("ASIA")
        val indonesia = parseString("INDONESIA")
        val medAnonNick = parseString("MEDIUM ANODIZED NICKEL")
        val scanLineitem = ScanOp(lineitemTable)
        val scanPart = SelectOp(ScanOp(partTable))(x => x.P_TYPE === medAnonNick)
        val jo1 = HashJoinOp(scanPart, scanLineitem)((x, y) => x.P_PARTKEY == y.L_PARTKEY)(x => x.P_PARTKEY)(x => x.L_PARTKEY)
        val scanOrders = SelectOp(ScanOp(ordersTable))(x => x.O_ORDERDATE >= constantDate1 && x.O_ORDERDATE <= constantDate2)
        val jo2 = HashJoinOp(jo1, scanOrders)((x, y) => x.L_ORDERKEY == y.O_ORDERKEY)(x => x.L_ORDERKEY[Int])(x => x.O_ORDERKEY)
        val scanCustomer = ScanOp(customerTable)
        val jo3 = HashJoinOp(jo2, scanCustomer)((x, y) => x.O_CUSTKEY == y.C_CUSTKEY)(x => x.O_CUSTKEY[Int])(x => x.C_CUSTKEY)
        val scanNation1 = ScanOp(nationTable)
        val jo4 = HashJoinOp(scanNation1, jo3)((x, y) => x.N_NATIONKEY == y.C_NATIONKEY[Int])(x => x.N_NATIONKEY)(x => x.C_NATIONKEY[Int])
        val scanRegion = SelectOp(ScanOp(regionTable))(x => x.R_NAME === asia)
        val jo5 = HashJoinOp(scanRegion, jo4)((x, y) => x.R_REGIONKEY == y.N_REGIONKEY[Int])(x => x.R_REGIONKEY)(x => x.N_REGIONKEY[Int])
        val scanSupplier = ScanOp(supplierTable)
        val jo6 = HashJoinOp(scanSupplier, jo5)((x, y) => x.S_SUPPKEY == y.L_SUPPKEY[Int])(x => x.S_SUPPKEY)(x => x.L_SUPPKEY[Int])
        val scanNation2 = ScanOp(nationTable)
        val jo7 = HashJoinOp(scanNation2, jo6, "REC1_", "REC2_")((x, y) => x.N_NATIONKEY == y.S_NATIONKEY[Int])(x => x.N_NATIONKEY)(x => x.S_NATIONKEY[Int])
        val aggOp = AggOp(jo7, 3)(x => new java.util.Date(x.REC2_O_ORDERDATE[Long]).getYear + 1900)(
          (t, currAgg) => { currAgg + (t.REC2_L_EXTENDEDPRICE[Double] * (1.0 - t.REC2_L_DISCOUNT[Double])) },
          (t, currAgg) => {
            if (t.REC1_N_NAME[Array[Byte]] === indonesia) currAgg + (t.REC2_L_EXTENDEDPRICE[Double] * (1.0 - t.REC2_L_DISCOUNT[Double]))
            else currAgg
          })
        val mapOp = MapOp(aggOp)(x => x.aggs(2) = x.aggs(1) / x.aggs(0))
        val sortOp = SortOp(mapOp)((x, y) => {
          if (x.key < y.key) -1
          else if (x.key > y.key) 1
          else 0
        })
        val po = PrintOp(sortOp)(kv => printf("%d|%.5f\n", kv.key, kv.aggs(2)))
        po.open
        po.next
        printf("(%d rows)\n", po.numRows)
        ()
      })
    }
  }
}

trait Q9 extends GenericQuery {
  case class Q9GRPRecord(
    val NATION: Array[Byte],
    val O_YEAR: Long) extends CaseClassRecord {
    def getField(key: String): Option[Any] = key match {
      case "NATION" => Some(NATION)
      case "O_YEAR" => Some(O_YEAR)
      case _        => None
    }
  }
  def newQ9GRPRecord(NATION: Array[Byte], YEAR: Long) = {
    new Q9GRPRecord(NATION, YEAR)
  }

  def Q9(numRuns: Int) {
    val partTable = loadPart()
    val nationTable = loadNation()
    val ordersTable = loadOrders()
    val partsuppTable = loadPartsupp()
    val supplierTable = loadSupplier()
    val lineitemTable = loadLineitem()
    for (i <- 0 until numRuns) {
      runQuery({
        val ghost = parseString("ghost")
        val soNation = ScanOp(nationTable)
        val soSupplier = ScanOp(supplierTable)
        val soLineitem = ScanOp(lineitemTable)
        val soPart = SelectOp(ScanOp(partTable))(x => x.P_NAME.containsSlice(ghost))
        val soPartsupp = ScanOp(partsuppTable)
        val soOrders = ScanOp(ordersTable)
        val hj1 = HashJoinOp(soNation, soSupplier)((x, y) => x.N_NATIONKEY == y.S_NATIONKEY)(x => x.N_NATIONKEY)(x => x.S_NATIONKEY)
        val hj2 = HashJoinOp(hj1, soLineitem)((x, y) => x.S_SUPPKEY[Int] == y.L_SUPPKEY)(x => x.S_SUPPKEY[Int])(x => x.L_SUPPKEY)
        val hj3 = HashJoinOp(soPart, hj2)((x, y) => x.P_PARTKEY == y.L_PARTKEY[Int])(x => x.P_PARTKEY)(x => x.L_PARTKEY[Int])
        val hj4 = HashJoinOp(soPartsupp, hj3)((x, y) => x.PS_PARTKEY == y.L_PARTKEY[Int] && x.PS_SUPPKEY == y.L_SUPPKEY[Int])(x => x.PS_PARTKEY)(x => x.L_PARTKEY[Int])
        val hj5 = HashJoinOp(hj4, soOrders)((x, y) => x.L_ORDERKEY == y.O_ORDERKEY)(x => x.L_ORDERKEY[Int])(x => x.O_ORDERKEY)
        val aggOp = AggOp(hj5, 1)(x => new Q9GRPRecord(x.N_NAME[Array[Byte]], new java.util.Date(x.O_ORDERDATE[Long]).getYear + 1900))(
          (t, currAgg) => { currAgg + ((t.L_EXTENDEDPRICE[Double] * (1.0 - t.L_DISCOUNT[Double]))) - ((1.0 * t.PS_SUPPLYCOST[Double]) * t.L_QUANTITY[Double]) })
        val sortOp = SortOp(aggOp)((kv1, kv2) => {
          val k1 = kv1.key; val k2 = kv2.key
          val r = k1.NATION diff k2.NATION
          if (r == 0) {
            if (k1.O_YEAR < k2.O_YEAR) 1
            else if (k1.O_YEAR > k2.O_YEAR) -1
            else 0
          } else r
        })
        val po = PrintOp(sortOp)(kv => printf("%s|%d|%.4f\n", kv.key.NATION.string, kv.key.O_YEAR, kv.aggs(0)))
        po.open
        po.next
        printf("(%d rows)\n", po.numRows)
        ()
      })
    }
  }
}

