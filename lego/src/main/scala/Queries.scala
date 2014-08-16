package ch.epfl.data
package legobase

import queryengine._
import queryengine.volcano._
import ch.epfl.data.pardis.shallow.{ CaseClassRecord }
import ch.epfl.data.pardis.shallow.{ AbstractRecord, DynamicCompositeRecord }

trait Queries extends Q1 with Q2 with Q3 with Q4 with Q5 with Q6 with Q7 with Q8 with Q9 with Q10 with Q11 with Q12 with Q13 with Q14 with Q15 with Q16 with Q17 with Q18 with Q19 with Q20 with Q21 with Q22

trait GenericQuery extends ScalaImpl with storagemanager.Loader {
  var profile = true

  def runQuery[T](query: => T): T = {
    if (profile) {
      utils.Utilities.time(query, "finish")
    } else {
      query
    }
  }
  def dateToString(dt: java.util.Date): String = {
    (dt.getYear + 1900) + "" +
      {
        val m = dt.getMonth + 1
        if (m < 10) "0" + m else m
      } + "" +
      {
        val d = dt.getDate
        if (d < 10) "0" + d else d
      }
  }
}

trait Q1 extends GenericQuery {
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
        val po = PrintOp(sortOp)(kv => printf("%c|%c|%.2f|%.2f|%.2f|%.2f|%.2f|%.2f|%.2f|%.0f\n",
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
    import queryengine._
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
        val regionScan = SelectOp(ScanOp(regionTable))(x => x.R_NAME === africa) // for comparing equality of LBString we should use === instead of ==
        val jo4 = HashJoinOp(regionScan, jo3)((x, y) => x.R_REGIONKEY == y.N_REGIONKEY[Int])(x => x.R_REGIONKEY)(x => x.N_REGIONKEY[Int])
        val wo = WindowOp(jo4)(x => x.P_PARTKEY[Int])(x => x.minBy(y => y.PS_SUPPLYCOST[Double]))
        val so = SortOp(wo)((x, y) => {
          if (x.wnd.S_ACCTBAL[Double] < y.wnd.S_ACCTBAL[Double]) 1
          else if (x.wnd.S_ACCTBAL[Double] > y.wnd.S_ACCTBAL[Double]) -1
          else {
            var res = x.wnd.N_NAME[LBString] compare y.wnd.N_NAME[LBString]
            if (res == 0) {
              res = x.wnd.S_NAME[LBString] compare y.wnd.S_NAME[LBString]
              if (res == 0) res = x.wnd.P_PARTKEY[Int] - y.wnd.P_PARTKEY[Int]
            }
            res
          }
        })
        var j = 0
        val po = PrintOp(so)(e => {
          val kv = e.wnd
          printf("%.2f|%s|%s|%d|%s|%s|%s|%s\n", kv.S_ACCTBAL, (kv.S_NAME[LBString]).string, (kv.N_NAME[LBString]).string, kv.P_PARTKEY, (kv.P_MFGR[LBString]).string, (kv.S_ADDRESS[LBString]).string, (kv.S_PHONE[LBString]).string, (kv.S_COMMENT[LBString]).string)
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
        val aggOp = AggOp(jo2, 1)(x => new Q3GRPRecord(x.L_ORDERKEY[Int], x.O_ORDERDATE[Long], x.O_SHIPPRIORITY[Int]))((t, currAgg) => { currAgg + (t.L_EXTENDEDPRICE[Double] * (1.0 - t.L_DISCOUNT[Double])) })
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
          printf("%d|%.4f|%s|%d\n", kv.key.L_ORDERKEY, kv.aggs(0), dateToString(new java.util.Date(kv.key.O_ORDERDATE)), kv.key.O_SHIPPRIORITY)
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
        val aggOp = AggOp(jo5, 1)(x => x.N_NAME[LBString])(
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
        val gb = AggOp(jo5, 1)(x => new Q7GRPRecord(
          x.N1_N_NAME[LBString],
          x.N2_N_NAME[LBString],
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
            if (t.REC1_N_NAME[LBString] === indonesia) currAgg + (t.REC2_L_EXTENDEDPRICE[Double] * (1.0 - t.REC2_L_DISCOUNT[Double]))
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
    val NATION: LBString,
    val O_YEAR: Long) extends CaseClassRecord {
    def getField(key: String): Option[Any] = key match {
      case "NATION" => Some(NATION)
      case "O_YEAR" => Some(O_YEAR)
      case _        => None
    }
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
        val aggOp = AggOp(hj5, 1)(x => new Q9GRPRecord(x.N_NAME[LBString], new java.util.Date(x.O_ORDERDATE[Long]).getYear + 1900))(
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

trait Q10 extends GenericQuery {
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

  def Q10(numRuns: Int) {
    val lineitemTable = loadLineitem()
    val nationTable = loadNation()
    val customerTable = loadCustomer()
    val ordersTable = loadOrders()
    for (i <- 0 until numRuns) {
      runQuery({
        val constantDate1 = parseDate("1994-11-01")
        val constantDate2 = parseDate("1995-02-01")
        val so1 = SelectOp(ScanOp(ordersTable))(x => x.O_ORDERDATE >= constantDate1 && x.O_ORDERDATE < constantDate2)
        val so2 = ScanOp(customerTable)
        val hj1 = HashJoinOp(so1, so2)((x, y) => x.O_CUSTKEY == y.C_CUSTKEY)(x => x.O_CUSTKEY)(x => x.C_CUSTKEY)
        val so3 = ScanOp(nationTable)
        val hj2 = HashJoinOp(so3, hj1)((x, y) => x.N_NATIONKEY == y.C_NATIONKEY[Int])(x => x.N_NATIONKEY)(x => x.C_NATIONKEY[Int])
        val so4 = SelectOp(ScanOp(lineitemTable))(x => x.L_RETURNFLAG == 'R')
        val hj3 = HashJoinOp(hj2, so4)((x, y) => x.O_ORDERKEY[Int] == y.L_ORDERKEY)(x => x.O_ORDERKEY[Int])(x => x.L_ORDERKEY)
        val aggOp = AggOp(hj3, 1)(x => new Q10GRPRecord(x.C_CUSTKEY[Int],
          x.C_NAME[LBString], x.C_ACCTBAL[Double],
          x.C_PHONE[LBString], x.N_NAME[LBString],
          x.C_ADDRESS[LBString], x.C_COMMENT[LBString]))((t, currAgg) => { currAgg + (t.L_EXTENDEDPRICE[Double] * (1.0 - t.L_DISCOUNT[Double])).asInstanceOf[Double] })
        val sortOp = SortOp(aggOp)((kv1, kv2) => {
          val k1 = kv1.aggs(0); val k2 = kv2.aggs(0)
          if (k1 < k2) 1
          else if (k1 > k2) -1
          else 0
        })
        var j = 0
        val po = PrintOp(sortOp)(kv => {
          printf("%d|%s|%.4f|%.2f|%s|%s|%s|%s\n", kv.key.C_CUSTKEY, kv.key.C_NAME.string, kv.aggs(0),
            kv.key.C_ACCTBAL, kv.key.N_NAME.string, kv.key.C_ADDRESS.string, kv.key.C_PHONE.string,
            kv.key.C_COMMENT.string)
          j += 1
        }, () => j < 20)
        po.open
        po.next
        printf("(%d rows)\n", po.numRows)
        ()
      })
    }
  }
}

trait Q11 extends GenericQuery {
  def Q11(numRuns: Int) {
    val partsuppTable = loadPartsupp()
    val supplierTable = loadSupplier()
    val nationTable = loadNation()
    for (i <- 0 until numRuns) {
      runQuery({
        val uk = parseString("UNITED KINGDOM")
        val scanSupplier = ScanOp(supplierTable)
        val scanNation = SelectOp(ScanOp(nationTable))(x => x.N_NAME === uk)
        val jo1 = HashJoinOp(scanNation, scanSupplier)((x, y) => x.N_NATIONKEY == y.S_NATIONKEY)(x => x.N_NATIONKEY)(x => x.S_NATIONKEY)
        val scanPartsupp = ScanOp(partsuppTable)
        val jo2 = HashJoinOp(jo1, scanPartsupp)((x, y) => x.S_SUPPKEY == y.PS_SUPPKEY)(x => x.S_SUPPKEY[Int])(x => x.PS_SUPPKEY)
        val wo = WindowOp(jo2)(x => x.PS_PARTKEY[Int])(x => {
          x.foldLeft(0.0) { (cnt, e) => cnt + (e.PS_SUPPLYCOST[Double] * e.PS_AVAILQTY[Int]) }
        })
        wo.open
        val vo = ViewOp(wo)
        // Calculate total sum
        val aggOp = AggOp(vo, 1)(x => "Total")((t, currAgg) => currAgg + t.wnd)
        val total = SubquerySingleResult(aggOp).getResult.aggs(0) * 0.0001
        // Calculate final result
        vo.reset
        val so = SelectOp(vo)(x => x.wnd > total)
        val sortOp = SortOp(so)((x, y) => {
          if (x.wnd > y.wnd) -1
          else if (x.wnd < y.wnd) 1
          else 0
        })
        val po = PrintOp(sortOp)(kv => printf("%d|%.2f\n", kv.key, kv.wnd))
        po.open
        po.next
        printf("(%d rows)\n", po.numRows)
        ()
      })
    }
  }
}

trait Q12 extends GenericQuery {
  def Q12(numRuns: Int) {
    val lineitemTable = loadLineitem()
    val ordersTable = loadOrders()
    for (i <- 0 until numRuns) {
      runQuery({
        val mail = parseString("MAIL")
        val ship = parseString("SHIP")
        val constantDate = parseDate("1995-01-01")
        val constantDate2 = parseDate("1994-01-01")
        val so1 = ScanOp(ordersTable)
        val so2 = SelectOp(ScanOp(lineitemTable))(x =>
          x.L_RECEIPTDATE < constantDate && x.L_COMMITDATE < constantDate && x.L_SHIPDATE < constantDate && x.L_SHIPDATE < x.L_COMMITDATE && x.L_COMMITDATE < x.L_RECEIPTDATE && x.L_RECEIPTDATE >= constantDate2 && (x.L_SHIPMODE === mail || x.L_SHIPMODE === ship))
        val jo = HashJoinOp(so1, so2)((x, y) => x.O_ORDERKEY == y.L_ORDERKEY)(x => x.O_ORDERKEY)(x => x.L_ORDERKEY)
        val URGENT = parseString("1-URGENT")
        val HIGH = parseString("2-HIGH")
        val aggOp = AggOp(jo, 2)(x => x.L_SHIPMODE[LBString])(
          (t, currAgg) => { if (t.O_ORDERPRIORITY[LBString] === URGENT || t.O_ORDERPRIORITY[LBString] === HIGH) currAgg + 1 else currAgg },

          (t, currAgg) => { if (t.O_ORDERPRIORITY[LBString] =!= URGENT && t.O_ORDERPRIORITY[LBString] =!= HIGH) currAgg + 1 else currAgg })
        val sortOp = SortOp(aggOp)((x, y) => x.key diff y.key)
        val po = PrintOp(sortOp)(kv => printf("%s|%.0f|%.0f\n", kv.key.string, kv.aggs(0), kv.aggs(1)))
        po.open
        po.next
        printf("(%d rows)\n", po.numRows)
        ()
      })
    }
  }
}

trait Q13 extends GenericQuery {
  def Q13(numRuns: Int) {
    val customerTable = loadCustomer()
    val ordersTable = loadOrders()
    for (i <- 0 until numRuns) {
      runQuery({
        val unusual = parseString("unusual")
        val packages = parseString("packages")
        val scanCustomer = ScanOp(customerTable)
        val scanOrders = SelectOp(ScanOp(ordersTable))(x => {
          val idxu = x.O_COMMENT.indexOfSlice(unusual, 0)
          val idxp = x.O_COMMENT.indexOfSlice(packages, idxu)
          !(idxu != -1 && idxp != -1)
        })
        val jo = LeftOuterJoinOp(scanCustomer, scanOrders)((x, y) => x.C_CUSTKEY == y.O_CUSTKEY)(x => x.C_CUSTKEY)(x => x.O_CUSTKEY)
        val aggOp1 = AggOp(jo, 1)(x => x.C_CUSTKEY[Int])(
          (t, currAgg) => { if (t.O_ORDERKEY != 0.0) currAgg + 1 else currAgg })
        val aggOp2 = AggOp(aggOp1, 1)(x => x.aggs(0))(
          (t, currAgg) => { currAgg + 1 })
        val sortOp = SortOp(aggOp2)((x, y) => {
          if (x.aggs(0) < y.aggs(0)) 1
          else if (x.aggs(0) > y.aggs(0)) -1
          else {
            if (x.key < y.key) 1
            else if (x.key > y.key) -1
            else 0
          }
        })
        val po = PrintOp(sortOp)(kv => printf("%.0f|%.0f\n", kv.key, kv.aggs(0)))
        po.open
        po.next
        printf("(%d rows)\n", po.numRows)
        ()
      })
    }
  }
}

trait Q14 extends GenericQuery {
  def Q14(numRuns: Int) {
    val lineitemTable = loadLineitem()
    val partTable = loadPart()
    for (i <- 0 until numRuns) {
      runQuery({
        val promo = parseString("PROMO")
        val constantDate = parseDate("1994-04-01")
        val constantDate2 = parseDate("1994-03-01")
        val so1 = ScanOp(partTable)
        val so2 = SelectOp(ScanOp(lineitemTable))(x => x.L_SHIPDATE >= constantDate2 && x.L_SHIPDATE < constantDate)
        val jo = HashJoinOp(so1, so2)((x, y) => x.P_PARTKEY == y.L_PARTKEY)(x => x.P_PARTKEY)(x => x.L_PARTKEY)
        val aggOp = AggOp(jo, 3)(x => "Total")(
          (t, currAgg) => {
            if (t.P_TYPE[LBString] startsWith promo)
              currAgg + (t.L_EXTENDEDPRICE[Double] * (1.0 - t.L_DISCOUNT[Double]))
            else currAgg
          },
          (t, currAgg) => { currAgg + (t.L_EXTENDEDPRICE[Double] * (1.0 - t.L_DISCOUNT[Double])) })
        val mapOp = MapOp(aggOp)(kv => kv.aggs(2) = (kv.aggs(0) * 100) / kv.aggs(1))
        val po = PrintOp(mapOp)(kv => printf("%.4f\n", kv.aggs(2)))
        po.open
        po.next
        printf("(%d rows)\n", po.numRows)
        ()
      })
    }
  }
}

trait Q15 extends GenericQuery {
  def Q15(numRuns: Int) {
    val lineitemTable = loadLineitem()
    val supplierTable = loadSupplier()
    for (i <- 0 until numRuns) {
      runQuery({
        val constantDate = parseDate("1993-09-01")
        val constantDate2 = parseDate("1993-12-01")
        val scanLineitem = SelectOp(ScanOp(lineitemTable))(x => x.L_SHIPDATE >= constantDate && x.L_SHIPDATE < constantDate2)
        val aggOp1 = AggOp(scanLineitem, 1)(x => x.L_SUPPKEY)(
          (t, currAgg) => { currAgg + (t.L_EXTENDEDPRICE * (1.0 - t.L_DISCOUNT)) })
        // Create view
        aggOp1.open
        val vo = ViewOp(aggOp1)
        // Get max
        val aggOp2 = AggOp(vo, 1)(x => "MAXREVENUE")(
          (t, currAgg) => { if (currAgg < t.aggs(0)) t.aggs(0) else currAgg })
        aggOp2.open
        val maxRevenue = SubquerySingleResult(aggOp2).getResult.aggs(0)
        vo.reset
        // Calcuate result
        val scanSupplier = ScanOp(supplierTable)
        val jo = HashJoinOp(scanSupplier, vo)((x, y) => x.S_SUPPKEY == y.key && y.aggs(0) == maxRevenue)(x => x.S_SUPPKEY)(x => x.key)
        val po = PrintOp(jo)(kv => printf("%d|%s|%s|%s|%.4f\n", kv.S_SUPPKEY, kv.S_NAME[LBString].string, kv.S_ADDRESS[LBString].string, kv.S_PHONE[LBString].string, kv.getField("aggs").get.asInstanceOf[Array[Double]](0)))
        po.open
        po.next
        printf("(%d rows)\n", po.numRows)
        ()
      })
    }
  }
}

trait Q16 extends GenericQuery {
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

  def Q16(numRuns: Int) {
    val supplierTable = loadSupplier()
    val partTable = loadPart()
    val partsuppTable = loadPartsupp()
    for (i <- 0 until numRuns) {
      runQuery({
        val str1 = parseString("Customer")
        val str2 = parseString("Complaints")
        val brand21 = parseString("Brand#21")
        val promoPlated = parseString("PROMO PLATED")
        val partScan = SelectOp(ScanOp(partTable))(x => !(x.P_BRAND startsWith brand21) && !(x.P_TYPE startsWith promoPlated) &&
          (x.P_SIZE == 23 || x.P_SIZE == 3 || x.P_SIZE == 33 || x.P_SIZE == 29 ||
            x.P_SIZE == 40 || x.P_SIZE == 27 || x.P_SIZE == 22 || x.P_SIZE == 4))
        val partsuppScan = ScanOp(partsuppTable)
        val jo1 = HashJoinOp(partScan, partsuppScan)((x, y) => x.P_PARTKEY == y.PS_PARTKEY)(x => x.P_PARTKEY)(x => x.PS_PARTKEY)
        val supplierScan = SelectOp(ScanOp(supplierTable))(x => {
          val idxu = x.S_COMMENT.indexOfSlice(str1, 0)
          val idxp = x.S_COMMENT.indexOfSlice(str2, idxu)
          idxu != -1 && idxp != -1
        })
        val jo2 = HashJoinAnti(jo1, supplierScan)((x, y) => x.PS_SUPPKEY == y.S_SUPPKEY)(x => x.PS_SUPPKEY[Int])(x => x.S_SUPPKEY)
        val aggOp = AggOp(jo2, 1)(x => new Q16GRPRecord1(x.P_BRAND[LBString], x.P_TYPE[LBString],
          x.P_SIZE[Int], x.PS_SUPPKEY[Int]))((t, currAgg) => currAgg)
        val aggOp2 = AggOp(aggOp, 1)(x => new Q16GRPRecord2(x.key.P_BRAND, x.key.P_TYPE, x.key.P_SIZE))(
          (t, currAgg) => currAgg + 1)
        val sortOp = SortOp(aggOp2)((x, y) => {
          if (x.aggs(0) < y.aggs(0)) 1
          else if (x.aggs(0) > y.aggs(0)) -1
          else {
            var res = x.key.P_BRAND diff y.key.P_BRAND
            if (res == 0) {
              res = x.key.P_TYPE diff y.key.P_TYPE
              if (res == 0) res = x.key.P_SIZE - y.key.P_SIZE
            }
            res
          }
        })
        val po = PrintOp(sortOp)(x => printf("%s|%s|%d|%.0f\n", x.key.P_BRAND.string, x.key.P_TYPE.string, x.key.P_SIZE, x.aggs(0)))
        po.open
        po.next
        printf("(%d rows)\n", po.numRows)
        ()
      })
    }
  }
}

trait Q17 extends GenericQuery {
  def Q17(numRuns: Int) {
    val lineitemTable = loadLineitem()
    val partTable = loadPart()
    for (i <- 0 until numRuns) {
      runQuery({
        val medbag = parseString("MED BAG")
        val brand15 = parseString("Brand#15")
        val scanLineitem = ScanOp(lineitemTable)
        val scanPart = SelectOp(ScanOp(partTable))(x => x.P_CONTAINER === medbag && x.P_BRAND === brand15)
        val jo = HashJoinOp(scanPart, scanLineitem)((x, y) => x.P_PARTKEY == y.L_PARTKEY)(x => x.P_PARTKEY)(x => x.L_PARTKEY)
        val wo = WindowOp(jo)(x => x.L_PARTKEY[Int])(x => {
          val sum = x.foldLeft(0.0)((cnt, e) => cnt + e.L_QUANTITY[Double])
          val count = x.size
          val avg = 0.2 * (sum / count)
          x.foldLeft(0.0)((cnt, e) => {
            if (e.L_QUANTITY[Double] < avg) cnt + e.L_EXTENDEDPRICE[Double]
            else cnt
          }) / 7.0
        })
        val aggOp = AggOp(wo, 1)(x => "Total")((t, currAgg) => currAgg + t.wnd)
        val po = PrintOp(aggOp)(kv => printf("%.6f\n", kv.aggs(0)))
        po.open
        po.next
        printf("(%d rows)\n", po.numRows)
        ()
      })
    }
  }
}

// Danger, Will Robinson!: Query takes a long time to complete in Scala (but we 
// knew that already!)
trait Q18 extends GenericQuery {
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
  def Q18(numRuns: Int) {
    val lineitemTable = loadLineitem()
    val ordersTable = loadOrders()
    val customerTable = loadCustomer()
    for (i <- 0 until numRuns) {
      runQuery({
        val scanOrders = ScanOp(ordersTable)
        val scanCustomer = ScanOp(customerTable)
        // Group aggregation on Lineitem
        val scanLineitem1 = ScanOp(lineitemTable)
        val aggOp1 = SelectOp(AggOp(scanLineitem1, 1)(x => x.L_ORDERKEY)(
          (t, currAgg) => { currAgg + t.L_QUANTITY }))(x => x.aggs(0) > 300)
        // Hash Join with orders
        val jo1 = HashJoinOp(aggOp1, scanOrders)((x, y) => y.O_ORDERKEY == x.key)(x => x.key)(x => x.O_ORDERKEY)
        val jo2 = HashJoinOp(jo1, scanCustomer)((x, y) => x.O_CUSTKEY == y.C_CUSTKEY)(x => x.O_CUSTKEY[Int])(x => x.C_CUSTKEY)
        val aggOp2 = AggOp(jo2, 1)(x => new Q18GRPRecord(x.C_NAME[LBString], x.C_CUSTKEY[Int],
          x.O_ORDERKEY[Int], x.O_ORDERDATE[Long], x.O_TOTALPRICE[Double]))(
          // Why doesn't a simple t.aggs work here?
          (t, currAgg) => { currAgg + (t.getField("aggs").get.asInstanceOf[Array[Double]])(0) } // aggs(0) => L_QUANTITY"
          )
        val sortOp = SortOp(aggOp2)((kv1, kv2) => {
          val k1 = kv1.key.O_TOTALPRICE; val k2 = kv2.key.O_TOTALPRICE
          if (k1 < k2) 1
          else if (k1 > k2) -1
          else {
            val d1 = kv1.key.O_ORDERDATE; val d2 = kv2.key.O_ORDERDATE;
            if (d1 < d2) -1
            else if (d1 > d2) 1
            else 0
          }
        })
        var j = 0
        val po = PrintOp(sortOp)(kv => {
          printf("%s|%d|%d|%s|%.2f|%.2f\n", kv.key.C_NAME.string, kv.key.C_CUSTKEY, kv.key.O_ORDERKEY, dateToString(new java.util.Date(kv.key.O_ORDERDATE)), kv.key.O_TOTALPRICE, kv.aggs(0))
          j += 1
        }, () => j < 100)
        po.open
        po.next
        printf("(%d rows)\n", po.numRows)
        ()
      })
    }
  }
}

trait Q19 extends GenericQuery {
  def Q19(numRuns: Int) {
    val lineitemTable = loadLineitem()
    val partTable = loadPart()
    for (i <- 0 until numRuns) {
      runQuery({
        val Brand31 = parseString("Brand#31")
        val Brand43 = parseString("Brand#43")
        val SMBOX = parseString("SM BOX")
        val SMCASE = parseString("SM CASE")
        val SMPACK = parseString("SM PACK")
        val SMPKG = parseString("SM PKG")
        val MEDBAG = parseString("MED BAG")
        val MEDBOX = parseString("MED BOX")
        val MEDPACK = parseString("MED PACK")
        val MEDPKG = parseString("MED PKG")
        val LGBOX = parseString("LG BOX")
        val LGCASE = parseString("LG CASE")
        val LGPACK = parseString("LG PACK")
        val LGPKG = parseString("LG PKG")
        val DELIVERINPERSON = parseString("DELIVER IN PERSON")
        val AIR = parseString("AIR")
        val AIRREG = parseString("AIRREG")

        val so1 = SelectOp(ScanOp(partTable))(x => x.P_SIZE >= 1 &&
          (x.P_SIZE <= 5 && x.P_BRAND === Brand31 && (x.P_CONTAINER === SMBOX || x.P_CONTAINER === SMCASE ||
            x.P_CONTAINER === SMPACK || x.P_CONTAINER === SMPKG)) ||
            (x.P_SIZE <= 10 && x.P_BRAND === Brand43 && (x.P_CONTAINER === MEDBAG || x.P_CONTAINER === MEDBOX ||
              x.P_CONTAINER === MEDPACK || x.P_CONTAINER === MEDPKG)) ||
              (x.P_SIZE <= 15 && x.P_BRAND === Brand43 && (x.P_CONTAINER === LGBOX || x.P_CONTAINER === LGCASE ||
                x.P_CONTAINER === LGPACK || x.P_CONTAINER === LGPKG)))
        val so2 = SelectOp(ScanOp(lineitemTable))(x =>
          ((x.L_QUANTITY <= 36 && x.L_QUANTITY >= 26) || (x.L_QUANTITY <= 25 && x.L_QUANTITY >= 15) ||
            (x.L_QUANTITY <= 14 && x.L_QUANTITY >= 4)) && x.L_SHIPINSTRUCT === DELIVERINPERSON &&
            (x.L_SHIPMODE === AIR || x.L_SHIPMODE === AIRREG))
        val jo = SelectOp(HashJoinOp(so1, so2)((x, y) => x.P_PARTKEY == y.L_PARTKEY)(x => x.P_PARTKEY)(x => x.L_PARTKEY))(
          x => x.P_BRAND[LBString] === Brand31 &&
            (x.P_CONTAINER[LBString] === SMBOX || x.P_CONTAINER[LBString] === SMCASE || x.P_CONTAINER[LBString] === SMPACK || x.P_CONTAINER[LBString] === SMPKG) &&
            x.L_QUANTITY[Double] >= 4 && x.L_QUANTITY[Double] <= 14 && x.P_SIZE[Int] <= 5 || x.P_BRAND[LBString] === Brand43 &&
            (x.P_CONTAINER[LBString] === MEDBAG || x.P_CONTAINER[LBString] === MEDBOX || x.P_CONTAINER[LBString] === MEDPACK || x.P_CONTAINER[LBString] === MEDPKG) &&
            x.L_QUANTITY[Double] >= 15 && x.L_QUANTITY[Double] <= 25 && x.P_SIZE[Int] <= 10 || x.P_BRAND[LBString] === Brand43 &&
            (x.P_CONTAINER[LBString] === LGBOX || x.P_CONTAINER[LBString] === LGCASE || x.P_CONTAINER[LBString] === LGPACK || x.P_CONTAINER[LBString] === LGPKG) &&
            x.L_QUANTITY[Double] >= 26 && x.L_QUANTITY[Double] <= 36 && x.P_SIZE[Int] <= 15)
        val aggOp = AggOp(jo, 1)(x => "Total")(
          (t, currAgg) => { currAgg + (t.L_EXTENDEDPRICE[Double] * (1.0 - t.L_DISCOUNT[Double])) })
        val po = PrintOp(aggOp)(kv => printf("%.4f\n", kv.aggs(0)))
        po.open
        po.next
        printf("(%d rows)\n", po.numRows)
        ()
      })
    }
  }
}

trait Q20 extends GenericQuery {
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

  def Q20(numRuns: Int) {
    val partTable = loadPart()
    val nationTable = loadNation()
    val supplierTable = loadSupplier()
    val partsuppTable = loadPartsupp()
    val lineitemTable = loadLineitem()
    for (i <- 0 until numRuns) {
      runQuery({
        val constantDate1 = parseDate("1996-01-01")
        val constantDate2 = parseDate("1997-01-01")
        val jordan = parseString("JORDAN")
        val azure = parseString("azure")
        val scanPart = SelectOp(ScanOp(partTable))(x => x.P_NAME startsWith azure)
        val scanPartsupp = ScanOp(partsuppTable)
        val jo1 = HashJoinOp(scanPart, scanPartsupp)((x, y) => x.P_PARTKEY == y.PS_PARTKEY)(x => x.P_PARTKEY)(x => x.PS_PARTKEY)
        val scanLineitem = SelectOp(ScanOp(lineitemTable))(x => x.L_SHIPDATE >= constantDate1 && x.L_SHIPDATE < constantDate2)
        val jo2 = HashJoinOp(jo1, scanLineitem)((x, y) => x.PS_PARTKEY[Int] == y.L_PARTKEY && x.PS_SUPPKEY[Int] == y.L_SUPPKEY)(x => x.PS_PARTKEY[Int])(x => x.L_PARTKEY)
        val aggOp = AggOp(jo2, 1)(x => new Q20GRPRecord(x.PS_PARTKEY[Int], x.PS_SUPPKEY[Int], x.PS_AVAILQTY[Int]))((t, currAgg) => { currAgg + t.L_QUANTITY[Double] })
        val selOp = SelectOp(aggOp)(x => x.key.PS_AVAILQTY > 0.5 * x.aggs(0))
        val scanSupplier = ScanOp(supplierTable)
        val jo3 = HashJoinOp(aggOp, scanSupplier)((x, y) => x.key.PS_SUPPKEY == y.S_SUPPKEY)(x => x.key.PS_SUPPKEY)(x => x.S_SUPPKEY)
        val scanNation = SelectOp(ScanOp(nationTable))(x => x.N_NAME === jordan)
        val jo4 = HashJoinOp(scanNation, jo3)((x, y) => x.N_NATIONKEY == y.S_NATIONKEY[Int])(x => x.N_NATIONKEY)(x => x.S_NATIONKEY[Int])
        val sortOp = SortOp(jo4)((x, y) => {
          // TODO: Possible bug here in ArrayByteOps. Diff does not work
          (x.S_NAME[LBString] zip y.S_NAME[LBString]).foldLeft(0)((res, e) => { if (res == 0) e._1.asInstanceOf[Byte] - e._2.asInstanceOf[Byte] else res })
        })
        val po = PrintOp(sortOp)(kv => printf("%s|%s\n", kv.S_NAME[LBString].string, kv.S_ADDRESS[LBString].string))
        po.open
        po.next
        printf("(%d rows)\n", po.numRows)
        ()
      })
    }
  }
}

trait Q21 extends GenericQuery {
  def Q21(numRuns: Int) {
    val lineitemTable = loadLineitem()
    val supplierTable = loadSupplier()
    val ordersTable = loadOrders()
    val nationTable = loadNation()
    for (i <- 0 until numRuns) {
      runQuery({
        val morocco = parseString("MOROCCO")
        val lineitemScan1 = SelectOp(ScanOp(lineitemTable))(x => x.L_RECEIPTDATE > x.L_COMMITDATE)
        val lineitemScan2 = ScanOp(lineitemTable)
        val lineitemScan3 = SelectOp(ScanOp(lineitemTable))(x => x.L_RECEIPTDATE > x.L_COMMITDATE)
        val supplierScan = ScanOp(supplierTable)
        val nationScan = SelectOp(ScanOp(nationTable))(x => x.N_NAME === morocco)
        val ordersScan = SelectOp(ScanOp(ordersTable))(x => x.O_ORDERSTATUS == 'F')
        val jo1 = HashJoinOp(nationScan, supplierScan)((x, y) => x.N_NATIONKEY == y.S_NATIONKEY)(x => x.N_NATIONKEY)(x => x.S_NATIONKEY)
        val jo2 = HashJoinOp(jo1, lineitemScan1)((x, y) => x.S_SUPPKEY == y.L_SUPPKEY)(x => x.S_SUPPKEY[Int])(x => x.L_SUPPKEY)
        val jo3 = LeftHashSemiJoinOp(jo2, lineitemScan2)((x, y) => x.L_ORDERKEY == y.L_ORDERKEY && x.L_SUPPKEY != y.L_SUPPKEY)(x => x.L_ORDERKEY[Int])(x => x.L_ORDERKEY)
        val jo4 = HashJoinAnti(jo3, lineitemScan3)((x, y) => x.L_ORDERKEY == y.L_ORDERKEY && x.L_SUPPKEY != y.L_SUPPKEY)(x => x.L_ORDERKEY[Int])(x => x.L_ORDERKEY)
        val jo5 = HashJoinOp(ordersScan, jo4)((x, y) => x.O_ORDERKEY == y.L_ORDERKEY[Int])(x => x.O_ORDERKEY)(x => x.L_ORDERKEY[Int])
        val aggOp = AggOp(jo5, 1)(x => x.S_NAME[LBString])((t, currAgg) => { currAgg + 1 })
        val sortOp = SortOp(aggOp)((x, y) => {
          val a1 = x.aggs(0); val a2 = y.aggs(0)
          if (a1 < a2) 1
          else if (a1 > a2) -1
          else x.key diff y.key
        })
        var i = 0
        val po = PrintOp(sortOp)(kv => {
          printf("%s|%.0f\n", kv.key.string, kv.aggs(0))
          i += 1
        }, () => i < 100)
        po.open
        po.next
        printf("(%d rows)\n", po.numRows)
        ()
      })
    }
  }
}

trait Q22 extends GenericQuery {
  def Q22(numRuns: Int) {
    val customerTable = loadCustomer()
    val ordersTable = loadOrders()
    for (i <- 0 until numRuns) {
      runQuery({
        val v23 = parseString("23")
        val v29 = parseString("29")
        val v22 = parseString("22")
        val v20 = parseString("20")
        val v24 = parseString("24")
        val v26 = parseString("26")
        val v25 = parseString("25")
        // Subquery
        val customerScan1 = SelectOp(ScanOp(customerTable))(x => {
          x.C_ACCTBAL > 0.00 && (
            x.C_PHONE.startsWith(v23) || (x.C_PHONE.startsWith(v29) || (x.C_PHONE.startsWith(v22) ||
              (x.C_PHONE.startsWith(v20) || (x.C_PHONE.startsWith(v24) || (x.C_PHONE.startsWith(v26) ||
                x.C_PHONE.startsWith(v25)))))))
        })
        val aggOp1 = AggOp(customerScan1, 3)(x => "AVG_C_ACCTBAL")(
          (t, currAgg) => { t.C_ACCTBAL + currAgg },
          (t, currAgg) => { currAgg + 1 })
        val mapOp = MapOp(aggOp1)(kv => kv.aggs(2) = kv.aggs(0) / kv.aggs(1))
        mapOp.open
        val nestedAVG = SubquerySingleResult(mapOp).getResult.aggs(2)
        // External Query
        val customerScan2 = SelectOp(ScanOp(customerTable))(x => {
          x.C_ACCTBAL > nestedAVG && (
            x.C_PHONE.startsWith(v23) || (x.C_PHONE.startsWith(v29) || (x.C_PHONE.startsWith(v22) ||
              (x.C_PHONE.startsWith(v20) || (x.C_PHONE.startsWith(v24) || (x.C_PHONE.startsWith(v26) ||
                x.C_PHONE.startsWith(v25)))))))
        })
        val ordersScan = ScanOp(ordersTable)
        val jo = HashJoinAnti(customerScan2, ordersScan)((x, y) => x.C_CUSTKEY == y.O_CUSTKEY)(x => x.C_CUSTKEY)(x => x.O_CUSTKEY)
        val aggOp2 = AggOp(jo, 2)(x => x.C_PHONE.slice(0, 2))(
          (t, currAgg) => { t.C_ACCTBAL + currAgg },
          (t, currAgg) => { currAgg + 1 })
        val sortOp = SortOp(aggOp2)((x, y) => {
          // We know that the substring has only two characters
          var res = x.key(0) - y.key(0)
          if (res == 0) res = x.key(1) - y.key(1)
          res
        })
        val po = PrintOp(sortOp)(kv => printf("%s|%.0f|%.2f\n", kv.key.string, kv.aggs(1), kv.aggs(0)))
        po.open
        po.next
        printf("(%d rows)\n", po.numRows)
        ()
      })
    }
  }
}

