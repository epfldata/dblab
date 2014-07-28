package ch.epfl.data
package legobase

import queryengine._
import queryengine.volcano._

trait Queries extends Q1 with Q2 with Q6
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
