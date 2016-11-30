package ch.epfl.data
package dblab
package experimentation
package tpch

import queryengine._
/**
 *   In order to change from pull engine to push engine the next line should be commented and the line after
 *   should be uncommented.
 */
// import queryengine.volcano._
import queryengine.push._
import sc.pardis.annotations.{ deep, metadeep, dontLift, dontInline, onlineInliner, needs, noDeepExt, :: }
import sc.pardis.shallow.OptimalString
import TPCHLoader._
import GenericEngine._
import schema.DynamicDataRow
// import queryengine.TPCHRelations._
import storagemanager._
import queryengine.monad.Query
// import queryengine.monad.{ QueryOptimized => Query }
// import queryengine.monad.{ QueryCPS => Query }
// import queryengine.monad.{ QueryUnfold => Query }
// import queryengine.monad.{ QueryIterator => Query }
// import queryengine.monad.{ QueryStream => Query }

@metadeep(
  folder = "",
  header = "",
  component = "QueryComponent",
  thisComponent = "ch.epfl.data.dblab.deep.dsls.QueryEngineExp")
class MetaInfo

@needs[TPCHLoader :: Q1GRPRecord :: Q3GRPRecord :: Q7GRPRecord :: Q9GRPRecord :: Q10GRPRecord :: Q13IntRecord :: Q16GRPRecord1 :: Q16GRPRecord2 :: Q18GRPRecord :: Q20GRPRecord]
@deep
@onlineInliner
@noDeepExt
trait Queries

/**
 * A module containing 22 TPCH queries
 */
object Queries {

  def Q1(numRuns: Int) {
    //ProfileFunctionCalls.numberTuplesSelect = 0
    //ProfileFunctionCalls.numberTuplesFiltered = 0
    //ProfileFunctionCalls.numberTuplesScanned = 0
    val lineitemTable = loadLineitem()
    for (i <- 0 until numRuns) {
      runQuery {
        val constantDate: Int = parseDate("1998-09-02")
        val lineitemScan = new SelectOp(new ScanOp(lineitemTable))(x => x.L_SHIPDATE <= constantDate)
        val aggOp = new AggOp(lineitemScan, 9)(x => new Q1GRPRecord(
          x.L_RETURNFLAG, x.L_LINESTATUS))((t, currAgg) => { t.L_DISCOUNT + currAgg },
          (t, currAgg) => { t.L_QUANTITY + currAgg },
          (t, currAgg) => { t.L_EXTENDEDPRICE + currAgg },
          (t, currAgg) => { (t.L_EXTENDEDPRICE * (1.0 - t.L_DISCOUNT)) + currAgg },
          (t, currAgg) => { (t.L_EXTENDEDPRICE * (1.0 - t.L_DISCOUNT) * (1.0 + t.L_TAX)) + currAgg },
          (t, currAgg) => { currAgg + 1 })
        val mapOp = new MapOp(aggOp)(kv => kv.aggs(6) = kv.aggs(1) / kv.aggs(5), // AVG(L_QUANTITY)
          kv => kv.aggs(7) = kv.aggs(2) / kv.aggs(5), // AVG(L_EXTENDEDPRICE)
          kv => kv.aggs(8) = kv.aggs(0) / kv.aggs(5)) // AVG(L_DISCOUNT)
        val sortOp = new SortOp(mapOp)((kv1, kv2) => {
          var res = kv1.key.L_RETURNFLAG - kv2.key.L_RETURNFLAG
          if (res == 0)
            res = kv1.key.L_LINESTATUS - kv2.key.L_LINESTATUS
          res
        })
        val po = new PrintOp(sortOp)(kv => printf("%c|%c|%.2f|%.2f|%.2f|%.2f|%.2f|%.2f|%.2f|%.0f\n",
          kv.key.L_RETURNFLAG, kv.key.L_LINESTATUS, kv.aggs(1), kv.aggs(2), kv.aggs(3), kv.aggs(4),
          kv.aggs(6), kv.aggs(7), kv.aggs(8), kv.aggs(5)), -1)
        po.run()
        ()
      }
    }
    //FunctionCalls.numberTuplesSelect: " + //ProfileFunctionCalls.numberTuplesSelect)
    //FunctionCalls.numberTuplesFiltered: " + //ProfileFunctionCalls.numberTuplesFiltered)
    //FunctionCalls.numberTuplesScanned: " + //ProfileFunctionCalls.numberTuplesScanned);
  }

  def Q1_functional(numRuns: Int) {
    //ProfileFunctionCalls.numberTuplesSelect = 0
    //ProfileFunctionCalls.numberTuplesFiltered = 0
    //ProfileFunctionCalls.numberTuplesScanned = 0
    val lineitemTable = Query(loadLineitem())
    for (i <- 0 until numRuns) {
      runQuery {
        val constantDate: Int = parseDate("1998-09-02")
        val result = lineitemTable.filter(_.L_SHIPDATE <= constantDate).groupBy(x => new Q1GRPRecord(
          x.L_RETURNFLAG, x.L_LINESTATUS)).mapValues(l =>
          Array(
            l.map(_.L_DISCOUNT).sum,
            l.map(_.L_QUANTITY).sum,
            l.map(_.L_EXTENDEDPRICE).sum,
            l.map(t => t.L_EXTENDEDPRICE * (1.0 - t.L_DISCOUNT)).sum,
            l.map(t => t.L_EXTENDEDPRICE * (1.0 - t.L_DISCOUNT) * (1.0 + t.L_TAX)).sum,
            l.count,
            l.map(_.L_QUANTITY).avg,
            l.map(_.L_EXTENDEDPRICE).avg,
            l.map(_.L_DISCOUNT).avg))
          .sortBy(t =>
            t._1.L_RETURNFLAG.toInt * 128 + t._1.L_LINESTATUS.toInt)
        result.printRows(kv => {
          printf("%c|%c|%.2f|%.2f|%.2f|%.2f|%.2f|%.2f|%.2f|%.0f\n",
            kv._1.L_RETURNFLAG, kv._1.L_LINESTATUS, kv._2.apply(1), kv._2.apply(2), kv._2.apply(3), kv._2.apply(4),
            kv._2.apply(6), kv._2.apply(7), kv._2.apply(8), kv._2.apply(5))
        }, -1)
      }
    }
  }

  def Q1_functional_p2(numRuns: Int) {
    //ProfileFunctionCalls.numberTuplesSelect = 0
    //ProfileFunctionCalls.numberTuplesFiltered = 0
    //ProfileFunctionCalls.numberTuplesScanned = 0
    val lineitemTable = loadLineitem()
    for (i <- 0 until numRuns) {
      runQuery {
        val constantDate: Int = parseDate("1998-09-02")
        val result = Query(lineitemTable).filter(_.L_SHIPDATE <= constantDate).groupBy(x => new Q1GRPRecord(
          x.L_RETURNFLAG, x.L_LINESTATUS)).mapValues(l => {
          var x0 = 0.0
          var x1 = 0.0
          var x2 = 0.0
          var x3 = 0.0
          var x4 = 0.0
          var x5 = 0.0
          l.foreach(cur => {
            x0 += cur.L_DISCOUNT
            x1 += cur.L_QUANTITY
            x2 += cur.L_EXTENDEDPRICE
            x3 += cur.L_EXTENDEDPRICE * (1.0 - cur.L_DISCOUNT)
            x4 += cur.L_EXTENDEDPRICE * (1.0 - cur.L_DISCOUNT) * (1.0 + cur.L_TAX)
            x5 += 1
          })
          Array(
            x0,
            x1,
            x2,
            x3,
            x4,
            x5,
            x1 / x5,
            x2 / x5,
            x0 / x5)
        })
          .sortBy(t =>
            t._1.L_RETURNFLAG.toInt * 128 + t._1.L_LINESTATUS.toInt)
        result.printRows(kv => {
          printf("%c|%c|%.2f|%.2f|%.2f|%.2f|%.2f|%.2f|%.2f|%.0f\n",
            kv._1.L_RETURNFLAG, kv._1.L_LINESTATUS, kv._2.apply(1), kv._2.apply(2), kv._2.apply(3), kv._2.apply(4),
            kv._2.apply(6), kv._2.apply(7), kv._2.apply(8), kv._2.apply(5))
        }, -1)
      }
    }
  }

  def Q2(numRuns: Int) {

    //ProfileFunctionCalls.numberTuplesSelect = 0
    //ProfileFunctionCalls.numberTuplesFiltered = 0
    //ProfileFunctionCalls.numberTuplesScanned = 0
    val partTable = loadPart()
    val partsuppTable = loadPartsupp()
    val nationTable = loadNation()
    val regionTable = loadRegion()
    val supplierTable = loadSupplier()
    for (i <- 0 until numRuns) {
      runQuery {
        val africa = parseString("AFRICA")
        val tin = parseString("TIN")
        val partsuppScan = new ScanOp(partsuppTable)
        val supplierScan = new ScanOp(supplierTable)
        val jo1 = new HashJoinOp(supplierScan, partsuppScan)((x, y) => x.S_SUPPKEY == y.PS_SUPPKEY)(x => x.S_SUPPKEY)(x => x.PS_SUPPKEY)
        val nationScan = new ScanOp(nationTable)
        val jo2 = new HashJoinOp(nationScan, jo1)((x, y) => x.N_NATIONKEY == y.S_NATIONKEY[Int])(x => x.N_NATIONKEY)(x => x.S_NATIONKEY[Int])
        val partScan = new SelectOp(new ScanOp(partTable))(x => x.P_SIZE == 43 && x.P_TYPE.endsWith(tin))
        val jo3 = new HashJoinOp(partScan, jo2)((x, y) => x.P_PARTKEY == y.PS_PARTKEY[Int])(x => x.P_PARTKEY)(x => x.PS_PARTKEY[Int])
        val regionScan = new SelectOp(new ScanOp(regionTable))(x => x.R_NAME === africa) //for comparing equality of OptimalString we should use === instead of ==
        val jo4 = new HashJoinOp(regionScan, jo3)((x, y) => x.R_REGIONKEY == y.N_REGIONKEY[Int])(x => x.R_REGIONKEY)(x => x.N_REGIONKEY[Int])
        val wo = new WindowOp(jo4)(x => x.P_PARTKEY[Int])(x => x.minBy(y => y.PS_SUPPLYCOST[Double]))
        val so = new SortOp(wo)((x, y) => {
          if (x.wnd.S_ACCTBAL[Double] < y.wnd.S_ACCTBAL[Double]) 1
          else if (x.wnd.S_ACCTBAL[Double] > y.wnd.S_ACCTBAL[Double]) -1
          else {
            var res = x.wnd.N_NAME[OptimalString] diff y.wnd.N_NAME[OptimalString]
            if (res == 0) {
              res = x.wnd.S_NAME[OptimalString] diff y.wnd.S_NAME[OptimalString]
              if (res == 0) res = x.wnd.P_PARTKEY[Int] - y.wnd.P_PARTKEY[Int]
            }
            res
          }
        })
        val po = new PrintOp(so)(e => {
          val kv = e.wnd
          printf("%.2f|%s|%s|%d|%s|%s|%s|%s\n", kv.S_ACCTBAL[Double], (kv.S_NAME[OptimalString]).string, (kv.N_NAME[OptimalString]).string, kv.P_PARTKEY[Int], (kv.P_MFGR[OptimalString]).string, (kv.S_ADDRESS[OptimalString]).string, (kv.S_PHONE[OptimalString]).string, (kv.S_COMMENT[OptimalString]).string)
        }, 100)
        po.run()
        ()
      }
    }
    //FunctionCalls.numberTuplesSelect: " + //ProfileFunctionCalls.numberTuplesSelect)
    //FunctionCalls.numberTuplesFiltered: " + //ProfileFunctionCalls.numberTuplesFiltered)
    //FunctionCalls.numberTuplesScanned: " + //ProfileFunctionCalls.numberTuplesScanned);
  }

  def Q2_functional(numRuns: Int) {
    //ProfileFunctionCalls.numberTuplesSelect = 0
    //ProfileFunctionCalls.numberTuplesFiltered = 0
    //ProfileFunctionCalls.numberTuplesScanned = 0
    val partTable = loadPart()
    val partsuppTable = loadPartsupp()
    val nationTable = loadNation()
    val regionTable = loadRegion()
    val supplierTable = loadSupplier()
    for (i <- 0 until numRuns) {
      runQuery {
        val africa = parseString("AFRICA")
        val tin = parseString("TIN")
        val jo1 = Query(supplierTable).hashJoin(Query(partsuppTable))(x => x.S_SUPPKEY)(x => x.PS_SUPPKEY)((x, y) => x.S_SUPPKEY == y.PS_SUPPKEY)
        val jo2 = Query(nationTable).hashJoin(jo1)(x => x.N_NATIONKEY)(x => x.S_NATIONKEY[Int])((x, y) => x.N_NATIONKEY == y.S_NATIONKEY[Int])
        val partScan = Query(partTable).filter(x => x.P_SIZE == 43 && x.P_TYPE.endsWith(tin))
        val jo3 = partScan.hashJoin(jo2)(x => x.P_PARTKEY)(x => x.PS_PARTKEY[Int])((x, y) => x.P_PARTKEY == y.PS_PARTKEY[Int])
        val regionScan = Query(regionTable).filter(_.R_NAME === africa)
        val jo4 = regionScan.hashJoin(jo3)(x => x.R_REGIONKEY)(x => x.N_REGIONKEY[Int])((x, y) => x.R_REGIONKEY == y.N_REGIONKEY[Int])
        val wo = jo4.groupBy(x => x.P_PARTKEY[Int]).mapValues(x => x.minBy(y => y.PS_SUPPLYCOST[Double]))
        val so = wo.sortBy(x => (-x._2.S_ACCTBAL[Double], x._2.N_NAME[OptimalString].string, x._2.S_NAME[OptimalString].string, x._2.P_PARTKEY[Int]))
        so.take(100).printRows(e => {
          val kv = e._2
          printf("%.2f|%s|%s|%d|%s|%s|%s|%s\n", kv.S_ACCTBAL[Double], (kv.S_NAME[OptimalString]).string, (kv.N_NAME[OptimalString]).string, kv.P_PARTKEY[Int], (kv.P_MFGR[OptimalString]).string, (kv.S_ADDRESS[OptimalString]).string, (kv.S_PHONE[OptimalString]).string, (kv.S_COMMENT[OptimalString]).string)
        }, -1)
        ()
      }
    }
  }

  def Q3(numRuns: Int) {
    //ProfileFunctionCalls.numberTuplesSelect = 0
    //ProfileFunctionCalls.numberTuplesFiltered = 0
    //ProfileFunctionCalls.numberTuplesScanned = 0
    val lineitemTable = loadLineitem()
    val ordersTable = loadOrders()
    val customerTable = loadCustomer()
    for (i <- 0 until numRuns) {
      runQuery {
        val constantDate = parseDate("1995-03-04")
        val houseHold = parseString("HOUSEHOLD")
        val scanCustomer = new SelectOp(new ScanOp(customerTable))(x => x.C_MKTSEGMENT === houseHold)
        val scanOrders = new SelectOp(new ScanOp(ordersTable))(x => x.O_ORDERDATE < constantDate)
        val scanLineitem = new SelectOp(new ScanOp(lineitemTable))(x => x.L_SHIPDATE > constantDate)
        val jo1 = new HashJoinOp(scanCustomer, scanOrders)((x, y) => x.C_CUSTKEY == y.O_CUSTKEY)(x => x.C_CUSTKEY)(x => x.O_CUSTKEY)
        val jo2 = new HashJoinOp(jo1, scanLineitem)((x, y) => x.O_ORDERKEY[Int] == y.L_ORDERKEY)(x => x.O_ORDERKEY[Int])(x => x.L_ORDERKEY)
        val aggOp = new AggOp(jo2, 1)(x => new Q3GRPRecord(x.L_ORDERKEY[Int], x.O_ORDERDATE[Int], x.O_SHIPPRIORITY[Int]))((t, currAgg) => { currAgg + (t.L_EXTENDEDPRICE[Double] * (1.0 - t.L_DISCOUNT[Double])) })
        val sortOp = new SortOp(aggOp)((kv1, kv2) => {
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
        val po = new PrintOp(sortOp)(kv => {
          printf("%d|%.4f|%s|%d\n", kv.key.L_ORDERKEY, kv.aggs(0), dateToString(kv.key.O_ORDERDATE), kv.key.O_SHIPPRIORITY)
        }, 10)
        po.run()
        ()
      }
    }
    //FunctionCalls.numberTuplesSelect: " + //ProfileFunctionCalls.numberTuplesSelect)
    //FunctionCalls.numberTuplesFiltered: " + //ProfileFunctionCalls.numberTuplesFiltered)
    //FunctionCalls.numberTuplesScanned: " + //ProfileFunctionCalls.numberTuplesScanned);
  }

  //Modified no date filtering
  /*def Q3(numRuns: Int) {
    //ProfileFunctionCalls.numberTuplesSelect = 0
    //ProfileFunctionCalls.numberTuplesFiltered = 0
    //ProfileFunctionCalls.numberTuplesScanned = 0
    val lineitemTable = loadLineitem()
    val ordersTable = loadOrders()
    val customerTable = loadCustomer()
    for (i <- 0 until numRuns) {
      runQuery {
        val scanCustomer = new ScanOp(customerTable)
        val scanOrders = new ScanOp(ordersTable)
        val scanLineitem = new ScanOp(lineitemTable)
        val jo1 = new HashJoinOp(scanCustomer, scanOrders)((x, y) => x.C_CUSTKEY == y.O_CUSTKEY)(x => x.C_CUSTKEY)(x => x.O_CUSTKEY)
	    val jo2 = new HashJoinOp(jo1, scanLineitem)((x, y) => x.O_ORDERKEY[Int] == y.L_ORDERKEY)(x => x.O_ORDERKEY[Int])(x => x.L_ORDERKEY)
//val aggOp = new AggOp(jo2, 1)(x => "Total")((t, currAgg) => {currAgg + 1})
        val aggOp = new AggOp(jo2, 1)(x => new Q3GRPRecord(x.L_ORDERKEY[Int], x.O_ORDERDATE[Int], x.O_SHIPPRIORITY[Int]))((t, currAgg) => { currAgg + (t.L_EXTENDEDPRICE[Double] * (1.0 - t.L_DISCOUNT[Double])) })
		
        val sortOp = new SortOp(aggOp)((kv1, kv2) => {
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
        val po = new PrintOp(sortOp)(kv => {
//			printf("%f %f\n", kv.aggs(0), kv.aggs(1))
          printf("%d|%.4f|%s|%d\n", kv.key.L_ORDERKEY, kv.aggs(0), dateToString(kv.key.O_ORDERDATE), kv.key.O_SHIPPRIORITY)
        }, 10)
        po.run()
        ()
      }
    }
  }*/

  def Q3_functional(numRuns: Int) {
    //ProfileFunctionCalls.numberTuplesSelect = 0
    //ProfileFunctionCalls.numberTuplesFiltered = 0
    //ProfileFunctionCalls.numberTuplesScanned = 0
    val lineitemTable = loadLineitem()
    val ordersTable = loadOrders()
    val customerTable = loadCustomer()
    for (i <- 0 until numRuns) {
      runQuery {
        val constantDate = parseDate("1995-03-04")
        val houseHold = parseString("HOUSEHOLD")
        val scanCustomer = Query(customerTable).filter(x => x.C_MKTSEGMENT === houseHold)
        val scanOrders = Query(ordersTable).filter(x => x.O_ORDERDATE < constantDate)
        val scanLineitem = Query(lineitemTable).filter(x => x.L_SHIPDATE > constantDate)
        val jo1 = scanCustomer.hashJoin(scanOrders)(x => x.C_CUSTKEY)(x => x.O_CUSTKEY)((x, y) => x.C_CUSTKEY == y.O_CUSTKEY)
        val jo2 = jo1.hashJoin(scanLineitem)(x => x.O_ORDERKEY[Int])(x => x.L_ORDERKEY)((x, y) => x.O_ORDERKEY[Int] == y.L_ORDERKEY)
        val aggOp = jo2.groupBy(x => new Q3GRPRecord(x.L_ORDERKEY[Int], x.O_ORDERDATE[Int], x.O_SHIPPRIORITY[Int])).mapValues(_.map(t => (t.L_EXTENDEDPRICE[Double] * (1.0 - t.L_DISCOUNT[Double]))).sum)
        val sortOp = aggOp.sortBy(x => (-x._2, x._1.O_ORDERDATE))
        var rows = 0
        sortOp.take(10).printRows(e =>
          printf("%d|%.4f|%s|%d\n", e._1.L_ORDERKEY, e._2, dateToString(e._1.O_ORDERDATE), e._1.O_SHIPPRIORITY), -1)
      }
    }
  }

  def Q4(numRuns: Int) {
    //ProfileFunctionCalls.numberTuplesSelect = 0
    //ProfileFunctionCalls.numberTuplesFiltered = 0
    //ProfileFunctionCalls.numberTuplesScanned = 0
    val lineitemTable = loadLineitem()
    val ordersTable = loadOrders()
    for (i <- 0 until numRuns) {
      runQuery({
        val constantDate1: Int = parseDate("1993-11-01")
        val constantDate2: Int = parseDate("1993-08-01")
        val scanOrders = new SelectOp(new ScanOp(ordersTable))(x => x.O_ORDERDATE < constantDate1 && x.O_ORDERDATE >= constantDate2)
        val scanLineitem = new SelectOp(new ScanOp(lineitemTable))(x => x.L_COMMITDATE < x.L_RECEIPTDATE)
        val hj = new LeftHashSemiJoinOp(scanOrders, scanLineitem)((x, y) => x.O_ORDERKEY == y.L_ORDERKEY)(x => x.O_ORDERKEY)(x => x.L_ORDERKEY)
        val aggOp = new AggOp(hj, 1)(x => x.O_ORDERPRIORITY)(
          (t, currAgg) => { currAgg + 1 })
        val sortOp = new SortOp(aggOp)((kv1, kv2) => {
          val k1 = kv1.key; val k2 = kv2.key
          k1 diff k2
        })
        val po = new PrintOp(sortOp)(kv => printf("%s|%.0f\n", kv.key.string, kv.aggs(0)), -1)
        po.run()
        ()
      })
    }
    //FunctionCalls.numberTuplesSelect: " + //ProfileFunctionCalls.numberTuplesSelect)
    //FunctionCalls.numberTuplesFiltered: " + //ProfileFunctionCalls.numberTuplesFiltered)
    //FunctionCalls.numberTuplesScanned: " + //ProfileFunctionCalls.numberTuplesScanned);
  }

  def Q4_functional(numRuns: Int) {
    //ProfileFunctionCalls.numberTuplesSelect = 0
    //ProfileFunctionCalls.numberTuplesFiltered = 0
    //ProfileFunctionCalls.numberTuplesScanned = 0
    val lineitemTable = loadLineitem()
    val ordersTable = loadOrders()
    for (i <- 0 until numRuns) {
      runQuery({
        val constantDate1: Int = parseDate("1993-11-01")
        val constantDate2: Int = parseDate("1993-08-01")
        val scanOrders = Query(ordersTable).filter(x => x.O_ORDERDATE < constantDate1 && x.O_ORDERDATE >= constantDate2)
        val scanLineitem = Query(lineitemTable).filter(x => x.L_COMMITDATE < x.L_RECEIPTDATE)
        val hj = scanOrders.leftHashSemiJoin(scanLineitem)(x => x.O_ORDERKEY)(x => x.L_ORDERKEY)((x, y) => x.O_ORDERKEY == y.L_ORDERKEY)
        val aggRes = hj.groupBy(x => x.O_ORDERPRIORITY).mapValues(_.count)
        val sortOp = aggRes.sortBy(_._1.string)
        sortOp.printRows(kv =>
          printf("%s|%d\n", kv._1.string, kv._2), -1)
      })
    }
  }

  def Q5(numRuns: Int) {
    //ProfileFunctionCalls.numberTuplesSelect = 0
    //ProfileFunctionCalls.numberTuplesFiltered = 0
    //ProfileFunctionCalls.numberTuplesScanned = 0
    val nationTable = loadNation()
    val regionTable = loadRegion()
    val supplierTable = loadSupplier()
    val lineitemTable = loadLineitem()
    val ordersTable = loadOrders()

    val customerTable = loadCustomer()

    for (i <- 0 until numRuns) {
      runQuery({
        val constantDate1 = parseDate("1996-01-01")
        val constantDate2 = parseDate("1997-01-01")
        val asia = parseString("ASIA")
        val scanRegion = new SelectOp(new ScanOp(regionTable))(x => x.R_NAME === asia)
        val scanNation = new ScanOp(nationTable)
        val scanLineitem = new ScanOp(lineitemTable)
        val scanOrders = new SelectOp(new ScanOp(ordersTable))(x => x.O_ORDERDATE >= constantDate1 && x.O_ORDERDATE < constantDate2)
        val scanCustomer = new ScanOp(customerTable)
        val scanSupplier = new ScanOp(supplierTable)
        val jo1 = new HashJoinOp(scanRegion, scanNation)((x, y) => x.R_REGIONKEY == y.N_REGIONKEY)(x => x.R_REGIONKEY)(x => x.N_REGIONKEY)
        val jo2 = new HashJoinOp(jo1, scanCustomer)((x, y) => x.N_NATIONKEY[Int] == y.C_NATIONKEY)(x => x.N_NATIONKEY[Int])(x => x.C_NATIONKEY)
        val jo3 = new HashJoinOp(jo2, scanOrders)((x, y) => x.C_CUSTKEY[Int] == y.O_CUSTKEY)(x => x.C_CUSTKEY[Int])(x => x.O_CUSTKEY)
        val jo4 = new HashJoinOp(jo3, scanLineitem)((x, y) => x.O_ORDERKEY[Int] == y.L_ORDERKEY)(x => x.O_ORDERKEY[Int])(x => x.L_ORDERKEY)
        val jo5 = new HashJoinOp(scanSupplier, jo4)((x, y) => x.S_SUPPKEY == y.L_SUPPKEY[Int] && y.N_NATIONKEY[Int] == x.S_NATIONKEY)(x => x.S_SUPPKEY)(x => x.L_SUPPKEY[Int])

        val aggOp = new AggOp(jo5, 1)(x => x.N_NAME[OptimalString])(
          (t, currAgg) => { currAgg + t.L_EXTENDEDPRICE[Double] * (1.0 - t.L_DISCOUNT[Double]) })
        val sortOp = new SortOp(aggOp)((x, y) => {
          if (x.aggs(0) < y.aggs(0)) 1
          else if (x.aggs(0) > y.aggs(0)) -1
          else 0
        })
        val po = new PrintOp(sortOp)(kv => { printf("%s|%.4f\n", kv.key.string, kv.aggs(0)) }, -1)
        po.run()
        ()
      })
    }
    //FunctionCalls.numberTuplesSelect: " + //ProfileFunctionCalls.numberTuplesSelect)
    //FunctionCalls.numberTuplesFiltered: " + //ProfileFunctionCalls.numberTuplesFiltered)
    //FunctionCalls.numberTuplesScanned: " + //ProfileFunctionCalls.numberTuplesScanned);
  }

  def Q5_functional(numRuns: Int) {
    //ProfileFunctionCalls.numberTuplesSelect = 0
    //ProfileFunctionCalls.numberTuplesFiltered = 0
    //ProfileFunctionCalls.numberTuplesScanned = 0
    val nationTable = loadNation()
    val regionTable = loadRegion()
    val supplierTable = loadSupplier()
    val lineitemTable = loadLineitem()
    val ordersTable = loadOrders()
    val customerTable = loadCustomer()

    for (i <- 0 until numRuns) {
      runQuery({
        val constantDate1 = parseDate("1996-01-01")
        val constantDate2 = parseDate("1997-01-01")
        val asia = parseString("ASIA")
        val scanRegion =
          Query(regionTable).filter(x => x.R_NAME === asia)
        val scanNation =
          Query(nationTable)
        val scanLineitem =
          Query(lineitemTable)
        val scanOrders =
          Query(ordersTable).filter(x => x.O_ORDERDATE >= constantDate1 && x.O_ORDERDATE < constantDate2)
        val scanCustomer =
          Query(customerTable)
        val scanSupplier =
          Query(supplierTable)
        val jo1 =
          scanRegion.hashJoin(scanNation)(x => x.R_REGIONKEY)(x => x.N_REGIONKEY)((x, y) => x.R_REGIONKEY == y.N_REGIONKEY)
        val jo2 =
          jo1.hashJoin(scanCustomer)(x => x.N_NATIONKEY[Int])(x => x.C_NATIONKEY)((x, y) => x.N_NATIONKEY[Int] == y.C_NATIONKEY)
        val jo3 =
          jo2.hashJoin(scanOrders)(x => x.C_CUSTKEY[Int])(x => x.O_CUSTKEY)((x, y) => x.C_CUSTKEY[Int] == y.O_CUSTKEY)
        val jo4 =
          jo3.hashJoin(scanLineitem)(x => x.O_ORDERKEY[Int])(x => x.L_ORDERKEY)((x, y) => x.O_ORDERKEY[Int] == y.L_ORDERKEY)
        val jo5 =
          scanSupplier.hashJoin(jo4)(x => x.S_SUPPKEY)(x => x.L_SUPPKEY[Int])((x, y) => x.S_SUPPKEY == y.L_SUPPKEY[Int] && y.N_NATIONKEY[Int] == x.S_NATIONKEY)

        val aggOp = jo5.groupBy(x => x.N_NAME[OptimalString]).mapValues(list => list.map(t => t.L_EXTENDEDPRICE[Double] * (1.0 - t.L_DISCOUNT[Double])).sum)
        val sortOp = aggOp.sortBy(x => -x._2)
        sortOp.printRows(kv =>
          printf("%s|%.4f\n", kv._1.string, kv._2), -1)
        ()
      })
    }
  }

  def Q6(numRuns: Int) {
    //ProfileFunctionCalls.numberTuplesSelect = 0
    //ProfileFunctionCalls.numberTuplesFiltered = 0
    //ProfileFunctionCalls.numberTuplesScanned = 0
    val lineitemTable = loadLineitem()
    for (i <- 0 until numRuns) {
      runQuery({
        val constantDate1: Int = parseDate("1996-01-01")
        val constantDate2: Int = parseDate("1997-01-01")
        val lineitemScan = new SelectOp(new ScanOp(lineitemTable))(x =>
          x.L_SHIPDATE >= constantDate1 && (x.L_SHIPDATE < constantDate2 && (x.L_DISCOUNT >= 0.08 && (x.L_DISCOUNT <= 0.1 && (x.L_QUANTITY < 24)))))
        val aggOp = new AggOp(lineitemScan, 1)(x => "Total")((t, currAgg) => { (t.L_EXTENDEDPRICE * t.L_DISCOUNT) + currAgg })
        val po = new PrintOp(aggOp)(kv => { kv.key; printf("%.4f\n", kv.aggs(0)) }, -1)
        po.run()
        ()
      })
    }
    //FunctionCalls.numberTuplesSelect: " + //ProfileFunctionCalls.numberTuplesSelect)
    //FunctionCalls.numberTuplesFiltered: " + //ProfileFunctionCalls.numberTuplesFiltered)
    //FunctionCalls.numberTuplesScanned: " + //ProfileFunctionCalls.numberTuplesScanned);
  }

  def Q6_functional(numRuns: Int) {
    //ProfileFunctionCalls.numberTuplesSelect = 0
    //ProfileFunctionCalls.numberTuplesFiltered = 0
    //ProfileFunctionCalls.numberTuplesScanned = 0
    val lineitemTable = loadLineitem()
    for (i <- 0 until numRuns) {
      runQuery {
        val constantDate1: Int = parseDate("1996-01-01")
        val constantDate2: Int = parseDate("1997-01-01")
        val result = Query(lineitemTable).filter(x => x.L_SHIPDATE >= constantDate1 && (x.L_SHIPDATE < constantDate2 && (x.L_DISCOUNT >= 0.08 && (x.L_DISCOUNT <= 0.1 && (x.L_QUANTITY < 24)))))
          .map(t => t.L_EXTENDEDPRICE * t.L_DISCOUNT).sum
        printf("%.4f\n", result)
        printf("(%d rows)\n", 1)
      }
    }
  }

  def Q7(numRuns: Int) {
    //ProfileFunctionCalls.numberTuplesSelect = 0
    //ProfileFunctionCalls.numberTuplesFiltered = 0
    //ProfileFunctionCalls.numberTuplesScanned = 0
    val nationTable = loadNation()
    val ordersTable = loadOrders()
    val lineitemTable = loadLineitem()
    val customerTable = loadCustomer()
    val supplierTable = loadSupplier()
    for (i <- 0 until numRuns) {
      runQuery({
        val usa = parseString("UNITED STATES")
        val indonesia = parseString("INDONESIA")
        val scanNation1 = new ScanOp(nationTable)
        val scanNation2 = new ScanOp(nationTable)
        val jo1 = new NestedLoopsJoinOp(scanNation1, scanNation2, "N1_", "N2_")((x, y) => ((x.N_NAME === usa && y.N_NAME === indonesia) || (x.N_NAME === indonesia && y.N_NAME === usa)))
        val scanSupplier = new ScanOp(supplierTable)
        val jo2 = new HashJoinOp(jo1, scanSupplier)((x, y) => x.N1_N_NATIONKEY[Int] == y.S_NATIONKEY)(x => x.N1_N_NATIONKEY[Int])(x => x.S_NATIONKEY)
        val scanLineitem = new SelectOp(new ScanOp(lineitemTable))(x => x.L_SHIPDATE >= parseDate("1995-01-01") && x.L_SHIPDATE <= parseDate("1996-12-31"))
        val jo3 = new HashJoinOp(jo2, scanLineitem)((x, y) => x.S_SUPPKEY[Int] == y.L_SUPPKEY)(x => x.S_SUPPKEY[Int])(x => x.L_SUPPKEY)
        val scanOrders = new ScanOp(ordersTable)
        val jo4 = new HashJoinOp(jo3, scanOrders)((x, y) => x.L_ORDERKEY[Int] == y.O_ORDERKEY)(x => x.L_ORDERKEY[Int])(x => x.O_ORDERKEY)
        val scanCustomer = new ScanOp(customerTable)
        val jo5 = new HashJoinOp(scanCustomer, jo4)((x, y) => x.C_CUSTKEY == y.O_CUSTKEY[Int] && x.C_NATIONKEY == y.N2_N_NATIONKEY[Int])(x => x.C_CUSTKEY)(x => x.O_CUSTKEY[Int])
        val gb = new AggOp(jo5, 1)(x => new Q7GRPRecord(
          x.N1_N_NAME[OptimalString],
          x.N2_N_NAME[OptimalString],
          dateToYear(x.L_SHIPDATE[Int])))((t, currAgg) => { currAgg + (t.L_EXTENDEDPRICE[Double] * (1.0 - t.L_DISCOUNT[Double])) })
        val so = new SortOp(gb)((kv1, kv2) => {
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
        val po = new PrintOp(so)(kv => printf("%s|%s|%d|%.4f\n", kv.key.SUPP_NATION.string, kv.key.CUST_NATION.string, kv.key.L_YEAR, kv.aggs(0)), -1)
        po.run()
        ()
      })
    }
    //FunctionCalls.numberTuplesSelect: " + //ProfileFunctionCalls.numberTuplesSelect)
    //FunctionCalls.numberTuplesFiltered: " + //ProfileFunctionCalls.numberTuplesFiltered)
    //FunctionCalls.numberTuplesScanned: " + //ProfileFunctionCalls.numberTuplesScanned);
  }

  def Q8(numRuns: Int) {
    //ProfileFunctionCalls.numberTuplesSelect = 0
    //ProfileFunctionCalls.numberTuplesFiltered = 0
    //ProfileFunctionCalls.numberTuplesScanned = 0
    val partTable = loadPart()
    val lineitemTable = loadLineitem()
    val ordersTable = loadOrders()
    val customerTable = loadCustomer()
    val nationTable = loadNation()
    val regionTable = loadRegion()
    val supplierTable = loadSupplier()
    for (i <- 0 until numRuns) {
      runQuery({
        val constantDate1 = parseDate("1995-01-01")
        val constantDate2 = parseDate("1996-12-31")
        val asia = parseString("ASIA")
        val indonesia = parseString("INDONESIA")
        val medAnonNick = parseString("MEDIUM ANODIZED NICKEL")
        val scanPart = new SelectOp(new ScanOp(partTable))(x => x.P_TYPE === medAnonNick)
        val scanLineitem = new ScanOp(lineitemTable)
        val scanOrders = new SelectOp(new ScanOp(ordersTable))(x => x.O_ORDERDATE >= constantDate1 && x.O_ORDERDATE <= constantDate2)
        val scanCustomer = new ScanOp(customerTable)
        val scanNation1 = new ScanOp(nationTable)
        val scanRegion = new SelectOp(new ScanOp(regionTable))(x => x.R_NAME === asia)
        val scanSupplier = new ScanOp(supplierTable)
        val scanNation2 = new ScanOp(nationTable)
        val jo1 = new HashJoinOp(scanLineitem, scanPart)((x, y) => x.L_PARTKEY == y.P_PARTKEY)(x => x.L_PARTKEY)(x => x.P_PARTKEY)
        val jo2 = new HashJoinOp(jo1, scanOrders)((x, y) => x.L_ORDERKEY[Int] == y.O_ORDERKEY)(x => x.L_ORDERKEY[Int])(x => x.O_ORDERKEY)
        val jo3 = new HashJoinOp(jo2, scanCustomer)((x, y) => x.O_CUSTKEY[Int] == y.C_CUSTKEY)(x => x.O_CUSTKEY[Int])(x => x.C_CUSTKEY)
        val jo4 = new HashJoinOp(scanNation1, jo3)((x, y) => x.N_NATIONKEY == y.C_NATIONKEY[Int])(x => x.N_NATIONKEY)(x => x.C_NATIONKEY[Int])
        val jo5 = new HashJoinOp(scanRegion, jo4)((x, y) => x.R_REGIONKEY == y.N_REGIONKEY[Int])(x => x.R_REGIONKEY)(x => x.N_REGIONKEY[Int])
        val jo6 = new HashJoinOp(scanSupplier, jo5)((x, y) => x.S_SUPPKEY == y.L_SUPPKEY[Int])(x => x.S_SUPPKEY)(x => x.L_SUPPKEY[Int])
        val jo7 = new HashJoinOp(scanNation2, jo6, "REC1_", "REC2_")((x, y) => x.N_NATIONKEY == y.S_NATIONKEY[Int])(x => x.N_NATIONKEY)(x => x.S_NATIONKEY[Int])
        val aggOp = new AggOp(jo7, 3)(x => dateToYear(x.REC2_O_ORDERDATE[Int]))(
          (t, currAgg) => { currAgg + (t.REC2_L_EXTENDEDPRICE[Double] * (1.0 - t.REC2_L_DISCOUNT[Double])) },
          (t, currAgg) => {
            if (t.REC1_N_NAME[OptimalString] === indonesia) currAgg + (t.REC2_L_EXTENDEDPRICE[Double] * (1.0 - t.REC2_L_DISCOUNT[Double]))
            else currAgg
          })
        val mapOp = new MapOp(aggOp)(x => x.aggs(2) = x.aggs(1) / x.aggs(0))
        val sortOp = new SortOp(mapOp)((x, y) => {
          if (x.key < y.key) -1
          else if (x.key > y.key) 1
          else 0
        })
        val po = new PrintOp(sortOp)(kv => printf("%d|%.4f\n", kv.key, kv.aggs(2)), -1)
        po.run()
        ()
      })
    }
    //FunctionCalls.numberTuplesSelect: " + //ProfileFunctionCalls.numberTuplesSelect)
    //FunctionCalls.numberTuplesFiltered: " + //ProfileFunctionCalls.numberTuplesFiltered)
    //FunctionCalls.numberTuplesScanned: " + //ProfileFunctionCalls.numberTuplesScanned);
  }

  def Q9(numRuns: Int) {
    //ProfileFunctionCalls.numberTuplesSelect = 0
    //ProfileFunctionCalls.numberTuplesFiltered = 0
    //ProfileFunctionCalls.numberTuplesScanned = 0
    val partTable = loadPart()
    val nationTable = loadNation()
    val ordersTable = loadOrders()
    val partsuppTable = loadPartsupp()
    val supplierTable = loadSupplier()
    val lineitemTable = loadLineitem()
    for (i <- 0 until numRuns) {
      runQuery({
        val ghost = parseString("ghost")
        val soNation = new ScanOp(nationTable)
        val soSupplier = new ScanOp(supplierTable)
        val soLineitem = new ScanOp(lineitemTable)
        val soPart = new SelectOp(new ScanOp(partTable))(x => x.P_NAME.containsSlice(ghost))
        val soPartsupp = new ScanOp(partsuppTable)
        val soOrders = new ScanOp(ordersTable)
        val hj1 = new HashJoinOp(soLineitem, soPart)((x, y) => x.L_PARTKEY == y.P_PARTKEY)(x => x.L_PARTKEY)(x => x.P_PARTKEY)
        val hj2 = new HashJoinOp(hj1, soSupplier)((x, y) => x.L_SUPPKEY[Int] == y.S_SUPPKEY)(x => x.L_SUPPKEY[Int])(x => x.S_SUPPKEY)
        val hj3 = new HashJoinOp(hj2, soNation)((x, y) => x.S_NATIONKEY[Int] == y.N_NATIONKEY)(x => x.S_NATIONKEY[Int])(x => x.N_NATIONKEY)
        val hj4 = new HashJoinOp(soPartsupp, hj3)((x, y) => x.PS_PARTKEY == y.L_PARTKEY[Int] && x.PS_SUPPKEY == y.L_SUPPKEY[Int])(x => x.PS_PARTKEY)(x => x.L_PARTKEY[Int])
        val hj5 = new HashJoinOp(hj4, soOrders)((x, y) => x.L_ORDERKEY[Int] == y.O_ORDERKEY)(x => x.L_ORDERKEY[Int])(x => x.O_ORDERKEY)
        val aggOp = new AggOp(hj5, 1)(x => new Q9GRPRecord(x.N_NAME[OptimalString], dateToYear(x.O_ORDERDATE[Int])))(
          (t, currAgg) => { currAgg + ((t.L_EXTENDEDPRICE[Double] * (1.0 - t.L_DISCOUNT[Double]))) - ((1.0 * t.PS_SUPPLYCOST[Double]) * t.L_QUANTITY[Double]) })
        val sortOp = new SortOp(aggOp)((kv1, kv2) => {
          val k1 = kv1.key; val k2 = kv2.key
          val r = k1.NATION diff k2.NATION
          if (r == 0) {
            if (k1.O_YEAR < k2.O_YEAR) 1
            else if (k1.O_YEAR > k2.O_YEAR) -1
            else 0
          } else r
        })
        val po = new PrintOp(sortOp)(kv => printf("%s|%d|%.4f\n", kv.key.NATION.string, kv.key.O_YEAR, kv.aggs(0)), -1)
        po.run()
        ()
      })
    }
    //FunctionCalls.numberTuplesSelect: " + //ProfileFunctionCalls.numberTuplesSelect)
    //FunctionCalls.numberTuplesFiltered: " + //ProfileFunctionCalls.numberTuplesFiltered)
    //FunctionCalls.numberTuplesScanned: " + //ProfileFunctionCalls.numberTuplesScanned);
  }

  def Q9_functional(numRuns: Int) {
    //ProfileFunctionCalls.numberTuplesSelect = 0
    //ProfileFunctionCalls.numberTuplesFiltered = 0
    //ProfileFunctionCalls.numberTuplesScanned = 0
    val partTable = loadPart()
    val nationTable = loadNation()
    val ordersTable = loadOrders()
    val partsuppTable = loadPartsupp()
    val supplierTable = loadSupplier()
    val lineitemTable = loadLineitem()
    for (i <- 0 until numRuns) {
      runQuery({
        val ghost = parseString("ghost")
        val soNation = Query(nationTable)
        val soSupplier = Query(supplierTable)
        val soLineitem = Query(lineitemTable)
        val soPart = Query(partTable).filter(x => x.P_NAME.containsSlice(ghost))
        val soPartsupp = Query(partsuppTable)
        val soOrders = Query(ordersTable)
        val hj1 = soPart.hashJoin(soLineitem)(x => x.P_PARTKEY)(x => x.L_PARTKEY)((x, y) => x.P_PARTKEY == y.L_PARTKEY)
        val hj2 = soSupplier.hashJoin(hj1)(x => x.S_SUPPKEY)(x => x.L_SUPPKEY[Int])((x, y) => x.S_SUPPKEY == y.L_SUPPKEY[Int])
        val hj3 = soNation.hashJoin(hj2)(x => x.N_NATIONKEY)(x => x.S_NATIONKEY[Int])((x, y) => x.N_NATIONKEY == y.S_NATIONKEY[Int])
        val hj4 = soPartsupp.hashJoin(hj3)(x => x.PS_PARTKEY)(x => x.L_PARTKEY[Int])((x, y) => x.PS_PARTKEY == y.L_PARTKEY[Int] && x.PS_SUPPKEY == y.L_SUPPKEY[Int])
        val hj5 = soOrders.hashJoin(hj4)(x => x.O_ORDERKEY)(x => x.L_ORDERKEY[Int])((x, y) => x.O_ORDERKEY == y.L_ORDERKEY[Int])
        val aggOp = hj5.groupBy(x => new Q9GRPRecord(x.N_NAME[OptimalString], dateToYear(x.O_ORDERDATE[Int]))).mapValues(_.map(t =>
          ((t.L_EXTENDEDPRICE[Double] * (1.0 - t.L_DISCOUNT[Double]))) - ((1.0 * t.PS_SUPPLYCOST[Double]) * t.L_QUANTITY[Double])).sum)
        val sortOp = aggOp.sortBy(x => (x._1.NATION.string, -x._1.O_YEAR))
        sortOp.printRows(kv => printf("%s|%d|%.4f\n", kv._1.NATION.string, kv._1.O_YEAR, kv._2), -1)
      })
    }
  }

  // adapted from http://www.qdpma.com/tpch/TPCH100_Query_plans.html
  @dontLift def Q9_functional_p2(numRuns: Int) {
    //ProfileFunctionCalls.numberTuplesSelect = 0
    //ProfileFunctionCalls.numberTuplesFiltered = 0
    //ProfileFunctionCalls.numberTuplesScanned = 0
    val partTable = loadPart()
    val nationTable = loadNation()
    val ordersTable = loadOrders()
    val partsuppTable = loadPartsupp()
    val supplierTable = loadSupplier()
    val lineitemTable = loadLineitem()
    for (i <- 0 until numRuns) {
      runQuery({
        val ghost = parseString("ghost")
        val soNation = Query(nationTable)
        val soSupplier = Query(supplierTable)
        val soLineitem = Query(lineitemTable)
        val soPart = Query(partTable).filter(x => x.P_NAME.containsSlice(ghost))
        val soPartsupp = Query(partsuppTable)
        val soOrders = Query(ordersTable)
        val hj1 = soLineitem.hashJoin(soPart)(x => x.L_PARTKEY)(x => x.P_PARTKEY)((x, y) => x.L_PARTKEY == y.P_PARTKEY)
        val hj2 = hj1.hashJoin(soSupplier)(x => x.L_SUPPKEY[Int])(x => x.S_SUPPKEY)((x, y) => x.L_SUPPKEY[Int] == y.S_SUPPKEY)
        val hj3 = hj2.hashJoin(soNation)(x => x.S_NATIONKEY[Int])(x => x.N_NATIONKEY)((x, y) => x.S_NATIONKEY[Int] == y.N_NATIONKEY)
        val shj3 = hj3.sortBy(x => x.L_ORDERKEY[Int])
        val mj4 = soOrders.mergeJoin(shj3)((x, y) => x.O_ORDERKEY - y.L_ORDERKEY[Int])((x, y) => x.O_ORDERKEY == y.L_ORDERKEY[Int])
        val smj4 = mj4.sortBy(x => (x.L_PARTKEY[Int], x.L_SUPPKEY[Int]))
        val sortedPartsupp = soPartsupp.sortBy(x => (x.PS_PARTKEY, x.PS_SUPPKEY))
        val mj5 = sortedPartsupp.mergeJoin(smj4)((x, y) => {
          val pk = x.PS_PARTKEY - y.L_PARTKEY[Int]
          if (pk == 0)
            x.PS_SUPPKEY - y.L_SUPPKEY[Int]
          else
            pk
        })((x, y) => x.PS_PARTKEY == y.L_PARTKEY[Int] && x.PS_SUPPKEY == y.L_SUPPKEY[Int])
        val aggOp = mj5.groupBy(x => new Q9GRPRecord(x.N_NAME[OptimalString], dateToYear(x.O_ORDERDATE[Int]))).mapValues(_.map(t =>
          ((t.L_EXTENDEDPRICE[Double] * (1.0 - t.L_DISCOUNT[Double]))) - ((1.0 * t.PS_SUPPLYCOST[Double]) * t.L_QUANTITY[Double])).sum)
        val sortOp = aggOp.sortBy(x => (x._1.NATION.string, -x._1.O_YEAR))
        sortOp.printRows(kv => printf("%s|%d|%.4f\n", kv._1.NATION.string, kv._1.O_YEAR, kv._2), -1)
      })
    }
  }

  def Q10(numRuns: Int) {
    //ProfileFunctionCalls.numberTuplesSelect = 0
    //ProfileFunctionCalls.numberTuplesFiltered = 0
    //ProfileFunctionCalls.numberTuplesScanned = 0
    val lineitemTable = loadLineitem()
    val nationTable = loadNation()
    val customerTable = loadCustomer()
    val ordersTable = loadOrders()
    for (i <- 0 until numRuns) {
      runQuery({
        val constantDate1 = parseDate("1994-11-01")
        val constantDate2 = parseDate("1995-02-01")
        val so1 = new SelectOp(new ScanOp(ordersTable))(x => x.O_ORDERDATE >= constantDate1 && x.O_ORDERDATE < constantDate2)
        val so2 = new ScanOp(customerTable)
        // val hj1 = new HashJoinOp(so1, so2)((x, y) => x.O_CUSTKEY == y.C_CUSTKEY)(x => x.O_CUSTKEY)(x => x.C_CUSTKEY)
        // val hj1 = new HashJoinOp(so2, so1)((x, y) => x.C_CUSTKEY == y.O_CUSTKEY)(x => x.C_CUSTKEY)(x => x.O_CUSTKEY)
        val so3 = new ScanOp(nationTable)
        // val hj2 = new HashJoinOp(so3, hj1)((x, y) => x.N_NATIONKEY == y.C_NATIONKEY[Int])(x => x.N_NATIONKEY)(x => x.C_NATIONKEY[Int])
        val so4 = new SelectOp(new ScanOp(lineitemTable))(x => x.L_RETURNFLAG == 'R')
        // val hj3 = new HashJoinOp(hj2, so4)((x, y) => x.O_ORDERKEY[Int] == y.L_ORDERKEY)(x => x.O_ORDERKEY[Int])(x => x.L_ORDERKEY)
        val hj1 = new HashJoinOp(so4, so1)((x, y) => x.L_ORDERKEY == y.O_ORDERKEY)(x => x.L_ORDERKEY)(x => x.O_ORDERKEY)
        val hj2 = new HashJoinOp(so2, hj1)((x, y) => x.C_CUSTKEY == y.O_CUSTKEY[Int])(x => x.C_CUSTKEY)(x => x.O_CUSTKEY[Int])
        val hj3 = new HashJoinOp(so3, hj2)((x, y) => x.N_NATIONKEY == y.C_NATIONKEY[Int])(x => x.N_NATIONKEY)(x => x.C_NATIONKEY[Int])
        val aggOp = new AggOp(hj3, 1)(x => new Q10GRPRecord(x.C_CUSTKEY[Int],
          x.C_NAME[OptimalString], x.C_ACCTBAL[Double],
          x.C_PHONE[OptimalString], x.N_NAME[OptimalString],
          x.C_ADDRESS[OptimalString], x.C_COMMENT[OptimalString]))((t, currAgg) => { currAgg + (t.L_EXTENDEDPRICE[Double] * (1.0 - t.L_DISCOUNT[Double])).asInstanceOf[Double] })
        val sortOp = new SortOp(aggOp)((kv1, kv2) => {
          val k1 = kv1.aggs(0); val k2 = kv2.aggs(0)
          if (k1 < k2) 1
          else if (k1 > k2) -1
          else 0
        })
        val po = new PrintOp(sortOp)(kv => {
          printf("%d|%s|%.4f|%.2f|%s|%s|%s|%s\n", kv.key.C_CUSTKEY, kv.key.C_NAME.string, kv.aggs(0),
            kv.key.C_ACCTBAL, kv.key.N_NAME.string, kv.key.C_ADDRESS.string, kv.key.C_PHONE.string,
            kv.key.C_COMMENT.string)
        }, 20)
        po.run()
        ()
      })
    }
    //FunctionCalls.numberTuplesSelect: " + //ProfileFunctionCalls.numberTuplesSelect)
    //FunctionCalls.numberTuplesFiltered: " + //ProfileFunctionCalls.numberTuplesFiltered)
    //FunctionCalls.numberTuplesScanned: " + //ProfileFunctionCalls.numberTuplesScanned);
  }

  def Q10_functional(numRuns: Int) {
    //ProfileFunctionCalls.numberTuplesSelect = 0
    //ProfileFunctionCalls.numberTuplesFiltered = 0
    //ProfileFunctionCalls.numberTuplesScanned = 0
    val lineitemTable = loadLineitem()
    val nationTable = loadNation()
    val customerTable = loadCustomer()
    val ordersTable = loadOrders()
    for (i <- 0 until numRuns) {
      runQuery({
        val constantDate1 = parseDate("1994-11-01")
        val constantDate2 = parseDate("1995-02-01")
        val so1 = Query(ordersTable).filter(x => x.O_ORDERDATE >= constantDate1 && x.O_ORDERDATE < constantDate2)
        val so2 = Query(customerTable)
        val so3 = Query(nationTable)
        val so4 = Query(lineitemTable).filter(x => x.L_RETURNFLAG == 'R')
        val hj1 = so1.hashJoin(so4)(x => x.O_ORDERKEY)(x => x.L_ORDERKEY)((x, y) => x.O_ORDERKEY == y.L_ORDERKEY)
        val hj2 = so2.hashJoin(hj1)(x => x.C_CUSTKEY)(x => x.O_CUSTKEY[Int])((x, y) => x.C_CUSTKEY == y.O_CUSTKEY[Int])
        val hj3 = so3.hashJoin(hj2)(x => x.N_NATIONKEY)(x => x.C_NATIONKEY[Int])((x, y) => x.N_NATIONKEY == y.C_NATIONKEY[Int])
        val aggOp = hj3.groupBy(x => new Q10GRPRecord(x.C_CUSTKEY[Int],
          x.C_NAME[OptimalString], x.C_ACCTBAL[Double],
          x.C_PHONE[OptimalString], x.N_NAME[OptimalString],
          x.C_ADDRESS[OptimalString], x.C_COMMENT[OptimalString])).mapValues(list => list.map(t => t.L_EXTENDEDPRICE[Double] * (1.0 - t.L_DISCOUNT[Double])).sum)
        val sortOp = aggOp.sortBy(-_._2)
        sortOp.take(20).printRows(kv => {
          printf("%d|%s|%.4f|%.2f|%s|%s|%s|%s\n", kv._1.C_CUSTKEY, kv._1.C_NAME.string, kv._2,
            kv._1.C_ACCTBAL, kv._1.N_NAME.string, kv._1.C_ADDRESS.string, kv._1.C_PHONE.string,
            kv._1.C_COMMENT.string)
        }, -1)
        ()
      })
    }
  }

  def Q11(numRuns: Int) {
    //ProfileFunctionCalls.numberTuplesSelect = 0
    //ProfileFunctionCalls.numberTuplesFiltered = 0
    //ProfileFunctionCalls.numberTuplesScanned = 0
    val partsuppTable = loadPartsupp()
    val supplierTable = loadSupplier()
    val nationTable = loadNation()
    for (i <- 0 until numRuns) {
      runQuery({
        val uk = parseString("UNITED KINGDOM")
        val scanSupplier = new ScanOp(supplierTable)
        val scanNation = new SelectOp(new ScanOp(nationTable))(x => x.N_NAME === uk)
        val jo1 = new HashJoinOp(scanNation, scanSupplier)((x, y) => x.N_NATIONKEY == y.S_NATIONKEY)(x => x.N_NATIONKEY)(x => x.S_NATIONKEY)
        val scanPartsupp = new ScanOp(partsuppTable)
        val jo2 = new HashJoinOp(jo1, scanPartsupp)((x, y) => x.S_SUPPKEY[Int] == y.PS_SUPPKEY)(x => x.S_SUPPKEY[Int])(x => x.PS_SUPPKEY)
        val wo = new WindowOp(jo2)(x => x.PS_PARTKEY[Int])(x => {
          x.foldLeft(0.0) { (cnt, e) => cnt + (e.PS_SUPPLYCOST[Double] * e.PS_AVAILQTY[Int]) }
        })
        wo.open
        val vo = new ViewOp(wo)
        // Calculate total sum
        val aggOp = new AggOp(vo, 1)(x => "Total")((t, currAgg) => currAgg + t.wnd)
        val total = new SubquerySingleResult(aggOp).getResult
        // Calculate final result
        vo.reset
        val so = new SelectOp(vo)(x => { total.key; x.wnd > (total.aggs(0) * 0.0001) })
        val sortOp = new SortOp(so)((x, y) => {
          if (x.wnd > y.wnd) -1
          else if (x.wnd < y.wnd) 1
          else 0
        })
        val po = new PrintOp(sortOp)(kv => printf("%d|%.2f\n", kv.key, kv.wnd), -1)
        po.run()
        ()
      })
    }
    //FunctionCalls.numberTuplesSelect: " + //ProfileFunctionCalls.numberTuplesSelect)
    //FunctionCalls.numberTuplesFiltered: " + //ProfileFunctionCalls.numberTuplesFiltered)
    //FunctionCalls.numberTuplesScanned: " + //ProfileFunctionCalls.numberTuplesScanned);
  }

  def Q11_functional(numRuns: Int) {
    //ProfileFunctionCalls.numberTuplesSelect = 0
    //ProfileFunctionCalls.numberTuplesFiltered = 0
    //ProfileFunctionCalls.numberTuplesScanned = 0
    val partsuppTable = loadPartsupp()
    val supplierTable = loadSupplier()
    val nationTable = loadNation()
    for (i <- 0 until numRuns) {
      runQuery({
        val uk = parseString("UNITED KINGDOM")
        val scanSupplier = Query(supplierTable)
        val scanNation = Query(nationTable).filter(x => x.N_NAME === uk)
        val jo1 = scanNation.hashJoin(scanSupplier)(x => x.N_NATIONKEY)(x => x.S_NATIONKEY)((x, y) => x.N_NATIONKEY == y.S_NATIONKEY)
        val scanPartsupp = Query(partsuppTable)
        val jo2 = jo1.hashJoin(scanPartsupp)(x => x.S_SUPPKEY[Int])(x => x.PS_SUPPKEY)((x, y) => x.S_SUPPKEY[Int] == y.PS_SUPPKEY)
        val wo = jo2.groupBy(x => x.PS_PARTKEY[Int]).mapValues(list => list.map(e => (e.PS_SUPPLYCOST[Double] * e.PS_AVAILQTY[Int])).sum)
          .materialize
        val total = wo.map(_._2).sum
        val so = wo.filter(_._2 > total * 0.0001)
        val sortOp = so.sortBy(-_._2)
        sortOp.printRows(kv => printf("%d|%.2f\n", kv._1, kv._2), -1)
      })
    }
  }

  def Q12(numRuns: Int) {
    //ProfileFunctionCalls.numberTuplesSelect = 0
    //ProfileFunctionCalls.numberTuplesFiltered = 0
    //ProfileFunctionCalls.numberTuplesScanned = 0
    val lineitemTable = loadLineitem()
    val ordersTable = loadOrders()
    for (i <- 0 until numRuns) {
      runQuery({
        val mail = parseString("MAIL")
        val ship = parseString("SHIP")
        val constantDate = parseDate("1995-01-01")
        val constantDate2 = parseDate("1994-01-01")
        val so1 = new ScanOp(ordersTable)
        val so2 = new SelectOp(new ScanOp(lineitemTable))(x =>
          x.L_RECEIPTDATE < constantDate && x.L_COMMITDATE < constantDate && x.L_SHIPDATE < constantDate && x.L_SHIPDATE < x.L_COMMITDATE && x.L_COMMITDATE < x.L_RECEIPTDATE && x.L_RECEIPTDATE >= constantDate2 && (x.L_SHIPMODE === mail || x.L_SHIPMODE === ship))
        val jo = new HashJoinOp(so1, so2)((x, y) => x.O_ORDERKEY == y.L_ORDERKEY)(x => x.O_ORDERKEY)(x => x.L_ORDERKEY)
        val URGENT = parseString("1-URGENT")
        val HIGH = parseString("2-HIGH")
        val aggOp = new AggOp(jo, 2)(x => x.L_SHIPMODE[OptimalString])(
          (t, currAgg) => { if (t.O_ORDERPRIORITY[OptimalString] === URGENT || t.O_ORDERPRIORITY[OptimalString] === HIGH) currAgg + 1 else currAgg },

          (t, currAgg) => { if (t.O_ORDERPRIORITY[OptimalString] =!= URGENT && t.O_ORDERPRIORITY[OptimalString] =!= HIGH) currAgg + 1 else currAgg })
        val sortOp = new SortOp(aggOp)((x, y) => x.key diff y.key)
        val po = new PrintOp(sortOp)(kv => printf("%s|%.0f|%.0f\n", kv.key.string, kv.aggs(0), kv.aggs(1)), -1)
        po.run()
        ()
      })
    }
    //FunctionCalls.numberTuplesSelect: " + //ProfileFunctionCalls.numberTuplesSelect)
    //FunctionCalls.numberTuplesFiltered: " + //ProfileFunctionCalls.numberTuplesFiltered)
    //FunctionCalls.numberTuplesScanned: " + //ProfileFunctionCalls.numberTuplesScanned);
  }

  def Q12_functional(numRuns: Int) {
    //ProfileFunctionCalls.numberTuplesSelect = 0
    //ProfileFunctionCalls.numberTuplesFiltered = 0
    //ProfileFunctionCalls.numberTuplesScanned = 0
    val lineitemTable = loadLineitem()
    val ordersTable = loadOrders()
    for (i <- 0 until numRuns) {
      runQuery({
        val mail = parseString("MAIL")
        val ship = parseString("SHIP")
        val constantDate = parseDate("1995-01-01")
        val constantDate2 = parseDate("1994-01-01")
        val so2 = Query(lineitemTable).filter(x =>
          x.L_RECEIPTDATE < constantDate && x.L_COMMITDATE < constantDate && x.L_SHIPDATE < constantDate && x.L_SHIPDATE < x.L_COMMITDATE && x.L_COMMITDATE < x.L_RECEIPTDATE && x.L_RECEIPTDATE >= constantDate2 && (x.L_SHIPMODE === mail || x.L_SHIPMODE === ship))
        val jo = Query(ordersTable).hashJoin(so2)(x => x.O_ORDERKEY)(x => x.L_ORDERKEY)((x, y) => x.O_ORDERKEY == y.L_ORDERKEY)
        val URGENT = parseString("1-URGENT")
        val HIGH = parseString("2-HIGH")
        val aggOp = jo.groupBy(x => x.L_SHIPMODE[OptimalString]).mapValues(list => {
          Array(list.filter(t => t.O_ORDERPRIORITY[OptimalString] === URGENT || t.O_ORDERPRIORITY[OptimalString] === HIGH).count,
            list.filter(t => t.O_ORDERPRIORITY[OptimalString] =!= URGENT && t.O_ORDERPRIORITY[OptimalString] =!= HIGH).count)
          // var x0 = 0
          // var x1 = 0
          // list.foreach { t =>
          //   if (t.O_ORDERPRIORITY[OptimalString] === URGENT || t.O_ORDERPRIORITY[OptimalString] === HIGH)
          //     x0 += 1
          //   if (t.O_ORDERPRIORITY[OptimalString] =!= URGENT && t.O_ORDERPRIORITY[OptimalString] =!= HIGH)
          //     x1 += 1
          // }
          // Array(x0, x1)
        })
        val sortOp = aggOp.sortBy(_._1.string)
        sortOp.printRows(kv =>
          printf("%s|%d|%d\n", kv._1.string, kv._2(0), kv._2(1)), -1)
      })
    }
  }

  def Q12_functional_p2(numRuns: Int) {
    //ProfileFunctionCalls.numberTuplesSelect = 0
    //ProfileFunctionCalls.numberTuplesFiltered = 0
    //ProfileFunctionCalls.numberTuplesScanned = 0
    val lineitemTable = loadLineitem()
    val ordersTable = loadOrders()
    for (i <- 0 until numRuns) {
      runQuery({
        val mail = parseString("MAIL")
        val ship = parseString("SHIP")
        val constantDate = parseDate("1995-01-01")
        val constantDate2 = parseDate("1994-01-01")
        val so2 = Query(lineitemTable).filter(x =>
          x.L_RECEIPTDATE < constantDate && x.L_COMMITDATE < constantDate && x.L_SHIPDATE < constantDate && x.L_SHIPDATE < x.L_COMMITDATE && x.L_COMMITDATE < x.L_RECEIPTDATE && x.L_RECEIPTDATE >= constantDate2 && (x.L_SHIPMODE === mail || x.L_SHIPMODE === ship))
        // val jo = ordersTable.hashJoin(so2)(x => x.O_ORDERKEY)(x => x.L_ORDERKEY)((x, y) => x.O_ORDERKEY == y.L_ORDERKEY)
        val jo = Query(ordersTable).mergeJoin(so2)((x, y) => x.O_ORDERKEY - y.L_ORDERKEY)((x, y) => x.O_ORDERKEY == y.L_ORDERKEY)
        val URGENT = parseString("1-URGENT")
        val HIGH = parseString("2-HIGH")
        val aggOp = jo.groupBy(x => x.L_SHIPMODE[OptimalString]).mapValues(list => {
          // Array(list.filter(t => t.O_ORDERPRIORITY[OptimalString] === URGENT || t.O_ORDERPRIORITY[OptimalString] === HIGH).count,
          //   list.filter(t => t.O_ORDERPRIORITY[OptimalString] =!= URGENT && t.O_ORDERPRIORITY[OptimalString] =!= HIGH).count)
          var x0 = 0
          var x1 = 0
          list.foreach { t =>
            if (t.O_ORDERPRIORITY[OptimalString] === URGENT || t.O_ORDERPRIORITY[OptimalString] === HIGH)
              x0 += 1
            if (t.O_ORDERPRIORITY[OptimalString] =!= URGENT && t.O_ORDERPRIORITY[OptimalString] =!= HIGH)
              x1 += 1
          }
          Array(x0, x1)
        })
        val sortOp = aggOp.sortBy(_._1.string)
        sortOp.printRows(kv =>
          printf("%s|%d|%d\n", kv._1.string, kv._2(0), kv._2(1)), -1)
      })
    }
  }

  /*def Q13(numRuns: Int) {
    val customerTable = loadCustomer()
    val ordersTable = loadOrders()
    for (i <- 0 until numRuns) {
      runQuery({
        val unusual = parseString("unusual")
        val packages = parseString("packages")
        val scanCustomer = new ScanOp(customerTable)
        val scanOrders = new SelectOp(new ScanOp(ordersTable))(x => {
          val idxu = x.O_COMMENT.indexOfSlice(unusual, 0)
          val idxp = x.O_COMMENT.indexOfSlice(packages, idxu)
          !(idxu != -1 && idxp != -1)
        })
        val jo = new LeftOuterJoinOp(scanCustomer, scanOrders)((x, y) => x.C_CUSTKEY == y.O_CUSTKEY)(x => x.C_CUSTKEY)(x => x.O_CUSTKEY)
        val aggOp1 = new AggOp(jo, 1)(x => x.C_CUSTKEY[Int])(
          (t, currAgg) => { if (t.O_ORDERKEY != 0.0) currAgg + 1 else currAgg })
        val aggOp2 = new AggOp(aggOp1, 1)(x => x.aggs(0))(
          (t, currAgg) => { currAgg + 1 })
        val sortOp = new SortOp(aggOp2)((x, y) => {
          if (x.aggs(0) < y.aggs(0)) 1
          else if (x.aggs(0) > y.aggs(0)) -1
          else {
            if (x.key < y.key) 1
            else if (x.key > y.key) -1
            else 0
          }
        })
        val po = new PrintOp(sortOp)(kv => printf("%.0f|%.0f\n", kv.key, kv.aggs(0)), -1)
        po.run()
        ()
      })
    }
  }*/

  def Q13(numRuns: Int) {
    val customerTable = loadCustomer()
    val ordersTable = loadOrders()
    val aggArray = new Array[Q13IntRecord](customerTable.length)
    for (i <- 0 until customerTable.length) {
      aggArray(i) = new Q13IntRecord(0)
    }
    for (i <- 0 until numRuns) {
      runQuery({
        val unusual = parseString("unusual")
        val packages = parseString("packages")
        val scanOrders = new SelectOp(new ScanOp(ordersTable))(x => {
          val idxu = x.O_COMMENT.indexOfSlice(unusual, 0)
          val idxp = x.O_COMMENT.indexOfSlice(packages, idxu)
          !(idxu != -1 && idxp != -1)
        })
        val aggOp1 = new MapOp(scanOrders)(t => aggArray(t.O_CUSTKEY).count += 1)
        aggOp1.open
        aggOp1.next
        val aggScan = new ScanOp(aggArray)
        val aggOp2 = new AggOp(aggScan, 1)(x => x.count)(
          (t, currAgg) => { currAgg + 1 })
        val sortOp = new SortOp(aggOp2)((x, y) => {
          if (x.aggs(0) < y.aggs(0)) 1
          else if (x.aggs(0) > y.aggs(0)) -1
          else {
            if (x.key < y.key) 1
            else if (x.key > y.key) -1
            else 0
          }
        })
        val po = new PrintOp(sortOp)(kv => printf("%d|%.0f\n", kv.key, kv.aggs(0)), -1)
        po.open
        po.next
        ()
      })
    }
  }

  def Q14(numRuns: Int) {
    //ProfileFunctionCalls.numberTuplesSelect = 0
    //ProfileFunctionCalls.numberTuplesFiltered = 0
    //ProfileFunctionCalls.numberTuplesScanned = 0
    val lineitemTable = loadLineitem()
    val partTable = loadPart()
    for (i <- 0 until numRuns) {
      runQuery({
        val promo = parseString("PROMO")
        val constantDate = parseDate("1994-04-01")
        val constantDate2 = parseDate("1994-03-01")
        val so1 = new ScanOp(partTable)
        val so2 = new SelectOp(new ScanOp(lineitemTable))(x => x.L_SHIPDATE >= constantDate2 && x.L_SHIPDATE < constantDate)
        val jo = new HashJoinOp(so1, so2)((x, y) => x.P_PARTKEY == y.L_PARTKEY)(x => x.P_PARTKEY)(x => x.L_PARTKEY)
        val aggOp = new AggOp(jo, 3)(x => "Total")(
          (t, currAgg) => {
            if (t.P_TYPE[OptimalString] startsWith promo)
              currAgg + (t.L_EXTENDEDPRICE[Double] * (1.0 - t.L_DISCOUNT[Double]))
            else currAgg
          },
          (t, currAgg) => { currAgg + (t.L_EXTENDEDPRICE[Double] * (1.0 - t.L_DISCOUNT[Double])) })
        val mapOp = new MapOp(aggOp)(kv => { kv.key; kv.aggs(2) = (kv.aggs(0) * 100) / kv.aggs(1) })
        val po = new PrintOp(mapOp)(kv => printf("%.4f\n", kv.aggs(2)), -1)
        po.run()
        ()
      })
    }
    //FunctionCalls.numberTuplesSelect: " + //ProfileFunctionCalls.numberTuplesSelect)
    //FunctionCalls.numberTuplesFiltered: " + //ProfileFunctionCalls.numberTuplesFiltered)
    //FunctionCalls.numberTuplesScanned: " + //ProfileFunctionCalls.numberTuplesScanned);
  }

  def Q14_functional(numRuns: Int) {
    //ProfileFunctionCalls.numberTuplesSelect = 0
    //ProfileFunctionCalls.numberTuplesFiltered = 0
    //ProfileFunctionCalls.numberTuplesScanned = 0
    val lineitemTable = loadLineitem()
    val partTable = loadPart()
    for (i <- 0 until numRuns) {
      runQuery({
        val promo = parseString("PROMO")
        val constantDate = parseDate("1994-04-01")
        val constantDate2 = parseDate("1994-03-01")
        val joinResult = Query(partTable)
          .hashJoin(Query(lineitemTable)
            .filter(x => x.L_SHIPDATE >= constantDate2 && x.L_SHIPDATE < constantDate))(_.P_PARTKEY)(_.L_PARTKEY)((x, y) => x.P_PARTKEY == y.L_PARTKEY)
        val agg1 = joinResult.filter(_.P_TYPE[OptimalString].startsWith(promo)).map(t => (t.L_EXTENDEDPRICE[Double] * (1.0 - t.L_DISCOUNT[Double]))).sum
        val agg2 = joinResult.map(t => (t.L_EXTENDEDPRICE[Double] * (1.0 - t.L_DISCOUNT[Double]))).sum
        // val (agg1, agg2) = joinResult.foldLeft(0.0, 0.0)((acc, t) => {
        //   val e = t.L_EXTENDEDPRICE[Double] * (1.0 - t.L_DISCOUNT[Double])
        //   val x = if (t.P_TYPE[OptimalString].startsWith(promo)) e else 0.0
        //   (acc._1 + x, acc._2 + e)
        // })
        val result = agg1 * 100 / agg2
        printf("%.4f\n", result)
        printf("(%d rows)\n", 1)
        ()
      })
    }
  }

  def Q15(numRuns: Int) {
    //ProfileFunctionCalls.numberTuplesSelect = 0
    //ProfileFunctionCalls.numberTuplesFiltered = 0
    //ProfileFunctionCalls.numberTuplesScanned = 0
    val lineitemTable = loadLineitem()
    val supplierTable = loadSupplier()
    for (i <- 0 until numRuns) {
      runQuery({
        val constantDate = parseDate("1993-09-01")
        val constantDate2 = parseDate("1993-12-01")
        val scanLineitem = new SelectOp(new ScanOp(lineitemTable))(x => x.L_SHIPDATE >= constantDate && x.L_SHIPDATE < constantDate2)
        val aggOp1 = new AggOp(scanLineitem, 1)(x => x.L_SUPPKEY)(
          (t, currAgg) => { currAgg + (t.L_EXTENDEDPRICE * (1.0 - t.L_DISCOUNT)) })
        // Create view
        aggOp1.open
        val vo = new ViewOp(aggOp1)
        // Get max
        val aggOp2 = new AggOp(vo, 1)(x => "MAXREVENUE")(
          (t, currAgg) => { t.key; if (currAgg < t.aggs(0)) t.aggs(0) else currAgg })
        aggOp2.open
        val maxRevenue = new SubquerySingleResult(aggOp2).getResult
        vo.reset
        // Calcuate result
        val scanSupplier = new ScanOp(supplierTable)
        val jo = new HashJoinOp(scanSupplier, vo)((x, y) => { maxRevenue.key; x.S_SUPPKEY == y.key && y.aggs(0) == maxRevenue.aggs(0) })(x => x.S_SUPPKEY)(x => x.key)
        val po = new PrintOp(jo)(kv => printf("%d|%s|%s|%s|%.4f\n", kv.S_SUPPKEY[Int], kv.S_NAME[OptimalString].string, kv.S_ADDRESS[OptimalString].string, kv.S_PHONE[OptimalString].string, kv.aggs[Array[Double]].apply(0)), -1)
        po.run()
        ()
      })
    }
    //FunctionCalls.numberTuplesSelect: " + //ProfileFunctionCalls.numberTuplesSelect)
    //FunctionCalls.numberTuplesFiltered: " + //ProfileFunctionCalls.numberTuplesFiltered)
    //FunctionCalls.numberTuplesScanned: " + //ProfileFunctionCalls.numberTuplesScanned);
  }

  def Q16(numRuns: Int) {
    //ProfileFunctionCalls.numberTuplesSelect = 0
    //ProfileFunctionCalls.numberTuplesFiltered = 0
    //ProfileFunctionCalls.numberTuplesScanned = 0
    val supplierTable = loadSupplier()
    val partTable = loadPart()
    val partsuppTable = loadPartsupp()
    for (i <- 0 until numRuns) {
      runQuery({
        val str1 = parseString("Customer")
        val str2 = parseString("Complaints")
        val brand21 = parseString("Brand#21")
        val promoPlated = parseString("PROMO PLATED")
        val partScan = new SelectOp(new ScanOp(partTable))(x => !(x.P_BRAND startsWith brand21) && !(x.P_TYPE startsWith promoPlated) &&
          (x.P_SIZE == 23 || x.P_SIZE == 3 || x.P_SIZE == 33 || x.P_SIZE == 29 ||
            x.P_SIZE == 40 || x.P_SIZE == 27 || x.P_SIZE == 22 || x.P_SIZE == 4))
        val partsuppScan = new ScanOp(partsuppTable)
        val supplierScan = new SelectOp(new ScanOp(supplierTable))(x => {
          val idxu = x.S_COMMENT.indexOfSlice(str1, 0)
          val idxp = x.S_COMMENT.indexOfSlice(str2, idxu)
          idxu != -1 && idxp != -1
        })
        val jo1 = new HashJoinOp(partScan, partsuppScan)((x, y) => x.P_PARTKEY == y.PS_PARTKEY)(x => x.P_PARTKEY)(x => x.PS_PARTKEY)
        val jo2 = new HashJoinAnti(jo1, supplierScan)((x, y) => x.PS_SUPPKEY[Int] == y.S_SUPPKEY)(x => x.PS_SUPPKEY[Int])(x => x.S_SUPPKEY)
        val aggOp = new AggOp(jo2, 1)(x => { x.PS_SUPPKEY[Int]; new Q16GRPRecord1(x.P_BRAND[OptimalString], x.P_TYPE[OptimalString], x.P_SIZE[Int], x.PS_SUPPKEY[Int]) })((t, currAgg) => currAgg)
        val aggOp2 = new AggOp(aggOp, 1)(x => { x.key.PS_SUPPKEY; new Q16GRPRecord2(x.key.P_BRAND, x.key.P_TYPE, x.key.P_SIZE) })(
          (t, currAgg) => currAgg + 1)
        val sortOp = new SortOp(aggOp2)((x, y) => {
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
        val po = new PrintOp(sortOp)(x => printf("%s|%s|%d|%.0f\n", x.key.P_BRAND.string, x.key.P_TYPE.string, x.key.P_SIZE, x.aggs(0)), -1)
        po.run()
        ()
      })
    }
    //FunctionCalls.numberTuplesSelect: " + //ProfileFunctionCalls.numberTuplesSelect)
    //FunctionCalls.numberTuplesFiltered: " + //ProfileFunctionCalls.numberTuplesFiltered)
    //FunctionCalls.numberTuplesScanned: " + //ProfileFunctionCalls.numberTuplesScanned);
  }

  def Q17(numRuns: Int) {
    //ProfileFunctionCalls.numberTuplesSelect = 0
    //ProfileFunctionCalls.numberTuplesFiltered = 0
    //ProfileFunctionCalls.numberTuplesScanned = 0
    val lineitemTable = loadLineitem()
    val partTable = loadPart()
    for (i <- 0 until numRuns) {
      runQuery({
        val medbag = parseString("MED BAG")
        val brand15 = parseString("Brand#15")
        val scanLineitem = new ScanOp(lineitemTable)
        val scanPart = new SelectOp(new ScanOp(partTable))(x => x.P_CONTAINER === medbag && x.P_BRAND === brand15)
        val jo = new HashJoinOp(scanLineitem, scanPart)((x, y) => x.L_PARTKEY == y.P_PARTKEY)(x => x.L_PARTKEY)(x => x.P_PARTKEY)
        val wo = new WindowOp(jo)(x => x.L_PARTKEY[Int])(x => {
          val sum = x.foldLeft(0.0)((cnt, e) => cnt + e.L_QUANTITY[Double])
          val count = x.size
          val avg = 0.2 * (sum / count)
          x.foldLeft(0.0)((cnt, e) => {
            if (e.L_QUANTITY[Double] < avg) cnt + e.L_EXTENDEDPRICE[Double]
            else cnt
          }) / 7.0
        })
        val aggOp = new AggOp(wo, 1)(x => "Total")((t, currAgg) => currAgg + t.wnd)
        val po = new PrintOp(aggOp)(kv => { kv.key; printf("%.6f\n", kv.aggs(0)) }, -1)
        po.run()
        ()
      })
    }
    //FunctionCalls.numberTuplesSelect: " + //ProfileFunctionCalls.numberTuplesSelect)
    //FunctionCalls.numberTuplesFiltered: " + //ProfileFunctionCalls.numberTuplesFiltered)
    //FunctionCalls.numberTuplesScanned: " + //ProfileFunctionCalls.numberTuplesScanned);
  }

  // Danger, Will Robinson!: Query takes a long time to complete in Scala (but we 
  // knew that already)
  def Q18(numRuns: Int) {
    //ProfileFunctionCalls.numberTuplesSelect = 0
    //ProfileFunctionCalls.numberTuplesFiltered = 0
    //ProfileFunctionCalls.numberTuplesScanned = 0
    val lineitemTable = loadLineitem()
    val ordersTable = loadOrders()
    val customerTable = loadCustomer()
    for (i <- 0 until numRuns) {
      runQuery({
        val scanOrders = new ScanOp(ordersTable)
        val scanCustomer = new ScanOp(customerTable)
        // Group aggregation on Lineitem
        val scanLineitem1 = new ScanOp(lineitemTable)
        val aggOp1 = new SelectOp(new AggOp(scanLineitem1, 1)(x => x.L_ORDERKEY)(
          (t, currAgg) => { currAgg + t.L_QUANTITY }))(x => x.aggs(0) > 300)
        // Hash Join with orders
        val jo1 = new HashJoinOp(aggOp1, scanOrders)((x, y) => { x.aggs; y.O_ORDERKEY == x.key })(x => x.key)(x => x.O_ORDERKEY)
        val jo2 = new HashJoinOp(jo1, scanCustomer)((x, y) => { x.aggs; x.O_CUSTKEY[Int] == y.C_CUSTKEY })(x => x.O_CUSTKEY[Int])(x => x.C_CUSTKEY)
        val aggOp2 = new AggOp(jo2, 1)(x => { x.aggs; new Q18GRPRecord(x.C_NAME[OptimalString], x.C_CUSTKEY[Int], x.O_ORDERKEY[Int], x.O_ORDERDATE[Int], x.O_TOTALPRICE[Double]) })(
          (t, currAgg) => { t.aggs; currAgg + (t.aggs[Array[Double]]).apply(0) } // aggs(0) => L_QUANTITY"
          )
        val sortOp = new SortOp(aggOp2)((kv1, kv2) => {
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
        val po = new PrintOp(sortOp)(kv => {
          kv.aggs;
          printf("%s|%d|%d|%s|%.2f|%.2f\n", kv.key.C_NAME.string, kv.key.C_CUSTKEY, kv.key.O_ORDERKEY, dateToString(kv.key.O_ORDERDATE), kv.key.O_TOTALPRICE, kv.aggs(0))
        }, 100)
        po.run()
        ()
      })
    }
    //FunctionCalls.numberTuplesSelect: " + //ProfileFunctionCalls.numberTuplesSelect)
    //FunctionCalls.numberTuplesFiltered: " + //ProfileFunctionCalls.numberTuplesFiltered)
    //FunctionCalls.numberTuplesScanned: " + //ProfileFunctionCalls.numberTuplesScanned);
  }

  def Q18_functional(numRuns: Int) {
    //ProfileFunctionCalls.numberTuplesSelect = 0
    //ProfileFunctionCalls.numberTuplesFiltered = 0
    //ProfileFunctionCalls.numberTuplesScanned = 0
    val lineitemTable = loadLineitem()
    val ordersTable = loadOrders()
    val customerTable = loadCustomer()
    for (i <- 0 until numRuns) {
      runQuery({
        // Group aggregation on Lineitem
        val aggOp1 = Query(lineitemTable).groupBy(_.L_ORDERKEY).mapValues(_.map(_.L_QUANTITY).sum).filter(_._2 > 300)
          .map(x => DynamicDataRow("AggRec")(("key", x._1), ("agg", x._2)))
        // Hash Join with orders
        val jo1 = aggOp1.hashJoin(Query(ordersTable))(_.key[Int])(_.O_ORDERKEY)(_.key[Int] == _.O_ORDERKEY)
        val jo2 = jo1.hashJoin(Query(customerTable))(_.O_CUSTKEY[Int])(_.C_CUSTKEY)(_.O_CUSTKEY[Int] == _.C_CUSTKEY)
        val aggOp2 = jo2.groupBy(x => new Q18GRPRecord(x.C_NAME[OptimalString], x.C_CUSTKEY[Int], x.O_ORDERKEY[Int], x.O_ORDERDATE[Int], x.O_TOTALPRICE[Double]))
          .mapValues(_.map(_.agg[Double]).sum)
        val sortOp = aggOp2.sortBy(t => (-t._1.O_TOTALPRICE, t._1.O_ORDERDATE))
        sortOp.printRows(kv =>
          printf("%s|%d|%d|%s|%.2f|%.2f\n", kv._1.C_NAME.string, kv._1.C_CUSTKEY, kv._1.O_ORDERKEY, dateToString(kv._1.O_ORDERDATE), kv._1.O_TOTALPRICE, kv._2), 100)
        ()
      })
    }
  }

  def Q19(numRuns: Int) {
    //ProfileFunctionCalls.numberTuplesSelect = 0
    //ProfileFunctionCalls.numberTuplesFiltered = 0
    //ProfileFunctionCalls.numberTuplesScanned = 0
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

        val so1 = new SelectOp(new ScanOp(partTable))(x => x.P_SIZE >= 1 &&
          (x.P_SIZE <= 5 && x.P_BRAND === Brand31 && (x.P_CONTAINER === SMBOX || x.P_CONTAINER === SMCASE ||
            x.P_CONTAINER === SMPACK || x.P_CONTAINER === SMPKG)) ||
            (x.P_SIZE <= 10 && x.P_BRAND === Brand43 && (x.P_CONTAINER === MEDBAG || x.P_CONTAINER === MEDBOX ||
              x.P_CONTAINER === MEDPACK || x.P_CONTAINER === MEDPKG)) ||
              (x.P_SIZE <= 15 && x.P_BRAND === Brand43 && (x.P_CONTAINER === LGBOX || x.P_CONTAINER === LGCASE ||
                x.P_CONTAINER === LGPACK || x.P_CONTAINER === LGPKG)))
        val so2 = new SelectOp(new ScanOp(lineitemTable))(x =>
          ((x.L_QUANTITY <= 36 && x.L_QUANTITY >= 26) || (x.L_QUANTITY <= 25 && x.L_QUANTITY >= 15) ||
            (x.L_QUANTITY <= 14 && x.L_QUANTITY >= 4)) && x.L_SHIPINSTRUCT === DELIVERINPERSON &&
            (x.L_SHIPMODE === AIR || x.L_SHIPMODE === AIRREG))
        val jo = new SelectOp(new HashJoinOp(so2, so1)((x, y) => y.P_PARTKEY == x.L_PARTKEY)(x => x.L_PARTKEY)(x => x.P_PARTKEY))(
          x => x.P_BRAND[OptimalString] === Brand31 &&
            (x.P_CONTAINER[OptimalString] === SMBOX || x.P_CONTAINER[OptimalString] === SMCASE || x.P_CONTAINER[OptimalString] === SMPACK || x.P_CONTAINER[OptimalString] === SMPKG) &&
            x.L_QUANTITY[Double] >= 4 && x.L_QUANTITY[Double] <= 14 && x.P_SIZE[Int] <= 5 || x.P_BRAND[OptimalString] === Brand43 &&
            (x.P_CONTAINER[OptimalString] === MEDBAG || x.P_CONTAINER[OptimalString] === MEDBOX || x.P_CONTAINER[OptimalString] === MEDPACK || x.P_CONTAINER[OptimalString] === MEDPKG) &&
            x.L_QUANTITY[Double] >= 15 && x.L_QUANTITY[Double] <= 25 && x.P_SIZE[Int] <= 10 || x.P_BRAND[OptimalString] === Brand43 &&
            (x.P_CONTAINER[OptimalString] === LGBOX || x.P_CONTAINER[OptimalString] === LGCASE || x.P_CONTAINER[OptimalString] === LGPACK || x.P_CONTAINER[OptimalString] === LGPKG) &&
            x.L_QUANTITY[Double] >= 26 && x.L_QUANTITY[Double] <= 36 && x.P_SIZE[Int] <= 15)
        val aggOp = new AggOp(jo, 1)(x => "Total")(
          (t, currAgg) => { currAgg + (t.L_EXTENDEDPRICE[Double] * (1.0 - t.L_DISCOUNT[Double])) })
        val po = new PrintOp(aggOp)(kv => { kv.key; printf("%.4f\n", kv.aggs(0)) }, -1)
        po.run()
        ()
      })
    }
    //FunctionCalls.numberTuplesSelect: " + //ProfileFunctionCalls.numberTuplesSelect)
    //FunctionCalls.numberTuplesFiltered: " + //ProfileFunctionCalls.numberTuplesFiltered)
    //FunctionCalls.numberTuplesScanned: " + //ProfileFunctionCalls.numberTuplesScanned);
  }

  def Q19_functional(numRuns: Int) {
    //ProfileFunctionCalls.numberTuplesSelect = 0
    //ProfileFunctionCalls.numberTuplesFiltered = 0
    //ProfileFunctionCalls.numberTuplesScanned = 0
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

        val so1 = Query(partTable).filter(x => x.P_SIZE >= 1 &&
          (x.P_SIZE <= 5 && x.P_BRAND === Brand31 && (x.P_CONTAINER === SMBOX || x.P_CONTAINER === SMCASE ||
            x.P_CONTAINER === SMPACK || x.P_CONTAINER === SMPKG)) ||
            (x.P_SIZE <= 10 && x.P_BRAND === Brand43 && (x.P_CONTAINER === MEDBAG || x.P_CONTAINER === MEDBOX ||
              x.P_CONTAINER === MEDPACK || x.P_CONTAINER === MEDPKG)) ||
              (x.P_SIZE <= 15 && x.P_BRAND === Brand43 && (x.P_CONTAINER === LGBOX || x.P_CONTAINER === LGCASE ||
                x.P_CONTAINER === LGPACK || x.P_CONTAINER === LGPKG)))
        val so2 = Query(lineitemTable).filter(x =>
          ((x.L_QUANTITY <= 36 && x.L_QUANTITY >= 26) || (x.L_QUANTITY <= 25 && x.L_QUANTITY >= 15) ||
            (x.L_QUANTITY <= 14 && x.L_QUANTITY >= 4)) && x.L_SHIPINSTRUCT === DELIVERINPERSON &&
            (x.L_SHIPMODE === AIR || x.L_SHIPMODE === AIRREG))
        val jo = so1.hashJoin(so2)(x => x.P_PARTKEY)(x => x.L_PARTKEY)((x, y) => x.P_PARTKEY == y.L_PARTKEY).filter(
          x => x.P_BRAND[OptimalString] === Brand31 &&
            (x.P_CONTAINER[OptimalString] === SMBOX || x.P_CONTAINER[OptimalString] === SMCASE || x.P_CONTAINER[OptimalString] === SMPACK || x.P_CONTAINER[OptimalString] === SMPKG) &&
            x.L_QUANTITY[Double] >= 4 && x.L_QUANTITY[Double] <= 14 && x.P_SIZE[Int] <= 5 || x.P_BRAND[OptimalString] === Brand43 &&
            (x.P_CONTAINER[OptimalString] === MEDBAG || x.P_CONTAINER[OptimalString] === MEDBOX || x.P_CONTAINER[OptimalString] === MEDPACK || x.P_CONTAINER[OptimalString] === MEDPKG) &&
            x.L_QUANTITY[Double] >= 15 && x.L_QUANTITY[Double] <= 25 && x.P_SIZE[Int] <= 10 || x.P_BRAND[OptimalString] === Brand43 &&
            (x.P_CONTAINER[OptimalString] === LGBOX || x.P_CONTAINER[OptimalString] === LGCASE || x.P_CONTAINER[OptimalString] === LGPACK || x.P_CONTAINER[OptimalString] === LGPKG) &&
            x.L_QUANTITY[Double] >= 26 && x.L_QUANTITY[Double] <= 36 && x.P_SIZE[Int] <= 15)
        val result = jo.map(t => (t.L_EXTENDEDPRICE[Double] * (1.0 - t.L_DISCOUNT[Double]))).sum
        printf("%.4f\n", result)
        printf("(%d rows)\n", 1)
      })
    }
  }

  def Q20(numRuns: Int) {
    //ProfileFunctionCalls.numberTuplesSelect = 0
    //ProfileFunctionCalls.numberTuplesFiltered = 0
    //ProfileFunctionCalls.numberTuplesScanned = 0
    val nationTable = loadNation()
    val supplierTable = loadSupplier()
    val partTable = loadPart()
    val partsuppTable = loadPartsupp()
    val lineitemTable = loadLineitem()
    for (i <- 0 until numRuns) {
      runQuery {
        val constantDate1 = parseDate("1996-01-01")
        val constantDate2 = parseDate("1997-01-01")
        val jordan = parseString("JORDAN")
        val azure = parseString("azure")
        val scanPart = new SelectOp(new ScanOp(partTable))(x => x.P_NAME startsWith azure)
        val scanPartsupp = new ScanOp(partsuppTable)
        val scanSupplier = new ScanOp(supplierTable)
        val scanNation = new SelectOp(new ScanOp(nationTable))(x => x.N_NAME === jordan)
        val scanLineitem = new SelectOp(new ScanOp(lineitemTable))(x => x.L_SHIPDATE >= constantDate1 && x.L_SHIPDATE < constantDate2)
        val jo1 = new HashJoinOp(scanPart, scanPartsupp)((x, y) => x.P_PARTKEY == y.PS_PARTKEY)(x => x.P_PARTKEY)(x => x.PS_PARTKEY)
        val jo2 = new HashJoinOp(jo1, scanLineitem)((x, y) => x.PS_PARTKEY[Int] == y.L_PARTKEY && x.PS_SUPPKEY[Int] == y.L_SUPPKEY)(x => x.PS_PARTKEY[Int])(x => x.L_PARTKEY)
        val aggOp = new AggOp(jo2, 1)(x => new Q20GRPRecord(x.PS_PARTKEY[Int], x.PS_SUPPKEY[Int], x.PS_AVAILQTY[Int]))((t, currAgg) => { currAgg + t.L_QUANTITY[Double] })
        val selOp = new SelectOp(aggOp)(x => { x.key.PS_PARTKEY; x.key.PS_SUPPKEY; x.key.PS_AVAILQTY > 0.5 * x.aggs(0) })
        val jo3 = new HashJoinOp(selOp, scanSupplier)((x, y) => x.key.PS_SUPPKEY == y.S_SUPPKEY)(x => x.key.PS_SUPPKEY)(x => x.S_SUPPKEY)
        val jo4 = new HashJoinOp(scanNation, jo3)((x, y) => x.N_NATIONKEY == y.S_NATIONKEY[Int])(x => x.N_NATIONKEY)(x => x.S_NATIONKEY[Int])
        val sortOp = new SortOp(jo4)((x, y) => {
          x.S_NAME[OptimalString] diff y.S_NAME[OptimalString]
        })
        val po = new PrintOp(sortOp)(kv => printf("%s|%s\n", kv.S_NAME[OptimalString].string, kv.S_ADDRESS[OptimalString].string), -1)
        po.run()
        ()
      }
    }
    //FunctionCalls.numberTuplesSelect: " + //ProfileFunctionCalls.numberTuplesSelect)
    //FunctionCalls.numberTuplesFiltered: " + //ProfileFunctionCalls.numberTuplesFiltered)
    //FunctionCalls.numberTuplesScanned: " + //ProfileFunctionCalls.numberTuplesScanned);
  }

  def Q20_functional(numRuns: Int) {
    //ProfileFunctionCalls.numberTuplesSelect = 0
    //ProfileFunctionCalls.numberTuplesFiltered = 0
    //ProfileFunctionCalls.numberTuplesScanned = 0
    val nationTable = loadNation()
    val supplierTable = loadSupplier()
    val partTable = loadPart()
    val partsuppTable = loadPartsupp()
    val lineitemTable = loadLineitem()
    for (i <- 0 until numRuns) {
      runQuery {
        val constantDate1 = parseDate("1996-01-01")
        val constantDate2 = parseDate("1997-01-01")
        val jordan = parseString("JORDAN")
        val azure = parseString("azure")
        val scanPart = Query(partTable).filter(x => x.P_NAME startsWith azure)
        val scanNation = Query(nationTable).filter(x => x.N_NAME === jordan)
        val scanLineitem = Query(lineitemTable).filter(x => x.L_SHIPDATE >= constantDate1 && x.L_SHIPDATE < constantDate2)
        val jo1 = scanPart.hashJoin(Query(partsuppTable))(x => x.P_PARTKEY)(x => x.PS_PARTKEY)((x, y) => x.P_PARTKEY == y.PS_PARTKEY)
        val jo2 = jo1.hashJoin(scanLineitem)(x => x.PS_PARTKEY[Int])(x => x.L_PARTKEY)((x, y) => x.PS_PARTKEY[Int] == y.L_PARTKEY && x.PS_SUPPKEY[Int] == y.L_SUPPKEY)
        val aggOp = jo2.groupBy(x => new Q20GRPRecord(x.PS_PARTKEY[Int], x.PS_SUPPKEY[Int], x.PS_AVAILQTY[Int])).mapValues(_.map(_.L_QUANTITY[Double]).sum)
        val selOp = aggOp.filter(x => x._1.PS_AVAILQTY > 0.5 * x._2).map(x => x._1)
        val jo3 = selOp.hashJoin(Query(supplierTable))(x => x.PS_SUPPKEY)(x => x.S_SUPPKEY)((x, y) => x.PS_SUPPKEY == y.S_SUPPKEY)
        val jo4 = scanNation.hashJoin(jo3)(x => x.N_NATIONKEY)(x => x.S_NATIONKEY[Int])((x, y) => x.N_NATIONKEY == y.S_NATIONKEY[Int])
        val sortOp = jo4.sortBy(_.S_NAME[OptimalString].string)
        sortOp.printRows(kv =>
          printf("%s|%s\n", kv.S_NAME[OptimalString].string, kv.S_ADDRESS[OptimalString].string), -1)
        ()
      }
    }
  }

  def Q21(numRuns: Int) {
    //ProfileFunctionCalls.numberTuplesSelect = 0
    //ProfileFunctionCalls.numberTuplesFiltered = 0
    //ProfileFunctionCalls.numberTuplesScanned = 0
    val lineitemTable = loadLineitem()
    val supplierTable = loadSupplier()
    val ordersTable = loadOrders()
    val nationTable = loadNation()
    for (i <- 0 until numRuns) {
      runQuery({
        val morocco = parseString("MOROCCO")
        val lineitemScan1 = new SelectOp(new ScanOp(lineitemTable))(x => x.L_RECEIPTDATE > x.L_COMMITDATE)
        val lineitemScan2 = new ScanOp(lineitemTable)
        val lineitemScan3 = new SelectOp(new ScanOp(lineitemTable))(x => x.L_RECEIPTDATE > x.L_COMMITDATE)
        val supplierScan = new ScanOp(supplierTable)
        val nationScan = new SelectOp(new ScanOp(nationTable))(x => x.N_NAME === morocco)
        val ordersScan = new SelectOp(new ScanOp(ordersTable))(x => x.O_ORDERSTATUS == 'F')
        val jo1 = new HashJoinOp(nationScan, supplierScan)((x, y) => x.N_NATIONKEY == y.S_NATIONKEY)(x => x.N_NATIONKEY)(x => x.S_NATIONKEY)
        val jo2 = new HashJoinOp(jo1, lineitemScan1)((x, y) => x.S_SUPPKEY[Int] == y.L_SUPPKEY)(x => x.S_SUPPKEY[Int])(x => x.L_SUPPKEY)
        // val jo1 = new HashJoinOp(supplierScan, lineitemScan1)((x, y) => x.S_SUPPKEY == y.L_SUPPKEY)(x => x.S_SUPPKEY)(x => x.L_SUPPKEY)
        // val jo2 = new HashJoinOp(nationScan, jo1)((x, y) => x.N_NATIONKEY == y.S_NATIONKEY[Int])(x => x.N_NATIONKEY)(x => x.S_NATIONKEY[Int])
        val jo3 = new LeftHashSemiJoinOp(jo2, lineitemScan2)((x, y) => (x.L_ORDERKEY[Int] == y.L_ORDERKEY) && (x.L_SUPPKEY[Int] != y.L_SUPPKEY))(x => x.L_ORDERKEY[Int])(x => x.L_ORDERKEY)
        val jo4 = new HashJoinAnti(jo3, lineitemScan3)((x, y) => (x.L_ORDERKEY[Int] == y.L_ORDERKEY) && (x.L_SUPPKEY[Int] != y.L_SUPPKEY))(x => x.L_ORDERKEY[Int])(x => x.L_ORDERKEY)
        val jo5 = new HashJoinOp(ordersScan, jo4)((x, y) => x.O_ORDERKEY == y.L_ORDERKEY[Int])(x => x.O_ORDERKEY)(x => x.L_ORDERKEY[Int])
        val aggOp = new AggOp(jo5, 1)(x => x.S_NAME[OptimalString])((t, currAgg) => { currAgg + 1 })
        val sortOp = new SortOp(aggOp)((x, y) => {
          val a1 = x.aggs(0); val a2 = y.aggs(0)
          if (a1 < a2) 1
          else if (a1 > a2) -1
          else x.key diff y.key
        })
        val po = new PrintOp(sortOp)(kv => {
          printf("%s|%.0f\n", kv.key.string, kv.aggs(0))
        }, 100)
        po.run()
        ()
      })
    }
    //FunctionCalls.numberTuplesSelect: " + //ProfileFunctionCalls.numberTuplesSelect)
    //FunctionCalls.numberTuplesFiltered: " + //ProfileFunctionCalls.numberTuplesFiltered)
    //FunctionCalls.numberTuplesScanned: " + //ProfileFunctionCalls.numberTuplesScanned);
  }

  def Q22(numRuns: Int) {
    //ProfileFunctionCalls.numberTuplesSelect = 0
    //ProfileFunctionCalls.numberTuplesFiltered = 0
    //ProfileFunctionCalls.numberTuplesScanned = 0

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
        val customerScan1 = new SelectOp(new ScanOp(customerTable))(x => {
          x.C_ACCTBAL > 0.00 && (
            x.C_PHONE.startsWith(v23) || (x.C_PHONE.startsWith(v29) || (x.C_PHONE.startsWith(v22) ||
              (x.C_PHONE.startsWith(v20) || (x.C_PHONE.startsWith(v24) || (x.C_PHONE.startsWith(v26) ||
                x.C_PHONE.startsWith(v25)))))))
        })
        val aggOp1 = new AggOp(customerScan1, 3)(x => "AVG_C_ACCTBAL")(
          (t, currAgg) => { t.C_ACCTBAL + currAgg },
          (t, currAgg) => { currAgg + 1 })
        val mapOp = new MapOp(aggOp1)(kv => { kv.key; kv.aggs(2) = kv.aggs(0) / kv.aggs(1) })
        mapOp.open
        val nestedAVG = new SubquerySingleResult(mapOp).getResult.aggs(2)
        // External Query
        val customerScan2 = new SelectOp(new ScanOp(customerTable))(x => {
          (x.C_PHONE.startsWith(v23) || (x.C_PHONE.startsWith(v29) || (x.C_PHONE.startsWith(v22) ||
            (x.C_PHONE.startsWith(v20) || (x.C_PHONE.startsWith(v24) || (x.C_PHONE.startsWith(v26) ||
              x.C_PHONE.startsWith(v25))))))) &&
              x.C_ACCTBAL > nestedAVG
        })
        val ordersScan = new ScanOp(ordersTable)
        val jo = new HashJoinAnti(customerScan2, ordersScan)((x, y) => x.C_CUSTKEY == y.O_CUSTKEY)(x => x.C_CUSTKEY)(x => x.O_CUSTKEY)
        val aggOp2 = new AggOp(jo, 2)(x => x.C_PHONE.slice(0, 2))(
          (t, currAgg) => { t.C_ACCTBAL + currAgg },
          (t, currAgg) => { currAgg + 1 })
        val sortOp = new SortOp(aggOp2)((x, y) => {
          // We know that the substring has only two characters
          var res = x.key(0) - y.key(0)
          if (res == 0) res = x.key(1) - y.key(1)
          res
        })
        val po = new PrintOp(sortOp)(kv => printf("%s|%.0f|%.2f\n", kv.key.string, kv.aggs(1), kv.aggs(0)), -1)
        po.run()
        ()
      })
    }
    //FunctionCalls.numberTuplesSelect: " + //ProfileFunctionCalls.numberTuplesSelect)
    //FunctionCalls.numberTuplesFiltered: " + //ProfileFunctionCalls.numberTuplesFiltered)
    //FunctionCalls.numberTuplesScanned: " + //ProfileFunctionCalls.numberTuplesScanned);
  }
}
