package ch.epfl.data
package legobase
package deep

import ch.epfl.data.pardis.ir.pardisTypeImplicits._
import scala.language.reflectiveCalls
// FIXME there's a bug for AggOpNew. The functions should not be (Rep[T] => Rep[S])* but they should be two distinct parameters Rep[T]* and Rep[S]*
// which ideally should be zipped together to perform further operations

// TODO think why new Foo(t)(s) is converted to fooNew(t, s)? If it's being changed lots of type annotations will be removed

/* This class is the manually completely lifted version of Query1 which ideally should be lifted by YY */
class LiftedQueries {
  val context = new LoweringLegoBase {
    // val context = new DeepDSL {
    def q1_u = {
      val lineitemTable = loadLineitem()
      runQuery {
        val constantDate = parseDate(unit("1998-08-11"))
        val lineitemScan = __newSelectOp(__newScanOp(lineitemTable))(__lambda { x => x.L_SHIPDATE <= constantDate })
        val aggOp = __newAggOp(lineitemScan, unit(9))(__lambda { x =>
          groupByClassNew(
            x.L_RETURNFLAG, x.L_LINESTATUS)
        })(
          __lambda { (t, currAgg) => { t.L_DISCOUNT + currAgg } },
          __lambda { (t, currAgg) => { t.L_QUANTITY + currAgg } },
          __lambda { (t, currAgg) => { t.L_EXTENDEDPRICE + currAgg } },
          __lambda { (t, currAgg) => { (t.L_EXTENDEDPRICE * (unit(1.0) - t.L_DISCOUNT)) + currAgg } },
          __lambda { (t, currAgg) => { (t.L_EXTENDEDPRICE * (unit(1.0) - t.L_DISCOUNT) * (unit(1.0) + t.L_TAX)) + currAgg } },
          __lambda { (t, currAgg) => { currAgg + unit(1) } })
        val mapOp = __newMapOp(aggOp)(__lambda { (kv) => kv.aggs(6) = kv.aggs(1) / kv.aggs(5) }, // AVG(L_QUANTITY)
          __lambda { (kv) => kv.aggs(7) = kv.aggs(2) / kv.aggs(5) }, // AVG(L_EXTENDEDPRICE)
          __lambda { (kv) => kv.aggs(8) = kv.aggs(0) / kv.aggs(5) }) // AVG(L_DISCOUNT)
        val sortOp = __newSortOp(mapOp)(__lambda { (kv1, kv2) =>
          {
            // TODO YY should automatically virtualize it
            val res = __newVar(kv1.key.L_RETURNFLAG - kv2.key.L_RETURNFLAG)

            __ifThenElse(infix_==(res, unit(0)),
              __assign(res, kv1.key.L_LINESTATUS - kv2.key.L_LINESTATUS),
              unit(()))
            res
          }
        })
        val po = __newPrintOp2(sortOp)(__lambda { kv =>
          printf(unit("%c|%c|%.2f|%.2f|%.2f|%.2f|%.2f|%.2f|%.2f|%.0f\n"),
            kv.key.L_RETURNFLAG, kv.key.L_LINESTATUS, kv.aggs(1), kv.aggs(2), kv.aggs(3), kv.aggs(4),
            kv.aggs(6), kv.aggs(7), kv.aggs(8), kv.aggs(5))
        })

        po.open
        po.next
        printf(unit("(%d rows)\n"), po.numRows)
      }
    }

    def q1 = {
      val lineitemTable = loadLineitem()
      unit()
      runQuery {
        val constantDate = parseDate(unit("1998-08-11"))
        val lineitemScan = __newSelectOp(__newScanOp(lineitemTable))(__lambda { x => x.L_SHIPDATE <= constantDate })
        val aggOp = __newAggOp(lineitemScan, unit(9))(__lambda { x =>
          __newGroupByClass(
            x.L_RETURNFLAG, x.L_LINESTATUS)
        })(
          __lambda { (t, currAgg) => { t.L_DISCOUNT + currAgg } },
          __lambda { (t, currAgg) => { t.L_QUANTITY + currAgg } },
          __lambda { (t, currAgg) => { t.L_EXTENDEDPRICE + currAgg } },
          __lambda { (t, currAgg) => { (t.L_EXTENDEDPRICE * (unit(1.0) - t.L_DISCOUNT)) + currAgg } },
          __lambda { (t, currAgg) => { (t.L_EXTENDEDPRICE * (unit(1.0) - t.L_DISCOUNT) * (unit(1.0) + t.L_TAX)) + currAgg } },
          __lambda { (t, currAgg) => { currAgg + unit(1) } })
        val mapOp = __newMapOp(aggOp)(__lambda { (kv) => kv.aggs(6) = kv.aggs(1) / kv.aggs(5) }, // AVG(L_QUANTITY)
          __lambda { (kv) => kv.aggs(7) = kv.aggs(2) / kv.aggs(5) }, // AVG(L_EXTENDEDPRICE)
          __lambda { (kv) => kv.aggs(8) = kv.aggs(0) / kv.aggs(5) }) // AVG(L_DISCOUNT)
        val sortOp = __newSortOp(mapOp)(__lambda { (kv1, kv2) =>
          {
            // TODO YY should automatically virtualize it
            val res = __newVar(kv1.key.L_RETURNFLAG - kv2.key.L_RETURNFLAG)

            __ifThenElse(infix_==(res, unit(0)),
              __assign(res, kv1.key.L_LINESTATUS - kv2.key.L_LINESTATUS),
              unit(()))
            res
          }
        })
        val po = __newPrintOp2(sortOp)(__lambda { kv =>
          printf(unit("%c|%c|%.2f|%.2f|%.2f|%.2f|%.2f|%.2f|%.2f|%.0f\n"),
            kv.key.L_RETURNFLAG, kv.key.L_LINESTATUS, kv.aggs(1), kv.aggs(2), kv.aggs(3), kv.aggs(4),
            kv.aggs(6), kv.aggs(7), kv.aggs(8), kv.aggs(5))
        })
        po.open
        po.next
        printf(unit("(%d rows)\n"), po.numRows)
      }
    }

    def q2 = {
      val partTable = loadPart()
      val partsuppTable = loadPartsupp()
      val nationTable = loadNation()
      val regionTable = loadRegion()
      val supplierTable = loadSupplier()
      runQuery {
        val africa = parseString(unit("AFRICA"))
        val tin = parseString(unit("TIN"))
        val partsuppScan = __newScanOp(partsuppTable)
        val supplierScan = __newScanOp(supplierTable)
        val jo1 = __newHashJoinOp2(supplierScan, partsuppScan)(__lambda { (x, y) => x.S_SUPPKEY __== y.PS_SUPPKEY })(__lambda { x => x.S_SUPPKEY })(__lambda { x => x.PS_SUPPKEY })
        val nationScan = __newScanOp(nationTable)
        val jo2 = __newHashJoinOp2(nationScan, jo1)(__lambda { (x, y) => x.N_NATIONKEY __== y.f.S_NATIONKEY[Int] })(__lambda { x => x.N_NATIONKEY })(__lambda { x => x.f.S_NATIONKEY[Int] })
        val partScan = __newSelectOp(__newScanOp(partTable))(__lambda { x => (x.P_SIZE __== unit(43)) && x.P_TYPE.endsWith(tin) })
        val jo3 = __newHashJoinOp2(partScan, jo2)(__lambda { (x, y) => x.P_PARTKEY __== y.f.PS_PARTKEY[Int] })(__lambda { x => x.P_PARTKEY })(__lambda { x => x.f.PS_PARTKEY[Int] })
        val regionScan = __newSelectOp(__newScanOp(regionTable))(__lambda { x => x.R_NAME === africa }) // for comparing equality of LBString we should use === instead of ==
        val jo4 = __newHashJoinOp2(regionScan, jo3)(__lambda { (x, y) => x.R_REGIONKEY __== y.f.N_REGIONKEY[Int] })(__lambda { x => x.R_REGIONKEY })(__lambda { x => x.f.N_REGIONKEY[Int] })
        val wo = __newWindowOp(jo4)(__lambda { x => x.f.P_PARTKEY[Int] })(__lambda { x => x.minBy(__lambda { y => y.f.PS_SUPPLYCOST[Double] }) })
        val so = __newSortOp(wo)(__lambda {
          (x, y) =>
            {
              __ifThenElse((x.wnd.f.S_ACCTBAL[Double] < y.wnd.f.S_ACCTBAL[Double]),
                unit(1), {
                  __ifThenElse(x.wnd.f.S_ACCTBAL[Double] > y.wnd.f.S_ACCTBAL[Double],
                    unit(-1), {
                      val res = __newVar(x.wnd.f.N_NAME[LBString] compare y.wnd.f.N_NAME[LBString])
                      __ifThenElse(infix_==(res, unit(0)), {
                        __assign(res, x.wnd.f.S_NAME[LBString] compare y.wnd.f.S_NAME[LBString])
                        __ifThenElse(infix_==(res, unit(0)), {
                          __assign(res, x.wnd.f.P_PARTKEY[Int] - y.wnd.f.P_PARTKEY[Int])
                        }, unit(()))
                      },
                        unit(()))
                      res
                    })
                })
            }
        })

        val j = __newVar(0)
        val po = __newPrintOp(so)(__lambda {
          e =>
            {
              val kv = e.wnd
              printf(unit("%.2f|%s|%s|%d|%s|%s|%s|%s\n"), kv.f.S_ACCTBAL[Double], (kv.f.S_NAME[LBString]).string, (kv.f.N_NAME[LBString]).string, kv.f.P_PARTKEY[Int], (kv.f.P_MFGR[LBString]).string, (kv.f.S_ADDRESS[LBString]).string, (kv.f.S_PHONE[LBString]).string, (kv.f.S_COMMENT[LBString]).string)
              __assign(j, readVar(j) + unit(1))
            }
        }, __lambda { () => readVar(j) < 100 })
        po.open
        po.next
        printf(unit("(%d rows)\n"), po.numRows)
        unit(())
      }
    }

    def q3 = {
      val lineitemTable = loadLineitem()
      val ordersTable = loadOrders()
      val customerTable = loadCustomer()
      runQuery({
        val constantDate = parseDate(unit("1995-03-04"))
        val scanCustomer = __newSelectOp(__newScanOp(customerTable))(__lambda { x => x.C_MKTSEGMENT __== parseString(unit("HOUSEHOLD")) })
        val scanOrders = __newSelectOp(__newScanOp(ordersTable))(__lambda { x => x.O_ORDERDATE < constantDate })
        val scanLineitem = __newSelectOp(__newScanOp(lineitemTable))(__lambda { x => x.L_SHIPDATE > constantDate })
        val jo1 = __newHashJoinOp2(scanCustomer, scanOrders)(__lambda { (x, y) => x.C_CUSTKEY __== y.O_CUSTKEY })(__lambda { x => x.C_CUSTKEY })(__lambda { x => x.O_CUSTKEY })
        val jo2 = __newHashJoinOp2(jo1, scanLineitem)(__lambda { (x, y) => x.f.O_ORDERKEY[Int] __== y.L_ORDERKEY })(__lambda { x => x.f.O_ORDERKEY[Int] })(__lambda { x => x.L_ORDERKEY })
        val aggOp = __newAggOp(jo2, 1)(__lambda { x => __newQ3GRPRecord(x.f.L_ORDERKEY[Int], x.f.O_ORDERDATE[Long], x.f.O_SHIPPRIORITY[Int]) })(__lambda { (t, currAgg) => { currAgg + (t.f.L_EXTENDEDPRICE[Double] * (unit(1.0) - t.f.L_DISCOUNT[Double])) } })
        val sortOp = __newSortOp(aggOp)(__lambda { (kv1, kv2) =>
          {
            val agg1 = kv1.aggs(unit(0))
            val agg2 = kv2.aggs(unit(0))
            __ifThenElse(agg1 < agg2,
              unit(1),
              __ifThenElse(agg1 > agg2,
                unit(-1), {
                  val k1 = kv1.key.O_ORDERDATE
                  val k2 = kv2.key.O_ORDERDATE
                  __ifThenElse(k1 < k2,
                    unit(-1),
                    __ifThenElse(k1 > k2,
                      unit(1),
                      unit(0)))
                }))
          }
        })
        var i = __newVar(unit(0))
        val po = __newPrintOp2(sortOp)(__lambda { kv =>
          {
            // TODO: The date is not printed properly (but is correct), and fails 
            // in the comparison with result file. Rest of fields OK.
            printf(unit("%d|%.4f|%s|%d\n"), kv.key.L_ORDERKEY, kv.aggs(unit(0)), dateToString(kv.key.O_ORDERDATE), kv.key.O_SHIPPRIORITY)
            __assign(i, readVar(i) + unit(1))
          }
        }, __lambda { () => readVar(i) < unit(10) })
        po.open
        po.next
        printf(unit("(%d rows)\n"), po.numRows)
        unit(())
      })
    }

    def q4 = {
      val lineitemTable = loadLineitem()
      val ordersTable = loadOrders()

      runQuery({
        val constantDate1 = parseDate(unit("1993-11-01"))
        val constantDate2 = parseDate(unit("1993-08-01"))
        val scanOrders = __newSelectOp(__newScanOp(ordersTable))(__lambda { x => x.O_ORDERDATE < constantDate1 && x.O_ORDERDATE >= constantDate2 })
        val scanLineitem = __newSelectOp(__newScanOp(lineitemTable))(__lambda { x => x.L_COMMITDATE < x.L_RECEIPTDATE })
        val hj = __newLeftHashSemiJoinOp(scanOrders, scanLineitem)(__lambda { (x, y) => x.O_ORDERKEY __== y.L_ORDERKEY })(__lambda { x => x.O_ORDERKEY })(__lambda { x => x.L_ORDERKEY })
        val aggOp = __newAggOp(hj, unit(1))(__lambda { x => x.O_ORDERPRIORITY })(
          __lambda { (t, currAgg) => { currAgg + unit(1) } })
        val sortOp = __newSortOp(aggOp)(__lambda { (kv1, kv2) =>
          {
            val k1 = kv1.key
            val k2 = kv2.key
            k1 diff k2
          }
        })
        val po = __newPrintOp2(sortOp)(__lambda { kv => printf(unit("%s|%.0f\n"), kv.key.string, kv.aggs(unit(0))) })
        po.open
        po.next
        printf(unit("(%d rows)\n"), po.numRows)
        unit(())
      })

    }

    def q5 = {
      val lineitemTable = loadLineitem()
      val nationTable = loadNation()
      val customerTable = loadCustomer()
      val supplierTable = loadSupplier()
      val regionTable = loadRegion()
      val ordersTable = loadOrders()
      runQuery({
        val constantDate1 = parseDate(unit("1996-01-01"))
        val constantDate2 = parseDate(unit("1997-01-01"))
        val scanRegion = __newSelectOp(__newScanOp(regionTable))(__lambda { x => infix_==(x.R_NAME, parseString(unit("ASIA"))) })
        val scanNation = __newScanOp(nationTable)
        val scanSupplier = __newScanOp(supplierTable)
        val scanCustomer = __newScanOp(customerTable)
        val scanLineitem = __newScanOp(lineitemTable)
        val scanOrders = __newSelectOp(__newScanOp(ordersTable))(__lambda { x => x.O_ORDERDATE >= constantDate1 && x.O_ORDERDATE < constantDate2 })
        val jo1 = __newHashJoinOp2(scanRegion, scanNation)(__lambda { (x, y) => infix_==(x.R_REGIONKEY, y.N_REGIONKEY) })(__lambda { x => x.R_REGIONKEY })(__lambda { x => x.N_REGIONKEY })
        val jo2 = __newHashJoinOp2(jo1, scanSupplier)(__lambda { (x, y) => infix_==(x.f.N_NATIONKEY[Int], y.S_NATIONKEY) })(__lambda { x => x.f.N_NATIONKEY[Int] })(__lambda { x => x.S_NATIONKEY })
        val jo3 = __newHashJoinOp2(jo2, scanCustomer)(__lambda { (x, y) => infix_==(x.f.N_NATIONKEY[Int], y.C_NATIONKEY) })(__lambda { x => x.f.S_NATIONKEY[Int] })(__lambda { x => x.C_NATIONKEY })
        val jo4 = __newHashJoinOp2(jo3, scanOrders)(__lambda { (x, y) => infix_==(x.f.C_CUSTKEY[Int], y.O_CUSTKEY) })(__lambda { x => x.f.C_CUSTKEY[Int] })(__lambda { x => x.O_CUSTKEY })
        val jo5 = __newSelectOp(__newHashJoinOp2(jo4, scanLineitem)(__lambda { (x, y) => infix_==(x.f.O_ORDERKEY[Int], y.L_ORDERKEY) })(__lambda { x => x.f.O_ORDERKEY[Int] })(__lambda { x => x.L_ORDERKEY }))(__lambda { x => infix_==(x.f.S_SUPPKEY[Int], x.f.L_SUPPKEY[Int]) })
        val aggOp = __newAggOp(jo5, unit(1))(__lambda { x => x.f.N_NAME[LBString] })(
          __lambda { (t, currAgg) => { currAgg + t.f.L_EXTENDEDPRICE[Double] * (unit(1.0) - t.f.L_DISCOUNT[Double]) } })
        val sortOp = __newSortOp(aggOp)(__lambda { (x, y) =>
          {
            __ifThenElse(x.aggs(unit(0)) < y.aggs(unit(0)),
              unit(1),
              __ifThenElse(x.aggs(unit(0)) > y.aggs(unit(0)),
                unit(-1),
                unit(0)))
          }
        })
        val po = __newPrintOp2(sortOp)(__lambda { kv => printf(unit("%s|%.4f\n"), kv.key.string, kv.aggs(unit(0))) })
        po.open
        po.next
        printf(unit("(%d rows)\n"), po.numRows)
        unit(())
      })
    }

    def q6 = {
      val lineitemTable = loadLineitem()
      runQuery({
        val constantDate1 = parseDate(unit("1996-01-01"))
        val constantDate2 = parseDate(unit("1997-01-01"))
        val lineitemScan = __newSelectOp(__newScanOp(lineitemTable))(__lambda { x => x.L_SHIPDATE >= constantDate1 && x.L_SHIPDATE < constantDate2 && x.L_DISCOUNT >= unit(0.08) && x.L_DISCOUNT <= unit(0.1) && x.L_QUANTITY < unit(24) })
        val aggOp = __newAggOp(lineitemScan, unit(1))(__lambda { x => unit("Total") })(__lambda { (t, currAgg) => { (t.L_EXTENDEDPRICE * t.L_DISCOUNT) + currAgg } })
        val po = __newPrintOp2(aggOp)(__lambda { kv => printf(unit("%.4f\n"), kv.aggs(unit(0))) })
        po.open
        po.next
        printf(unit("(%d rows)\n"), po.numRows)
        unit(())
      })

    }

    def q1Block = reifyBlock(q1)
    def q1_uBlock = reifyBlock(q1_u)
    def q2Block = reifyBlock(q2)
    def q3Block = reifyBlock(q3)
    def q4Block = reifyBlock(q4)
    def q5Block = reifyBlock(q5)
    def q6Block = reifyBlock(q6)
  }

  def Q1() =
    context.q1Block

  def Q1_U() =
    context.q1_uBlock

  def Q2() =
    context.q2Block

  def Q3() =
    context.q3Block

  def Q4() =
    context.q4Block

  def Q5() =
    context.q5Block

  def Q6() =
    context.q6Block
}
