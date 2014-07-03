package ch.epfl.data
package legobase

import queryengine.volcano._

trait Queries extends Q1 //with Q2
trait GenericQuery extends ScalaImpl with storagemanager.Loader

trait Q1 extends GenericQuery {
  case class GroupByClass(val L_RETURNFLAG: java.lang.Character, val L_LINESTATUS: java.lang.Character);
  def Q1(numRuns: Int) {
    val lineitemTable = loadLineitem()
    for (i <- 0 until numRuns) {
      import utils.Utilities.time
      time({
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
      }, "finish")
    }
  }
}
/*
trait Q2 extends GenericQuery {
  def Q2(numRuns: Int) {
    val partTable = loadPart()
    val partsuppTable = loadPartsupp()
    val nationTable = loadNation()
    val regionTable = loadRegion()
    val supplierTable = loadSupplier()
    for (i <- 0 until numRuns) {
      val africa = parseString("AFRICA")
      val tin = parseString("TIN")
      val partsuppScan = ScanOp(partsuppTable)
      val supplierScan = ScanOp(supplierTable)
      val jo1 = HashJoinOp(supplierScan, partsuppScan)((x, y) => x.S_SUPPKEY == y.PS_SUPPKEY)(x => x.S_SUPPKEY)(x => x.PS_SUPPKEY)
      val nationScan = ScanOp(nationTable)
      val jo2 = HashJoinOp(nationScan, jo1)((x, y) => x.N_NATIONKEY == y.S_NATIONKEY)(x => x.N_NATIONKEY)(x => x.S_NATIONKEY)
      val partScan = SelectOp(ScanOp(partTable))(x => x.P_SIZE == 43 && x.P_TYPE.endsWith(tin))
      val jo3 = HashJoinOp(partScan, jo2)((x, y) => x.P_PARTKEY == y.PS_PARTKEY)(x => x.P_PARTKEY)(x => x.PS_PARTKEY)
      val regionScan = SelectOp(ScanOp(regionTable))(x => x.R_NAME == africa)
      val jo4 = HashJoinOp(regionScan, jo3)((x, y) => x.R_REGIONKEY == y.N_REGIONKEY)(x => x.R_REGIONKEY)(x => x.N_REGIONKEY)
      val wo = WindowOp(jo4)(x => x.P_PARTKEY)(x => x.minBy(y => y.PS_SUPPLYCOST))
      val so = SortOp(wo)((x, y) => {
        if (x.wnd.S_ACCTBAL < y.wnd.S_ACCTBAL) 1
        else if (x.wnd.S_ACCTBAL > y.wnd.S_ACCTBAL) -1
        else {
          var res = x.wnd.apply[Array[Byte]]("N_NAME") compare y.wnd.apply[Array[Byte]]("N_NAME")
          if (res == 0) {
            res = x.wnd.apply[Array[Byte]]("S_NAME") compare y.wnd.apply[Array[Byte]]("S_NAME")
            if (res == 0) res = x.wnd.apply[Int]("P_PARTKEY") - y.wnd.apply[Int]("P_PARTKEY")
          }
          res
        }
      })
      var j = 0
      val po = PrintOp(so)(e => {
        val kv = e.wnd
        printf("%.2f|%s|%s|%d|%s|%s|%s|%s\n", kv.S_ACCTBAL, string_new(kv.S_NAME), string_new(kv.N_NAME), kv.P_PARTKEY, string_new(kv.P_MFGR), string_new(kv.S_ADDRESS), string_new(kv.S_PHONE), string_new(kv.S_COMMENT))
        j += 1
      }, () => j < 100)
      po.open
      po.next
      ()
    }
  }
}*/
