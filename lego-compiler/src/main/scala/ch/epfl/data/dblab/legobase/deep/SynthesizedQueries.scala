package ch.epfl.data.dblab.legobase.deep
import ch.epfl.data.sc.pardis
import pardis.ir._
import pardis.types.PardisTypeImplicits._
import pardis.effects._
import pardis.deep._
import pardis.deep.scalalib._
import pardis.deep.scalalib.collection._
import pardis.deep.scalalib.io._

/** A polymophic embedding cake which creates a synthesized version of TPCH query 12 */
trait SynthesizedQueriesComponent extends tpch.QueriesImplementations { this: ch.epfl.data.dblab.legobase.deep.DeepDSL =>

  def Q12Synthesized(numRuns: Rep[Int], fields: Int): Rep[Unit] = {
    {
      val lineitemTable: this.Rep[Array[ch.epfl.data.dblab.legobase.tpch.LINEITEMRecord]] = TPCHLoader.loadLineitem();
      val ordersTable: this.Rep[Array[ch.epfl.data.dblab.legobase.tpch.ORDERSRecord]] = TPCHLoader.loadOrders();
      intWrapper(unit(0)).until(numRuns).foreach[Unit](__lambda(((i: this.Rep[Int]) => GenericEngine.runQuery[Unit]({
        val mail: this.Rep[ch.epfl.data.dblab.legobase.LBString] = GenericEngine.parseString(unit("MAIL"));
        val ship: this.Rep[ch.epfl.data.dblab.legobase.LBString] = GenericEngine.parseString(unit("SHIP"));
        val constantDate: this.Rep[Int] = GenericEngine.parseDate(unit("1995-01-01"));
        val constantDate2: this.Rep[Int] = GenericEngine.parseDate(unit("1994-01-01"));
        val dummyDate: this.Rep[Int] = GenericEngine.parseDate(unit("1990-01-01"));
        val so1: this.Rep[ch.epfl.data.dblab.legobase.queryengine.push.ScanOp[ch.epfl.data.dblab.legobase.tpch.ORDERSRecord]] = __newScanOp(ordersTable);
        val so2 = __newSelectOp(__newScanOp(lineitemTable))(__lambda(((x: this.Rep[ch.epfl.data.dblab.legobase.tpch.LINEITEMRecord]) => {
          def firstPart = x.L_SHIPDATE < constantDate && x.L_SHIPDATE < x.L_COMMITDATE
          def secondPart = x.L_COMMITDATE < constantDate && x.L_COMMITDATE < x.L_RECEIPTDATE
          def thirdPart = x.L_RECEIPTDATE < constantDate && x.L_RECEIPTDATE >= constantDate2
          def lastPart = (x.L_SHIPMODE === mail || x.L_SHIPMODE === ship)
          // val dummyInt = unit(0)
          // val dummyDouble = unit(0.0)
          // def firstPart = x.L_SHIPDATE > dummyDate
          // def secondPart = x.L_COMMITDATE > dummyDate
          // def thirdPart = x.L_RECEIPTDATE > dummyDate
          // def lastPart = x.L_DISCOUNT >= dummyDouble
          // def extra1 = x.L_SUPPKEY >= dummyInt
          // def extra2 = x.L_PARTKEY >= dummyInt
          // def extra3 = x.L_LINENUMBER >= dummyInt
          // def extra4 = x.L_QUANTITY >= dummyDouble
          // def extra5 = x.L_EXTENDEDPRICE >= dummyDouble
          // def extra6 = x.L_TAX >= dummyDouble
          def original = firstPart && secondPart && thirdPart && lastPart
          // fields match {
          //   case 0 => lastPart
          //   case 1 => thirdPart && lastPart
          //   case 2 => secondPart && thirdPart && lastPart
          //   case 3 => original
          //   case 4 => original && extra1
          //   case 5 => original && extra1 && extra2
          //   case 6 => original && extra1 && extra2 && extra3
          //   case 7 => original && extra1 && extra2 && extra3 && extra4
          //   case 8 => original && extra1 && extra2 && extra3 && extra4 && extra5
          //   case 9 => original && extra1 && extra2 && extra3 && extra4 && extra5 && extra6
          // }
          original
        })));
        val jo: this.Rep[ch.epfl.data.dblab.legobase.queryengine.push.HashJoinOp[ch.epfl.data.dblab.legobase.tpch.ORDERSRecord, ch.epfl.data.dblab.legobase.tpch.LINEITEMRecord, Int]] = __newHashJoinOp(so1, so2)(__lambda(((x: this.Rep[ch.epfl.data.dblab.legobase.tpch.ORDERSRecord], y: this.Rep[ch.epfl.data.dblab.legobase.tpch.LINEITEMRecord]) => infix_$eq$eq(x.O_ORDERKEY, y.L_ORDERKEY))))(__lambda(((x: this.Rep[ch.epfl.data.dblab.legobase.tpch.ORDERSRecord]) => x.O_ORDERKEY)))(__lambda(((x: this.Rep[ch.epfl.data.dblab.legobase.tpch.LINEITEMRecord]) => x.L_ORDERKEY)));
        // val URGENT: this.Rep[ch.epfl.data.dblab.legobase.LBString] = GenericEngine.parseString(unit("1-URGENT"));
        // val HIGH: this.Rep[ch.epfl.data.dblab.legobase.LBString] = GenericEngine.parseString(unit("2-HIGH"));
        // val aggOp: this.Rep[ch.epfl.data.dblab.legobase.queryengine.push.AggOp[ch.epfl.data.sc.pardis.shallow.DynamicCompositeRecord[ch.epfl.data.dblab.legobase.tpch.ORDERSRecord, ch.epfl.data.dblab.legobase.tpch.LINEITEMRecord], ch.epfl.data.dblab.legobase.LBString]] = __newAggOp(jo, unit(2))(__lambda(((x: this.Rep[ch.epfl.data.sc.pardis.shallow.DynamicCompositeRecord[ch.epfl.data.dblab.legobase.tpch.ORDERSRecord, ch.epfl.data.dblab.legobase.tpch.LINEITEMRecord]]) => x.selectDynamic[ch.epfl.data.dblab.legobase.LBString](unit("L_SHIPMODE")))))(__lambda(((t: this.Rep[ch.epfl.data.sc.pardis.shallow.DynamicCompositeRecord[ch.epfl.data.dblab.legobase.tpch.ORDERSRecord, ch.epfl.data.dblab.legobase.tpch.LINEITEMRecord]], currAgg: this.Rep[Double]) => __ifThenElse(t.selectDynamic[ch.epfl.data.dblab.legobase.LBString](unit("O_ORDERPRIORITY")).$eq$eq$eq(URGENT).$bar$bar(t.selectDynamic[ch.epfl.data.dblab.legobase.LBString](unit("O_ORDERPRIORITY")).$eq$eq$eq(HIGH)), currAgg.$plus(unit(1)), currAgg))), __lambda(((t: this.Rep[ch.epfl.data.sc.pardis.shallow.DynamicCompositeRecord[ch.epfl.data.dblab.legobase.tpch.ORDERSRecord, ch.epfl.data.dblab.legobase.tpch.LINEITEMRecord]], currAgg: this.Rep[Double]) => __ifThenElse(t.selectDynamic[ch.epfl.data.dblab.legobase.LBString](unit("O_ORDERPRIORITY")).$eq$bang$eq(URGENT).$amp$amp(t.selectDynamic[ch.epfl.data.dblab.legobase.LBString](unit("O_ORDERPRIORITY")).$eq$bang$eq(HIGH)), currAgg.$plus(unit(1)), currAgg))));
        // val sortOp: this.Rep[ch.epfl.data.dblab.legobase.queryengine.push.SortOp[ch.epfl.data.dblab.legobase.queryengine.AGGRecord[ch.epfl.data.dblab.legobase.LBString]]] = __newSortOp(aggOp)(__lambda(((x: this.Rep[ch.epfl.data.dblab.legobase.queryengine.AGGRecord[ch.epfl.data.dblab.legobase.LBString]], y: this.Rep[ch.epfl.data.dblab.legobase.queryengine.AGGRecord[ch.epfl.data.dblab.legobase.LBString]]) => x.key.diff(y.key))));
        // val po: this.Rep[ch.epfl.data.dblab.legobase.queryengine.push.PrintOp[ch.epfl.data.dblab.legobase.queryengine.AGGRecord[ch.epfl.data.dblab.legobase.LBString]]] = __newPrintOp(sortOp)(__lambda(((kv: this.Rep[ch.epfl.data.dblab.legobase.queryengine.AGGRecord[ch.epfl.data.dblab.legobase.LBString]]) => printf(unit("%s|%.0f|%.0f\n"), kv.key.string, kv.aggs.apply(unit(0)), kv.aggs.apply(unit(1))))), __lambda((() => unit(true))));
        val po = __newPrintOp(jo)(__lambda { kv =>
          def ex1 = kv.selectDynamic[Int](unit("L_SUPPKEY"))
          def ex2 = kv.selectDynamic[Int](unit("L_PARTKEY"))
          def ex3 = kv.selectDynamic[Int](unit("L_LINENUMBER"))
          def ex4 = kv.selectDynamic[Int](unit("L_QUANTITY"))
          def ex5 = kv.selectDynamic[Double](unit("L_EXTENDEDPRICE"))
          def ex6 = kv.selectDynamic[Double](unit("L_TAX"))
          fields match {
            case 0 => printf(unit("nothing"))
            case 1 => printf(unit("%d"), ex1)
            case 2 => printf(unit("%d, %d"), ex1, ex2)
            case 3 => printf(unit("%d, %d, %d"), ex1, ex2, ex3)
            case 4 => printf(unit("%d, %d, %d, %d"), ex1, ex2, ex3, ex4)
            case 5 => printf(unit("%d, %d, %d, %d, %f"), ex1, ex2, ex3, ex4, ex5)
            case 6 => printf(unit("%d, %d, %d, %d, %f, %f"), ex1, ex2, ex3, ex4, ex5, ex6)
          }
        }, __lambda { () => unit(true) })
        po.open();
        po.next();
        unit(())
      }))))
    }
  }
}
