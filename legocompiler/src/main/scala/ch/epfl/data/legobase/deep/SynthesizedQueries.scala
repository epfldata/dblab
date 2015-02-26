package ch.epfl.data.legobase.deep
import ch.epfl.data.pardis
import pardis.ir._
import pardis.types.PardisTypeImplicits._
import pardis.effects._
import pardis.deep._
import pardis.deep.scalalib._
import pardis.deep.scalalib.collection._
import pardis.deep.scalalib.io._

trait SynthesizedQueriesComponent extends QueriesImplementations { this: ch.epfl.data.legobase.deep.DeepDSL =>

  def Q12Synthesized(numRuns: Rep[Int], fields: Int): Rep[Unit] = {
    {
      val lineitemTable: this.Rep[Array[ch.epfl.data.legobase.queryengine.LINEITEMRecord]] = Loader.loadLineitem();
      val ordersTable: this.Rep[Array[ch.epfl.data.legobase.queryengine.ORDERSRecord]] = Loader.loadOrders();
      intWrapper(unit(0)).until(numRuns).foreach[Unit](__lambda(((i: this.Rep[Int]) => GenericEngine.runQuery[Unit]({
        val mail: this.Rep[ch.epfl.data.legobase.LBString] = GenericEngine.parseString(unit("MAIL"));
        val ship: this.Rep[ch.epfl.data.legobase.LBString] = GenericEngine.parseString(unit("SHIP"));
        val constantDate: this.Rep[Int] = GenericEngine.parseDate(unit("1995-01-01"));
        val constantDate2: this.Rep[Int] = GenericEngine.parseDate(unit("1994-01-01"));
        val so1: this.Rep[ch.epfl.data.legobase.queryengine.push.ScanOp[ch.epfl.data.legobase.queryengine.ORDERSRecord]] = __newScanOp(ordersTable);
        val so2 = __newSelectOp(__newScanOp(lineitemTable))(__lambda(((x: this.Rep[ch.epfl.data.legobase.queryengine.LINEITEMRecord]) => {
          def firstPart = x.L_SHIPDATE < constantDate && x.L_SHIPDATE < x.L_COMMITDATE
          def secondPart = x.L_COMMITDATE < constantDate && x.L_COMMITDATE < x.L_RECEIPTDATE
          def thirdPart = x.L_RECEIPTDATE < constantDate && x.L_RECEIPTDATE >= constantDate2
          def lastPart = (x.L_SHIPMODE === mail || x.L_SHIPMODE === ship)
          fields match {
            case 0 => lastPart
            case 1 => thirdPart && lastPart
            case 2 => secondPart && thirdPart && lastPart
            case 3 => firstPart && secondPart && thirdPart && lastPart
          }
        })));
        val jo: this.Rep[ch.epfl.data.legobase.queryengine.push.HashJoinOp[ch.epfl.data.legobase.queryengine.ORDERSRecord, ch.epfl.data.legobase.queryengine.LINEITEMRecord, Int]] = __newHashJoinOp(so1, so2)(__lambda(((x: this.Rep[ch.epfl.data.legobase.queryengine.ORDERSRecord], y: this.Rep[ch.epfl.data.legobase.queryengine.LINEITEMRecord]) => infix_$eq$eq(x.O_ORDERKEY, y.L_ORDERKEY))))(__lambda(((x: this.Rep[ch.epfl.data.legobase.queryengine.ORDERSRecord]) => x.O_ORDERKEY)))(__lambda(((x: this.Rep[ch.epfl.data.legobase.queryengine.LINEITEMRecord]) => x.L_ORDERKEY)));
        val URGENT: this.Rep[ch.epfl.data.legobase.LBString] = GenericEngine.parseString(unit("1-URGENT"));
        val HIGH: this.Rep[ch.epfl.data.legobase.LBString] = GenericEngine.parseString(unit("2-HIGH"));
        val aggOp: this.Rep[ch.epfl.data.legobase.queryengine.push.AggOp[ch.epfl.data.pardis.shallow.DynamicCompositeRecord[ch.epfl.data.legobase.queryengine.ORDERSRecord, ch.epfl.data.legobase.queryengine.LINEITEMRecord], ch.epfl.data.legobase.LBString]] = __newAggOp(jo, unit(2))(__lambda(((x: this.Rep[ch.epfl.data.pardis.shallow.DynamicCompositeRecord[ch.epfl.data.legobase.queryengine.ORDERSRecord, ch.epfl.data.legobase.queryengine.LINEITEMRecord]]) => x.selectDynamic[ch.epfl.data.legobase.LBString](unit("L_SHIPMODE")))))(__lambda(((t: this.Rep[ch.epfl.data.pardis.shallow.DynamicCompositeRecord[ch.epfl.data.legobase.queryengine.ORDERSRecord, ch.epfl.data.legobase.queryengine.LINEITEMRecord]], currAgg: this.Rep[Double]) => __ifThenElse(t.selectDynamic[ch.epfl.data.legobase.LBString](unit("O_ORDERPRIORITY")).$eq$eq$eq(URGENT).$bar$bar(t.selectDynamic[ch.epfl.data.legobase.LBString](unit("O_ORDERPRIORITY")).$eq$eq$eq(HIGH)), currAgg.$plus(unit(1)), currAgg))), __lambda(((t: this.Rep[ch.epfl.data.pardis.shallow.DynamicCompositeRecord[ch.epfl.data.legobase.queryengine.ORDERSRecord, ch.epfl.data.legobase.queryengine.LINEITEMRecord]], currAgg: this.Rep[Double]) => __ifThenElse(t.selectDynamic[ch.epfl.data.legobase.LBString](unit("O_ORDERPRIORITY")).$eq$bang$eq(URGENT).$amp$amp(t.selectDynamic[ch.epfl.data.legobase.LBString](unit("O_ORDERPRIORITY")).$eq$bang$eq(HIGH)), currAgg.$plus(unit(1)), currAgg))));
        val sortOp: this.Rep[ch.epfl.data.legobase.queryengine.push.SortOp[ch.epfl.data.legobase.queryengine.AGGRecord[ch.epfl.data.legobase.LBString]]] = __newSortOp(aggOp)(__lambda(((x: this.Rep[ch.epfl.data.legobase.queryengine.AGGRecord[ch.epfl.data.legobase.LBString]], y: this.Rep[ch.epfl.data.legobase.queryengine.AGGRecord[ch.epfl.data.legobase.LBString]]) => x.key.diff(y.key))));
        val po: this.Rep[ch.epfl.data.legobase.queryengine.push.PrintOp[ch.epfl.data.legobase.queryengine.AGGRecord[ch.epfl.data.legobase.LBString]]] = __newPrintOp(sortOp)(__lambda(((kv: this.Rep[ch.epfl.data.legobase.queryengine.AGGRecord[ch.epfl.data.legobase.LBString]]) => printf(unit("%s|%.0f|%.0f\n"), kv.key.string, kv.aggs.apply(unit(0)), kv.aggs.apply(unit(1))))), __lambda((() => unit(true))));
        po.open();
        po.next();
        unit(())
      }))))
    }
  }
}
