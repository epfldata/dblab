package miniDB

import QueryEngine.VolcanoPullEngine._

trait Queries extends Q1 
trait GenericQuery extends ScalaImpl with StorageManager.Loader

trait Q1 extends GenericQuery {
	case class GroupByClass(val L_RETURNFLAG: java.lang.Character, val L_LINESTATUS: java.lang.Character);
	def Q1(numRuns: Int) {
		val lineitemTable = loadLineitem()
		println("Loading complete!")
		for (i <- 0 until numRuns) {
			val constantDate: Long = parseDate("1998-08-11")
			val lineitemScan = SelectOp(ScanOp(lineitemTable))(x => x.L_SHIPDATE <= constantDate)
			val aggOp = AggOp(lineitemScan, 9)(x => new GroupByClass( 
				x.L_RETURNFLAG,x.L_LINESTATUS 
			))(	(t,currAgg) => {t.L_DISCOUNT + currAgg},
				(t,currAgg) => {t.L_QUANTITY + currAgg},
				(t,currAgg) => {t.L_EXTENDEDPRICE + currAgg},
				(t,currAgg) => {(t.L_EXTENDEDPRICE * (1.0-t.L_DISCOUNT)) + currAgg},
				(t,currAgg) => {(t.L_EXTENDEDPRICE * (1.0-t.L_DISCOUNT) * (1.0+t.L_TAX)) + currAgg},
				(t,currAgg) => {currAgg + 1} )
			val mapOp = MapOp(aggOp)(kv => kv.aggs(6) = kv.aggs(1)/kv.aggs(5), // AVG(L_QUANTITY)
									 kv => kv.aggs(7) = kv.aggs(2)/kv.aggs(5), // AVG(L_EXTENDEDPRICE)
									 kv => kv.aggs(8) = kv.aggs(0)/kv.aggs(5)) // AVG(L_DISCOUNT)
			val sortOp = SortOp(mapOp)((kv1,kv2) => {
				var res = kv1.key.L_RETURNFLAG - kv2.key.L_RETURNFLAG
				if (res == 0)
					res = kv1.key.L_LINESTATUS - kv2.key.L_LINESTATUS
				res
			})
			val po = PrintOp(sortOp)(kv => printf("%c|%c|%.2f|%.2f|%.2f|%.2f|%.2f|%.2f|%.6f|%.0f\n",
                 kv.key.L_RETURNFLAG,kv.key.L_LINESTATUS,kv.aggs(1),kv.aggs(2),kv.aggs(3),kv.aggs(4),
                 kv.aggs(6),kv.aggs(7),kv.aggs(8),kv.aggs(5))
            )
			po.open
			po.next
			printf("(%d rows)\n", po.numRows)
			()
		}
	}
}
