package ch.epfl.data
package legobase
package deep

// FIXME there's a bug for AggOpNew. The functions should not be (Rep[T] => Rep[S])* but they should be two distinct parameters Rep[T]* and Rep[S]*
// which ideally should be zipped together to perform further operations

// TODO think why new Foo(t)(s) is converted to fooNew(t, s)? If it's being changed lots of type annotations will be removed

/* This class is the manually completely lifted version of Query1 which ideally should be lifted by YY */
class LiftedQueries {
  val context = new InliningLegoBase {
    def q1 = {
      val lineitemTable = loadLineitem()
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
        printf(unit("%c|%c|%.2f|%.2f|%.2f|%.2f|%.2f|%.2f|%.6f|%.0f\n"),
          kv.key.L_RETURNFLAG, kv.key.L_LINESTATUS, kv.aggs(1), kv.aggs(2), kv.aggs(3), kv.aggs(4),
          kv.aggs(6), kv.aggs(7), kv.aggs(8), kv.aggs(5))
      })

      val poo = po.open
      val pon = po.next
      val pop = printf(unit("(%d rows)\n"), po.numRows)
      pop
      // List(lineitemTable, pl, constantDate, lineitemScan, aggOp, mapOp, sortOp, po, poo, pon, pop)
    }
    // }.q1.mkString("\n---\n")

    def q1Block = reifyBlock(q1)
  }

  def Q1() =
    context.q1Block
}
