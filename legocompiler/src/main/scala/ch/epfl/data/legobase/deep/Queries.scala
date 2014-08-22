
package ch.epfl.data
package legobase
package deep

import scalalib._
import pardis.ir._
import pardis.ir.pardisTypeImplicits._
import pardis.deep.scalalib._
trait QueriesOps extends Base { this: QueryComponent =>
  implicit class QueriesRep(self: Rep[Queries]) {

  }
  object Queries {
    def Q1(numRuns: Rep[Int]): Rep[Unit] = queriesQ1Object(numRuns)
  }
  // constructors

  // case classes
  case class QueriesQ1Object(numRuns: Rep[Int]) extends FunctionDef[Unit](None, "Queries.Q1", List(List(numRuns))) {
    override def curriedConstructor = (copy _)
  }

  // method definitions
  def queriesQ1Object(numRuns: Rep[Int]): Rep[Unit] = QueriesQ1Object(numRuns)
  type Queries = ch.epfl.data.legobase.Queries
  case object QueriesType extends TypeRep[Queries] {
    def rebuild(newArguments: TypeRep[_]*): TypeRep[_] = QueriesType
    val name = "Queries"
    val typeArguments = Nil

    val typeTag = scala.reflect.runtime.universe.typeTag[Queries]
  }
  implicit val typeQueries = QueriesType
}
trait QueriesImplicits { this: QueriesComponent =>
  // Add implicit conversions here!
}
trait QueriesImplementations { self: DeepDSL =>
  override def queriesQ1Object(numRuns: Rep[Int]): Rep[Unit] = {
    {
      val lineitemTable: this.Rep[Array[ch.epfl.data.legobase.queryengine.TPCHRelations.LINEITEMRecord]] = Loader.loadLineitem();
      GenericEngine.runQuery[Unit]({
        val constantDate: this.Rep[Long] = GenericEngine.parseDate(unit("1998-08-11"));
        val lineitemScan: this.Rep[ch.epfl.data.legobase.queryengine.volcano.SelectOp[ch.epfl.data.legobase.queryengine.TPCHRelations.LINEITEMRecord]] = __newSelectOp(__newScanOp(lineitemTable))(__lambda(((x: this.Rep[ch.epfl.data.legobase.queryengine.TPCHRelations.LINEITEMRecord]) => x.L_SHIPDATE.$less$eq(constantDate))));
        val aggOp: this.Rep[ch.epfl.data.legobase.queryengine.volcano.AggOp[ch.epfl.data.legobase.queryengine.TPCHRelations.LINEITEMRecord, ch.epfl.data.legobase.queryengine.GroupByClass]] = __newAggOp(lineitemScan, unit(9))(__lambda(((x: this.Rep[ch.epfl.data.legobase.queryengine.TPCHRelations.LINEITEMRecord]) => __newGroupByClass(x.L_RETURNFLAG, x.L_LINESTATUS))))(__lambda(((t: this.Rep[ch.epfl.data.legobase.queryengine.TPCHRelations.LINEITEMRecord], currAgg: this.Rep[Double]) => t.L_DISCOUNT.$plus(currAgg))), __lambda(((t: this.Rep[ch.epfl.data.legobase.queryengine.TPCHRelations.LINEITEMRecord], currAgg: this.Rep[Double]) => t.L_QUANTITY.$plus(currAgg))), __lambda(((t: this.Rep[ch.epfl.data.legobase.queryengine.TPCHRelations.LINEITEMRecord], currAgg: this.Rep[Double]) => t.L_EXTENDEDPRICE.$plus(currAgg))), __lambda(((t: this.Rep[ch.epfl.data.legobase.queryengine.TPCHRelations.LINEITEMRecord], currAgg: this.Rep[Double]) => t.L_EXTENDEDPRICE.$times(unit(1.0).$minus(t.L_DISCOUNT)).$plus(currAgg))), __lambda(((t: this.Rep[ch.epfl.data.legobase.queryengine.TPCHRelations.LINEITEMRecord], currAgg: this.Rep[Double]) => t.L_EXTENDEDPRICE.$times(unit(1.0).$minus(t.L_DISCOUNT)).$times(unit(1.0).$plus(t.L_TAX)).$plus(currAgg))), __lambda(((t: this.Rep[ch.epfl.data.legobase.queryengine.TPCHRelations.LINEITEMRecord], currAgg: this.Rep[Double]) => currAgg.$plus(unit(1)))));
        val mapOp: this.Rep[ch.epfl.data.legobase.queryengine.volcano.MapOp[ch.epfl.data.legobase.queryengine.AGGRecord[ch.epfl.data.legobase.queryengine.GroupByClass]]] = __newMapOp(aggOp)(__lambda(((kv: this.Rep[ch.epfl.data.legobase.queryengine.AGGRecord[ch.epfl.data.legobase.queryengine.GroupByClass]]) => kv.aggs.update(unit(6), kv.aggs.apply(unit(1)).$div(kv.aggs.apply(unit(5)))))), __lambda(((kv: this.Rep[ch.epfl.data.legobase.queryengine.AGGRecord[ch.epfl.data.legobase.queryengine.GroupByClass]]) => kv.aggs.update(unit(7), kv.aggs.apply(unit(2)).$div(kv.aggs.apply(unit(5)))))), __lambda(((kv: this.Rep[ch.epfl.data.legobase.queryengine.AGGRecord[ch.epfl.data.legobase.queryengine.GroupByClass]]) => kv.aggs.update(unit(8), kv.aggs.apply(unit(0)).$div(kv.aggs.apply(unit(5)))))));
        val sortOp: this.Rep[ch.epfl.data.legobase.queryengine.volcano.SortOp[ch.epfl.data.legobase.queryengine.AGGRecord[ch.epfl.data.legobase.queryengine.GroupByClass]]] = __newSortOp(mapOp)(__lambda(((kv1: this.Rep[ch.epfl.data.legobase.queryengine.AGGRecord[ch.epfl.data.legobase.queryengine.GroupByClass]], kv2: this.Rep[ch.epfl.data.legobase.queryengine.AGGRecord[ch.epfl.data.legobase.queryengine.GroupByClass]]) => {
          var res: this.Var[Int] = __newVar(Character2char(kv1.key.L_RETURNFLAG).$minus(Character2char(kv2.key.L_RETURNFLAG)));
          __ifThenElse(infix_$eq$eq(readVar(res), unit(0)), __assign(res, Character2char(kv1.key.L_LINESTATUS).$minus(Character2char(kv2.key.L_LINESTATUS))), unit(()));
          readVar(res)
        })));
        val po: this.Rep[ch.epfl.data.legobase.queryengine.volcano.PrintOp[ch.epfl.data.legobase.queryengine.AGGRecord[ch.epfl.data.legobase.queryengine.GroupByClass]]] = __newPrintOp(sortOp)(__lambda(((kv: this.Rep[ch.epfl.data.legobase.queryengine.AGGRecord[ch.epfl.data.legobase.queryengine.GroupByClass]]) => printf(unit("%c|%c|%.2f|%.2f|%.2f|%.2f|%.2f|%.2f|%.2f|%.0f\n"), kv.key.L_RETURNFLAG, kv.key.L_LINESTATUS, kv.aggs.apply(unit(1)), kv.aggs.apply(unit(2)), kv.aggs.apply(unit(3)), kv.aggs.apply(unit(4)), kv.aggs.apply(unit(6)), kv.aggs.apply(unit(7)), kv.aggs.apply(unit(8)), kv.aggs.apply(unit(5))))), __lambda((() => unit(true))));
        po.open();
        po.next();
        printf(unit("(%d rows)\n"), po.numRows);
        unit(())
      })
    }
  }
}
trait QueriesComponent extends QueriesOps with QueriesImplicits { self: QueryComponent => }
trait QueryComponent extends QueriesComponent with AGGRecordComponent with WindowRecordComponent with CharacterComponent with DoubleComponent with IntComponent with LongComponent with ArrayComponent with LINEITEMRecordComponent with K2DBScannerComponent with IntegerComponent with BooleanComponent with HashMapComponent with SetComponent with TreeSetComponent with DefaultEntryComponent with ArrayBufferComponent with ManualLiftedLegoBase { self: DeepDSL => }