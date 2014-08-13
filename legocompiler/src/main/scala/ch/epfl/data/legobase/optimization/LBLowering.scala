package ch.epfl.data
package legobase
package optimization

import legobase.deep._
import scala.language.implicitConversions
import pardis.ir._
import pardis.optimization._

class LBLowering(override val from: InliningLegoBase, override val to: LoweringLegoBase) extends Lowering[InliningLegoBase, LoweringLegoBase](from, to) {
  import from._

  override def transformDef[T: Manifest](node: Def[T]): to.Def[T] = node match {
    case an: AggOpNew[_, _] => {
      val ma = an.manifestA
      val mb = an.manifestB
      val maa = ma.asInstanceOf[Manifest[Any]]
      val marrDouble = manifest[Array[Double]]
      class B
      implicit val manifestRec: Manifest[B] = mb.asInstanceOf[Manifest[B]]
      val maggRecB = manifest[AGGRecord[B]].asInstanceOf[Manifest[Any]]
      to.__newDef[AggOp[_, _]](("hm", false, to.__newHashMap()(to.overloaded2, mb, marrDouble)),
        ("NullDynamicRecord", false, to.infix_asInstanceOf(to.unit[Any](null))(maggRecB)),
        ("keySet", true, to.Set()(mb, to.overloaded2)),
        ("numAggs", false, an.numAggs)).asInstanceOf[to.Def[T]]
    }
    case po: PrintOpNew[_] => {
      val ma = po.manifestA
      val maa = ma.asInstanceOf[Manifest[Any]]
      to.__newDef[PrintOp[_]](("numRows", true, to.unit[Int](0)),
        ("NullDynamicRecord", false, to.infix_asInstanceOf(to.unit[Any](null))(maa))).asInstanceOf[to.Def[T]]
    }
    case so: ScanOpNew[_] => {
      val ma = so.manifestA
      val maa = ma.asInstanceOf[Manifest[Any]]
      to.__newDef[ScanOp[_]](("i", true, to.unit[Int](0)),
        ("table", false, so.table),
        ("NullDynamicRecord", false, to.infix_asInstanceOf(to.unit[Any](null))(maa))).asInstanceOf[to.Def[T]]
    }
    case mo: MapOpNew[_] => {
      val ma = mo.manifestA
      val maa = ma.asInstanceOf[Manifest[Any]]
      to.__newDef[MapOp[_]](
        ("NullDynamicRecord", false, to.infix_asInstanceOf(to.unit[Any](null))(maa))).asInstanceOf[to.Def[T]]
    }
    case so: SelectOpNew[_] => {
      val ma = so.manifestA
      val maa = ma.asInstanceOf[Manifest[Any]]
      to.__newDef[SelectOp[_]](
        ("NullDynamicRecord", false, to.infix_asInstanceOf(to.unit[Any](null))(maa))).asInstanceOf[to.Def[T]]
    }
    case so: SortOpNew[_] => {
      val ma = so.manifestA
      val maa = ma.asInstanceOf[Manifest[Any]]
      to.__newDef[SortOp[_]](("sortedTree", false, to.__newTreeSet2(to.Ordering[Any](so.orderingFunc.asInstanceOf[Rep[(Any, Any) => Int]])(maa))(maa)),
        ("NullDynamicRecord", false, to.infix_asInstanceOf(to.unit[Any](null))(maa))).asInstanceOf[to.Def[T]]
    }
    case ho: HashJoinOpNew[_, _, _] => {
      val ma = ho.manifestA
      val mb = ho.manifestB
      val mc = ho.manifestC
      val mba = mb.asInstanceOf[Manifest[Any]]
      trait A extends pardis.shallow.AbstractRecord
      implicit val manifestASynthetic: Manifest[A] = ma.asInstanceOf[Manifest[A]]
      trait B extends pardis.shallow.AbstractRecord
      implicit val manifestBSynthetic: Manifest[B] = mb.asInstanceOf[Manifest[B]]
      class C
      implicit val manifestCSynthetic: Manifest[C] = mc.asInstanceOf[Manifest[C]]
      val marrBuffA = manifest[ArrayBuffer[A]].asInstanceOf[Manifest[Any]]
      val mCompRec = manifest[DynamicCompositeRecord[A, B]].asInstanceOf[Manifest[Any]]
      to.__newDef[HashJoinOp[_, _, _]](("hm", false, to.__newHashMap()(to.overloaded2, mc, marrBuffA)),
        ("NullDynamicRecord", false, to.infix_asInstanceOf(to.unit[Any](null))(mCompRec)),
        ("tmpCount", true, to.unit[Int](-1)),
        ("tmpLine", true, to.infix_asInstanceOf(to.unit[Any](null))(mba)),
        ("tmpBuffer", true, to.ArrayBuffer()(ma))).asInstanceOf[to.Def[T]]
    }
    case wo: WindowOpNew[_, _, _] => {
      val ma = wo.manifestA
      val mb = wo.manifestB
      val mc = wo.manifestC
      val maa = ma.asInstanceOf[Manifest[Any]]
      class A
      implicit val manifestASynthetic: Manifest[A] = ma.asInstanceOf[Manifest[A]]
      class B
      implicit val manifestBSynthetic: Manifest[B] = mb.asInstanceOf[Manifest[B]]
      class C
      implicit val manifestCSynthetic: Manifest[C] = mc.asInstanceOf[Manifest[C]]
      val marrBuffA = manifest[ArrayBuffer[A]].asInstanceOf[Manifest[Any]]
      val mwinRecBC = manifest[WindowRecord[B, C]].asInstanceOf[Manifest[Any]]
      to.__newDef[WindowOp[_, _, _]](("hm", false, to.__newHashMap()(to.overloaded2, mb, marrBuffA)),
        ("NullDynamicRecord", false, to.infix_asInstanceOf(to.unit[Any](null))(mwinRecBC)),
        ("keySet", true, to.Set()(mb, to.overloaded2))).asInstanceOf[to.Def[T]]
    }
    // case gc: GroupByClassNew => {
    //   to.__newDef[GroupByClass](("L_RETURNFLAG", false, transformExp(gc.L_RETURNFLAG)),
    //     ("L_LINESTATUS", false, transformExp(gc.L_LINESTATUS))).asInstanceOf[to.Def[T]]
    // }
    // case li: LINEITEMRecordNew => {
    //   to.__newDef[LINEITEMRecord](("L_ORDERKEY", false, li.L_ORDERKEY),
    //     ("L_PARTKEY", false, li.L_PARTKEY),
    //     ("L_SUPPKEY", false, li.L_SUPPKEY),
    //     ("L_LINENUMBER", false, li.L_LINENUMBER),
    //     ("L_QUANTITY", false, li.L_QUANTITY),
    //     ("L_EXTENDEDPRICE", false, li.L_EXTENDEDPRICE),
    //     ("L_DISCOUNT", false, li.L_DISCOUNT),
    //     ("L_TAX", false, li.L_TAX),
    //     ("L_RETURNFLAG", false, li.L_RETURNFLAG),
    //     ("L_LINESTATUS", false, li.L_LINESTATUS),
    //     ("L_SHIPDATE", false, li.L_SHIPDATE),
    //     ("L_COMMITDATE", false, li.L_COMMITDATE),
    //     ("L_RECEIPTDATE", false, li.L_RECEIPTDATE),
    //     ("L_SHIPINSTRUCT", false, li.L_SHIPINSTRUCT),
    //     ("L_SHIPMODE", false, li.L_SHIPMODE),
    //     ("L_COMMENT", false, li.L_COMMENT)).asInstanceOf[to.Def[T]]
    // }
    case _ => super.transformDef(node)
  }

  // GroupByClass
  // LINEITEMRecord
  // SUPPLIERRecord
  // PARTSUPPRecord
  // REGIONRecord
  // PARTRecord
  // NATIONRecord
  // CUSTOMERRecord
  // ORDERSRecord
  object CaseClassNew extends DefExtractor {
    def unapply[T](exp: Def[T]): Option[Def[T]] =
      exp match {
        case _: GroupByClassNew | _: LINEITEMRecordNew | _: SUPPLIERRecordNew | _: PARTSUPPRecordNew | _: REGIONRecordNew | _: PARTRecordNew | _: NATIONRecordNew | _: CUSTOMERRecordNew | _: ORDERSRecordNew => Some(exp)
        case _ => None
      }
  }

  object LoweredNew extends RepExtractor {
    def unapply[T](exp: Rep[T]): Option[Def[T]] = exp match {
      case Def(d) => d match {
        case _ if List(classOf[GroupByClass], classOf[LINEITEMRecord], classOf[SUPPLIERRecord], classOf[PARTSUPPRecord], classOf[REGIONRecord], classOf[PARTRecord], classOf[NATIONRecord], classOf[CUSTOMERRecord], classOf[ORDERSRecord], classOf[AggOp[_, _]], classOf[PrintOp[_]], classOf[ScanOp[_]], classOf[MapOp[_]], classOf[SelectOp[_]], classOf[SortOp[_]], classOf[HashJoinOp[_, _, _]], classOf[WindowOp[_, _, _]]).contains(d.tp.runtimeClass) => Some(d)
        case _ => None
      }
      case _ => None
    }
  }
}
