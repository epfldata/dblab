package ch.epfl.data
package legobase
package optimization

import legobase.deep._
import scala.language.implicitConversions
import pardis.ir._
import pardis.optimization._

class LBLowering(override val from: InliningLegoBase, override val to: LoweringLegoBase) extends Lowering[InliningLegoBase, LoweringLegoBase](from, to) {
  import from._

  override def transformType[T: Manifest]: Manifest[Any] = {
    val tp = manifest[T].asInstanceOf[Manifest[Any]]
    // Amir: it's a hack until TypeReps are comming
    if (tp <:< manifest[ArrayBuffer[_]]) {
      val arg = tp.typeArguments.head
      // class Rec
      // implicit val ma: Manifest[Rec] = transformType(arg).asInstanceOf[Manifest[Rec]]
      // manifest[ArrayBuffer[Rec]].asInstanceOf[Manifest[Any]]
      val res = reflect.ManifestFactory.classType(classOf[ArrayBuffer[_]], transformType(arg)).asInstanceOf[Manifest[Any]]
      // System.out.println(s"before was $arg now is:\n $res \n !!${pardis.utils.Utils.manifestToString(res)}")
      res
    } else {
      super.transformType[T]
    }
  }

  override def transformDef[T: Manifest](node: Def[T]): to.Def[T] = node match {
    case an: AggOpNew[_, _] => {
      val ma = an.manifestA
      val mb = an.manifestB
      val maa = ma.asInstanceOf[Manifest[Any]]
      val marrDouble = manifest[Array[Double]]
      class B
      implicit val manifestRec: Manifest[B] = mb.asInstanceOf[Manifest[B]]
      val maggRecB = manifest[AGGRecord[B]].asInstanceOf[Manifest[Any]]
      to.__newDef[AggOp[_, _]](("hm", false, to.__newHashMap()(to.overloaded2, apply(mb), apply(marrDouble))),
        ("NullDynamicRecord", false, to.infix_asInstanceOf(to.unit[Any](null))(apply(maggRecB))),
        ("keySet", true, to.Set()(apply(mb), to.overloaded2)),
        ("numAggs", false, an.numAggs)).asInstanceOf[to.Def[T]]
    }
    case po: PrintOpNew[_] => {
      val ma = po.manifestA
      val maa = ma.asInstanceOf[Manifest[Any]]
      to.__newDef[PrintOp[_]](("numRows", true, to.unit[Int](0)),
        ("NullDynamicRecord", false, to.infix_asInstanceOf(to.unit[Any](null))(apply(maa)))).asInstanceOf[to.Def[T]]
    }
    case so: ScanOpNew[_] => {
      val ma = so.manifestA
      val maa = ma.asInstanceOf[Manifest[Any]]
      to.__newDef[ScanOp[_]](("i", true, to.unit[Int](0)),
        ("table", false, so.table),
        ("NullDynamicRecord", false, to.infix_asInstanceOf(to.unit[Any](null))(apply(maa)))).asInstanceOf[to.Def[T]]
    }
    case mo: MapOpNew[_] => {
      val ma = mo.manifestA
      val maa = ma.asInstanceOf[Manifest[Any]]
      to.__newDef[MapOp[_]](
        ("NullDynamicRecord", false, to.infix_asInstanceOf(to.unit[Any](null))(apply(maa)))).asInstanceOf[to.Def[T]]
    }
    case so: SelectOpNew[_] => {
      val ma = so.manifestA
      val maa = ma.asInstanceOf[Manifest[Any]]
      to.__newDef[SelectOp[_]](
        ("NullDynamicRecord", false, to.infix_asInstanceOf(to.unit[Any](null))(apply(maa)))).asInstanceOf[to.Def[T]]
    }
    case so: SortOpNew[_] => {
      val ma = so.manifestA
      val maa = ma.asInstanceOf[Manifest[Any]]
      to.__newDef[SortOp[_]](("sortedTree", false, to.__newTreeSet2(to.Ordering[Any](apply(so.orderingFunc.asInstanceOf[Rep[(Any, Any) => Int]]))(apply(maa)))(apply(maa))),
        ("NullDynamicRecord", false, to.infix_asInstanceOf(to.unit[Any](null))(apply(maa)))).asInstanceOf[to.Def[T]]
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
      to.__newDef[HashJoinOp[_, _, _]](("hm", false, to.__newHashMap()(to.overloaded2, apply(mc), apply(marrBuffA))),
        ("NullDynamicRecord", false, to.infix_asInstanceOf(to.unit[Any](null))(apply(mCompRec))),
        ("tmpCount", true, to.unit[Int](-1)),
        ("tmpLine", true, to.infix_asInstanceOf(to.unit[Any](null))(apply(mba))),
        ("tmpBuffer", true, to.ArrayBuffer()(apply(ma)))).asInstanceOf[to.Def[T]]
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
      to.__newDef[WindowOp[_, _, _]](("hm", false, to.__newHashMap()(to.overloaded2, apply(mb), apply(marrBuffA))),
        ("NullDynamicRecord", false, to.infix_asInstanceOf(to.unit[Any](null))(apply(mwinRecBC))),
        ("keySet", true, to.Set()(apply(mb), to.overloaded2))).asInstanceOf[to.Def[T]]
    }
    case pc @ PardisCast(exp) => {
      // System.out.print("--->")
      // System.out.print(pc)
      // System.out.print("<---")
      // System.out.println(transformType(pc.castTp))
      PardisCast(transformExp[Any, Any](exp))(transformType(exp.tp), transformType(pc.castTp)).asInstanceOf[to.Def[T]]
    }
    case ab @ ArrayBufferNew2_2() => {
      ArrayBufferNew2_2()(transformType(ab.manifestA)).asInstanceOf[to.Def[T]]
    }
    case hm @ HashMapNew2_2() => {
      HashMapNew2_2()(transformType(hm.manifestA), transformType(hm.manifestB)).asInstanceOf[to.Def[T]]
    }
    // case PardisLambda(f, i, o) => {
    //   val newI = newSym(i)
    //   subst += i -> newI
    //   System.err.println(s"-->${newI.id}")
    //   System.err.println(s"tp " + newI.tp)
    //   System.err.println(s"manToString" + pardis.utils.Utils.manifestToString(newI.tp))
    //   val newO = transformBlockTyped(o).asInstanceOf[Block[Any]]
    //   to.Lambda(f, newI, newO)
    // }
    case _ => super.transformDef(node)
  }

  // WindowRecord
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
        case _: WindowRecordNew[_, _] | _: GroupByClassNew | _: LINEITEMRecordNew | _: SUPPLIERRecordNew | _: PARTSUPPRecordNew | _: REGIONRecordNew | _: PARTRecordNew | _: NATIONRecordNew | _: CUSTOMERRecordNew | _: ORDERSRecordNew => Some(exp)
        case _ => None
      }
  }

  object LoweredNew extends RepExtractor {
    def unapply[T](exp: Rep[T]): Option[Def[T]] = exp match {
      case Def(d) => d match {
        case _ if List(classOf[WindowRecord[_, _]], classOf[GroupByClass], classOf[LINEITEMRecord], classOf[SUPPLIERRecord], classOf[PARTSUPPRecord], classOf[REGIONRecord], classOf[PARTRecord], classOf[NATIONRecord], classOf[CUSTOMERRecord], classOf[ORDERSRecord], classOf[AggOp[_, _]], classOf[PrintOp[_]], classOf[ScanOp[_]], classOf[MapOp[_]], classOf[SelectOp[_]], classOf[SortOp[_]], classOf[HashJoinOp[_, _, _]], classOf[WindowOp[_, _, _]]).contains(d.tp.runtimeClass) => Some(d)
        case _ => None
      }
      case _ => None
    }
  }
}
