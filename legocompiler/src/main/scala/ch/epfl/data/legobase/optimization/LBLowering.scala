package ch.epfl.data
package legobase
package optimization

import legobase.deep._
import scala.language.implicitConversions
import pardis.ir._
import pardis.optimization._

trait LBLowering extends TopDownTransformer[InliningLegoBase, LoweringLegoBase] {
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
    case CaseClassNew(ccn) => {
      val tp = node.tp.asInstanceOf[Manifest[Any]]
      val tpe = pardis.utils.Utils.manifestToType(tp)
      val fields = tpe.members.toList.map(_.asTerm).filter(_.isVal)
      import scala.language.reflectiveCalls
      /** adopted from http://stackoverflow.com/questions/1589603/scala-set-a-field-value-reflectively-from-field-name */
      implicit def reflector(ref: AnyRef) = new {
        def getV(name: String): Any = {
          val field = ref.getClass.getDeclaredFields.toList.find(x => x.getName == name).get
          field.setAccessible(true)
          field.get(ref)
        }
      }
      def normalizeFieldName(originalName: String): String = if (originalName.endsWith(" ")) originalName.dropRight(1) else originalName
      val structFields = fields.map(x => (normalizeFieldName(x.name.toString), false, ccn.getV(normalizeFieldName(x.name.toString)).asInstanceOf[Rep[Any]]))
      to.__newDef[Any](structFields: _*)(tp).asInstanceOf[to.Def[T]]
    }
    case ImmutableField(self @ LoweredNew(d), fieldName) => {
      StructImmutableField(transformExp(self), fieldName)
    }
    case FieldGetter(self @ LoweredNew(d), fieldName) => {
      StructFieldGetter(transformExp(self), fieldName)
    }
    case FieldSetter(self @ LoweredNew(d), fieldName, rhs) => {
      StructFieldSetter[T](transformExp(self), fieldName, rhs).asInstanceOf[to.Def[T]]
    }
    case _ => super.transformDef(node)
  }

  object ImmutableField {
    def unapply[T](exp: Def[T]): Option[(Rep[Any], String)] = exp match {
      case fd: FieldDef[_] => Some(fd.obj -> fd.field)
      case _               => None
    }
  }

  object FieldSetter {
    def unapply[T](exp: Def[T]): Option[(Rep[Any], String, Rep[T])] = exp match {
      case fd: FieldSetter[_] => Some((fd.obj, fd.field, fd.newValue.asInstanceOf[Rep[T]]))
      case _                  => None
    }
  }

  object FieldGetter {
    def unapply[T](exp: Def[T]): Option[(Rep[Any], String)] = exp match {
      case fd: FieldGetter[_] => Some(fd.obj -> fd.field)
      case _                  => None
    }
  }

  object CaseClassNew {
    def unapply[T](exp: Def[T]): Option[Def[T]] =
      exp match {
        case _: GroupByClassNew | _: LINEITEMRecordNew | _: SUPPLIERRecordNew | _: PARTSUPPRecordNew => Some(exp)
        case _ => None
      }
  }

  object LoweredNew {
    def unapply[T](exp: Rep[T]): Option[Def[T]] = exp match {
      case Def(d) => d match {
        case _ if List(classOf[PARTSUPPRecord], classOf[SUPPLIERRecord], classOf[LINEITEMRecord], classOf[AggOp[_, _]], classOf[PrintOp[_]], classOf[ScanOp[_]], classOf[MapOp[_]], classOf[SelectOp[_]], classOf[SortOp[_]], classOf[HashJoinOp[_, _, _]], classOf[WindowOp[_, _, _]], classOf[GroupByClass]).contains(d.tp.runtimeClass) => Some(d)
        case _ => None
      }
      case _ => None
    }
  }
}
