package ch.epfl.data
package legobase
package optimization

import scala.reflect.runtime.universe.{ typeTag => tag }
import legobase.deep._
import scala.language.implicitConversions
import pardis.ir._
import pardis.ir.pardisTypeImplicits._
import pardis.optimization._

trait LBLowering extends TopDownTransformer[InliningLegoBase, LoweringLegoBase] {
  import from._

  override def transformDef[T: TypeRep](node: Def[T]): to.Def[T] = node match {
    case an: AggOpNew[_, _] => {
      val ma = an.typeA
      val mb = an.typeB
      val maa = ma.asInstanceOf[TypeRep[Any]]
      val marrDouble = implicitly[to.TypeRep[to.Array[to.Double]]]
      class Rec
      case object RecType extends TypeRep[Rec] {
        def rebuild(newArguments: TypeRep[_]*): TypeRep[_] = RecType
        val name = "Rec"
        val typeArguments = Nil
        val typeTag = ??? //TODO: tag[Rec]
      }

      implicit val manifestRec: TypeRep[Rec] = mb.asInstanceOf[TypeRep[Rec]]
      val magg = implicitly[TypeRep[AGGRecord[Rec]]].asInstanceOf[TypeRep[Any]]
      // val magg = maa
      // to.reifyBlock ({
      to.__newDef[AggOp[_, _]](("hm", false, to.__newHashMap[Rec, to.Array[to.Double]]()(to.overloaded2, mb.asInstanceOf[to.TypeRep[Rec]], marrDouble)),
        ("NullDynamicRecord", false, to.infix_asInstanceOf(to.unit[Any](null))(magg)),
        ("keySet", true, to.Set()(mb, to.overloaded2)),
        ("numAggs", false, an.numAggs))(an.tp.asInstanceOf[to.TypeRep[AggOp[_, _]]]).asInstanceOf[to.Def[T]]
      // }).correspondingNode
    }
    case po: PrintOpNew[_] => {
      val ma = po.typeA
      val maa = ma.asInstanceOf[TypeRep[Any]]
      // to.reifyBlock({
      to.__newDef[PrintOp[_]](("numRows", true, to.unit[Int](0)),
        ("NullDynamicRecord", false, to.infix_asInstanceOf(to.unit[Any](null))(maa)))(po.tp.asInstanceOf[to.TypeRep[PrintOp[_]]]).asInstanceOf[to.Def[T]]
      // }).correspondingNode.asInstanceOf[to.Def[T]]
    }
    case so: ScanOpNew[_] => {
      val ma = so.typeA
      val maa = ma.asInstanceOf[TypeRep[Any]]
      // to.reifyBlock({
      to.__newDef[ScanOp[_]](("i", true, to.unit[Int](0)),
        ("table", false, so.table),
        ("NullDynamicRecord", false, to.infix_asInstanceOf(to.unit[Any](null))(maa)))(so.tp.asInstanceOf[to.TypeRep[ScanOp[_]]]).asInstanceOf[to.Def[T]]
      // }).correspondingNode.asInstanceOf[to.Def[T]]
    }
    case mo: MapOpNew[_] => {
      val ma = mo.typeA
      val maa = ma.asInstanceOf[TypeRep[Any]]
      // to.reifyBlock({
      to.__newDef[MapOp[_]](
        ("NullDynamicRecord", false, to.infix_asInstanceOf(to.unit[Any](null))(maa)))(mo.tp.asInstanceOf[to.TypeRep[MapOp[_]]]).asInstanceOf[to.Def[T]]
      // }).correspondingNode.asInstanceOf[to.Def[T]]
    }
    case so: SelectOpNew[_] => {
      val ma = so.typeA
      val maa = ma.asInstanceOf[TypeRep[Any]]
      // to.reifyBlock({
      to.__newDef[SelectOp[_]](
        ("NullDynamicRecord", false, to.infix_asInstanceOf(to.unit[Any](null))(maa)))(so.tp.asInstanceOf[to.TypeRep[SelectOp[_]]]).asInstanceOf[to.Def[T]]
      // }).correspondingNode.asInstanceOf[to.Def[T]]
    }
    case so: SortOpNew[_] => {
      val ma = so.typeA
      val maa = ma.asInstanceOf[TypeRep[Any]]
      // to.reifyBlock({
      to.__newDef[SortOp[_]](("sortedTree", false, to.__newTreeSet2(to.Ordering[Any](so.orderingFunc.asInstanceOf[Rep[(Any, Any) => Int]])(maa))),
        ("NullDynamicRecord", false, to.infix_asInstanceOf(to.unit[Any](null))(maa)))(so.tp.asInstanceOf[to.TypeRep[SortOp[_]]]).asInstanceOf[to.Def[T]]
      // }).correspondingNode.asInstanceOf[to.Def[T]]
    }
    case gc: GroupByClassNew => {
      // to.reifyBlock({
      to.__newDef[GroupByClass](("L_RETURNFLAG", false, transformExp(gc.L_RETURNFLAG)),
        ("L_LINESTATUS", false, transformExp(gc.L_LINESTATUS))).asInstanceOf[to.Def[T]]
      // }).correspondingNode.asInstanceOf[to.Def[T]]
    }
    case li: LINEITEMRecordNew => {
      to.__newDef[LINEITEMRecord](("L_ORDERKEY", false, li.L_ORDERKEY),
        ("L_PARTKEY", false, li.L_PARTKEY),
        ("L_SUPPKEY", false, li.L_SUPPKEY),
        ("L_LINENUMBER", false, li.L_LINENUMBER),
        ("L_QUANTITY", false, li.L_QUANTITY),
        ("L_EXTENDEDPRICE", false, li.L_EXTENDEDPRICE),
        ("L_DISCOUNT", false, li.L_DISCOUNT),
        ("L_TAX", false, li.L_TAX),
        ("L_RETURNFLAG", false, li.L_RETURNFLAG),
        ("L_LINESTATUS", false, li.L_LINESTATUS),
        ("L_SHIPDATE", false, li.L_SHIPDATE),
        ("L_COMMITDATE", false, li.L_COMMITDATE),
        ("L_RECEIPTDATE", false, li.L_RECEIPTDATE),
        ("L_SHIPINSTRUCT", false, li.L_SHIPINSTRUCT),
        ("L_SHIPMODE", false, li.L_SHIPMODE),
        ("L_COMMENT", false, li.L_COMMENT)).asInstanceOf[to.Def[T]]
    }
    case f @ ImmutableField(self @ LoweredNew(d), fieldName) => {
      StructImmutableField(transformExp(self)(self.tp, self.tp), fieldName)(f.tp)
    }
    case fg @ FieldGetter(self @ LoweredNew(d), fieldName) => {
      StructFieldGetter(transformExp(self)(self.tp, self.tp), fieldName)(fg.tp)
    }
    case fs @ FieldSetter(self @ LoweredNew(d), fieldName, rhs) => {
      StructFieldSetter[T](transformExp(self)(self.tp, self.tp), fieldName, rhs)(fs.tp).asInstanceOf[to.Def[T]]
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

  object LoweredNew {
    def unapply[T](exp: Rep[T]): Option[Def[T]] = exp match {
      case Def(d) => d.tp match {
        case LINEITEMRecordType | AggOpType(_, _) | PrintOpType(_) | ScanOpType(_) | MapOpType(_) | SelectOpType(_) | SortOpType(_) | GroupByClassType => Some(d)
        case _ => None
      }
      case _ => None
    }
  }
}
