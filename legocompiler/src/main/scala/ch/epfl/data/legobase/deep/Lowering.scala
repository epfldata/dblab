package ch.epfl.data
package legobase
package deep

import scala.language.implicitConversions
import pardis.ir._

trait Lowering extends TopDownTransformer[InliningLegoBase, LoweringLegoBase] {
  import from._

  override def transformDef[T: Manifest](node: Def[T]): to.Def[T] = node match {
    case RunQuery(b)                                 => to.RunQuery(transformBlock(b))
    case HashMapGetOrElseUpdate(self, key, opOutput) => to.HashMapGetOrElseUpdate(transformExp(self)(self.tp, self.tp), transformExp(key), transformBlock(opOutput))
    case an: AggOpNew[_, _] => {
      val ma = an.manifestA
      val mb = an.manifestB
      val maa = ma.asInstanceOf[Manifest[Any]]
      val marrDouble = manifest[Array[Double]]
      class Rec
      implicit val manifestRec: Manifest[Rec] = mb.asInstanceOf[Manifest[Rec]]
      val magg = manifest[AGGRecord[Rec]].asInstanceOf[Manifest[Any]]
      // val magg = maa
      // to.reifyBlock ({
      to.__newDef[AggOp[_, _]](("hm", false, to.__newHashMap(to.overloaded2, mb, marrDouble)),
        ("NullDynamicRecord", false, unit[Any](null)(magg)),
        ("keySet", true, to.Set()(mb)),
        ("numAggs", false, an.numAggs)).asInstanceOf[to.Def[T]]
      // }).correspondingNode
    }
    case po: PrintOpNew[_] => {
      val ma = po.manifestA
      val maa = ma.asInstanceOf[Manifest[Any]]
      // to.reifyBlock({
      to.__newDef[PrintOp[_]](("numRows", true, to.unit[Int](0)),
        ("NullDynamicRecord", false, unit[Any](null)(maa))).asInstanceOf[to.Def[T]]
      // }).correspondingNode.asInstanceOf[to.Def[T]]
    }
    case so: ScanOpNew[_] => {
      val ma = so.manifestA
      val maa = ma.asInstanceOf[Manifest[Any]]
      // to.reifyBlock({
      to.__newDef[ScanOp[_]](("i", true, to.unit[Int](0)),
        ("table", false, so.table),
        ("NullDynamicRecord", false, unit[Any](null)(maa))).asInstanceOf[to.Def[T]]
      // }).correspondingNode.asInstanceOf[to.Def[T]]
    }
    case mo: MapOpNew[_] => {
      val ma = mo.manifestA
      val maa = ma.asInstanceOf[Manifest[Any]]
      // to.reifyBlock({
      to.__newDef[MapOp[_]](
        ("NullDynamicRecord", false, unit[Any](null)(maa))).asInstanceOf[to.Def[T]]
      // }).correspondingNode.asInstanceOf[to.Def[T]]
    }
    case so: SelectOpNew[_] => {
      val ma = so.manifestA
      val maa = ma.asInstanceOf[Manifest[Any]]
      // to.reifyBlock({
      to.__newDef[SelectOp[_]](
        ("NullDynamicRecord", false, unit[Any](null)(maa))).asInstanceOf[to.Def[T]]
      // }).correspondingNode.asInstanceOf[to.Def[T]]
    }
    case so: SortOpNew[_] => {
      val ma = so.manifestA
      val maa = ma.asInstanceOf[Manifest[Any]]
      // to.reifyBlock({
      to.__newDef[SortOp[_]](("sortedTree", false, to.__newTreeSet2(to.Ordering[Any](so.orderingFunc.asInstanceOf[Rep[(Any, Any) => Int]])(maa))(maa)),
        ("NullDynamicRecord", false, unit[Any](null)(maa))).asInstanceOf[to.Def[T]]
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
    case ImmutableField(self @ LoweredNew(d), fieldName) => {
      StructImmutableField(transformExp(self), fieldName)
    }
    case FieldGetter(self @ LoweredNew(d), fieldName) => {
      StructFieldGetter(transformExp(self), fieldName)
    }
    case FieldSetter(self @ LoweredNew(d), fieldName, rhs) => {
      StructFieldSetter[T](transformExp(self), fieldName, rhs).asInstanceOf[to.Def[T]]
    }
    case AGGRecord_Field_Aggs(self) => {
      Predef.println("here")
      super.transformDef(node)
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
      case Def(d) => d match {
        case _ if List(classOf[LINEITEMRecord], classOf[AggOp[_, _]], classOf[PrintOp[_]], classOf[ScanOp[_]], classOf[MapOp[_]], classOf[SelectOp[_]], classOf[SortOp[_]], classOf[GroupByClass]).contains(d.tp.runtimeClass) => Some(d)
        case _ => None
      }
      case _ => None
    }
  }

  // override def transformStmToMultiple(stm: Stm[_]): List[to.Stm[_]] = stm match {
  //   case Stm(sym, an: AggOpNew[_, _]) => {
  //     val ma = an.manifestA
  //     val mb = an.manifestB
  //     val marrDouble = manifest[Array[Double]]
  //     val hm = to.__newHashMap(to.overloaded2, mb, marrDouble)
  //     val NullDynamicRecord = unit[Any](null)(ma.asInstanceOf[Manifest[Any]])
  //     val keySet = reifyBlock {
  //       __newVar(to.Set[Any]()(mb.asInstanceOf[Manifest[Any]]))
  //     }
  //     List(statementFromExp(hm), statementFromExp(keySet))
  //   }
  //   case _ =>
  //     super.transformStmToMultiple(stm)
  // }

  // def statementFromExp(exp: Rep[_]): Stm[_] = exp match {
  //   case s @ Sym(_) => Stm(s, s.correspondingNode)
  //   // case c @ Constant(v) => Stm(fresh(c.tp), c)
  // }
}

trait LoweringLegoBase extends InliningLegoBase with pardis.ir.InlineFunctions {
}
