package ch.epfl.data
package legobase
package optimization

import scala.reflect.runtime.universe.{ typeTag => tag }
import legobase.deep._
import scala.language.implicitConversions
import pardis.ir._
import pardis.ir.pardisTypeImplicits._
import pardis.optimization._

class LBLowering(override val from: InliningLegoBase, override val to: LoweringLegoBase) extends Lowering[InliningLegoBase, LoweringLegoBase](from, to) {
  import from._

  override def transformDef[T: TypeRep](node: Def[T]): to.Def[T] = node match {
    case an: AggOpNew[_, _] => {
      val ma = an.typeA
      val mb = an.typeB
      val maa = ma.asInstanceOf[TypeRep[Any]]
      val marrDouble = implicitly[to.TypeRep[to.Array[to.Double]]]
      val magg = typeRep[AGGRecord[Any]].rebuild(mb).asInstanceOf[TypeRep[Any]]
      val hm = to.__newHashMap()(to.overloaded2, apply(mb), apply(marrDouble))
      to.__newDef[AggOp[Any, Any]](("hm", false, hm),
        ("NullDynamicRecord", false, to.infix_asInstanceOf(to.unit[Any](null))(apply(magg))),
        ("keySet", true, to.Set()(apply(mb), to.overloaded2)),
        ("numAggs", false, an.numAggs)).asInstanceOf[to.Def[T]]
    }
    case po: PrintOpNew[_] => {
      val ma = po.typeA
      val maa = ma.asInstanceOf[TypeRep[Any]]
      to.__newDef[PrintOp[Any]](("numRows", true, to.unit[Int](0)),
        ("NullDynamicRecord", false, to.infix_asInstanceOf(to.unit[Any](null))(apply(maa)))).asInstanceOf[to.Def[T]]
    }
    case so: ScanOpNew[_] => {
      val ma = so.typeA
      val maa = ma.asInstanceOf[TypeRep[Any]]
      to.__newDef[ScanOp[Any]](("i", true, to.unit[Int](0)),
        ("table", false, so.table),
        ("NullDynamicRecord", false, to.infix_asInstanceOf(to.unit[Any](null))(apply(maa)))).asInstanceOf[to.Def[T]]
    }
    case mo: MapOpNew[_] => {
      val ma = mo.typeA
      val maa = ma.asInstanceOf[TypeRep[Any]]
      to.__newDef[MapOp[Any]](
        ("NullDynamicRecord", false, to.infix_asInstanceOf(to.unit[Any](null))(apply(maa)))).asInstanceOf[to.Def[T]]
    }
    case so: SelectOpNew[_] => {
      val ma = so.typeA
      val maa = ma.asInstanceOf[TypeRep[Any]]
      to.__newDef[SelectOp[Any]](
        ("NullDynamicRecord", false, to.infix_asInstanceOf(to.unit[Any](null))(apply(maa)))).asInstanceOf[to.Def[T]]
    }
    case so: SortOpNew[_] => {
      val ma = so.typeA
      val maa = ma.asInstanceOf[TypeRep[Any]]
      to.__newDef[SortOp[Any]](("sortedTree", false, to.__newTreeSet2(to.Ordering[Any](apply(so.orderingFunc.asInstanceOf[Rep[(Any, Any) => Int]]))(apply(maa)))(apply(maa))),
        ("NullDynamicRecord", false, to.infix_asInstanceOf(to.unit[Any](null))(apply(maa)))).asInstanceOf[to.Def[T]]
    }
    case ho: HashJoinOpNew[_, _, _] => {
      val ma = ho.typeA
      val mb = ho.typeB
      val mc = ho.typeC
      val mba = mb.asInstanceOf[TypeRep[Any]]
      type HashJoinOpTp = HashJoinOp[pardis.shallow.AbstractRecord, pardis.shallow.AbstractRecord, Any]
      val tp = ho.tp.asInstanceOf[TypeRep[HashJoinOpTp]]
      val marrBuffA = implicitly[TypeRep[ArrayBuffer[Any]]].rebuild(ma).asInstanceOf[TypeRep[Any]]
      val mCompRec = implicitly[TypeRep[DynamicCompositeRecord[pardis.shallow.AbstractRecord, pardis.shallow.AbstractRecord]]].rebuild(ma, mb).asInstanceOf[TypeRep[Any]]
      to.__newDef[HashJoinOpTp](("hm", false, to.__newHashMap()(to.overloaded2, apply(mc), apply(marrBuffA))),
        ("NullDynamicRecord", false, to.infix_asInstanceOf(to.unit[Any](null))(apply(mCompRec))),
        ("tmpCount", true, to.unit[Int](-1)),
        ("tmpLine", true, to.infix_asInstanceOf(to.unit[Any](null))(apply(mba))),
        ("tmpBuffer", true, to.ArrayBuffer()(apply(ma))))(tp).asInstanceOf[to.Def[T]]
    }
    case wo: WindowOpNew[_, _, _] => {
      val ma = wo.typeA
      val mb = wo.typeB
      val mc = wo.typeC
      val maa = ma.asInstanceOf[TypeRep[Any]]
      val marrBuffA = implicitly[TypeRep[ArrayBuffer[Any]]].rebuild(ma).asInstanceOf[TypeRep[Any]]
      val mwinRecBC = implicitly[TypeRep[WindowRecord[Any, Any]]].rebuild(mb, mc).asInstanceOf[TypeRep[Any]]
      to.__newDef[WindowOp[Any, Any, Any]](("hm", false, to.__newHashMap()(to.overloaded2, apply(mb), apply(marrBuffA))),
        ("NullDynamicRecord", false, to.infix_asInstanceOf(to.unit[Any](null))(apply(mwinRecBC))),
        ("keySet", true, to.Set()(apply(mb), to.overloaded2))).asInstanceOf[to.Def[T]]
    }
    case lho: LeftHashSemiJoinOpNew[_, _, _] => {
      val ma = lho.typeA
      val mb = lho.typeB
      val mc = lho.typeC
      val maa = ma.asInstanceOf[TypeRep[Any]]
      val marrBuffB = implicitly[TypeRep[ArrayBuffer[Any]]].rebuild(mb).asInstanceOf[TypeRep[Any]]
      to.__newDef[LeftHashSemiJoinOp[Any, Any, Any]](("hm", false, to.__newHashMap()(to.overloaded2, apply(mc), apply(marrBuffB))),
        ("NullDynamicRecord", false, to.infix_asInstanceOf(to.unit[Any](null))(apply(ma)))).asInstanceOf[to.Def[T]]
    }
    case pc @ PardisCast(exp) => {
      PardisCast(transformExp[Any, Any](exp))(transformType(exp.tp), transformType(pc.castTp)).asInstanceOf[to.Def[T]]
    }
    case ab @ ArrayBufferNew2() => {
      ArrayBufferNew2()(transformType(ab.typeA)).asInstanceOf[to.Def[T]]
    }
    case _ => super.transformDef(node)
  }

  object CaseClassNew extends DefExtractor {
    def unapply[T](exp: Def[T]): Option[Def[T]] =
      exp match {
        case _: ConstructorDef[_] if exp.tp.isRecord => Some(exp)
        case _                                       => None
      }
  }

  object LoweredNew extends RepExtractor {
    def unapply[T](exp: Rep[T]): Option[Def[T]] = exp match {
      case Def(d) => d.tp match {
        case x if x.isRecord => Some(d)
        case LeftHashSemiJoinOpType(_, _, _) | HashJoinOpType(_, _, _) | WindowOpType(_, _, _) | AggOpType(_, _) | PrintOpType(_) | ScanOpType(_) | MapOpType(_) | SelectOpType(_) | SortOpType(_) => Some(d)
        case _ => None
      }
      case _ => None
    }
  }
}
