package ch.epfl.data
package dblab.legobase
package optimization

import scala.reflect.runtime.universe.{ typeTag => tag }
import dblab.legobase.deep._
import scala.language.implicitConversions
import sc.pardis.ir._
import sc.pardis.types._
import sc.pardis.types.PardisTypeImplicits._
import sc.pardis.optimization._

object LBLowering {
  def apply(removeUnusedFields: Boolean) = new TransformerHandler {
    def apply[Lang <: Base, T: PardisType](context: Lang)(block: context.Block[T]): context.Block[T] = {
      val lbContext = context.asInstanceOf[LoweringLegoBase]
      val lbLowering = new LBLowering(lbContext, lbContext, removeUnusedFields)
      val newBlock = lbLowering.lower(block)
      new LBLoweringPostProcess(lbContext, lbLowering).optimize(newBlock)
    }
  }
}

/**
 * Lowers the query operators as well as the case class records for the relation
 * records and intermediate records to structs.
 * Furthermore, while lowering the case class records, finds the fields that are
 * unused and if `removeUnusedFields` is true, removes them.
 * For example:
 * {{{
 *    val rec = new Record { var fieldA: Int, val fieldB: String }
 *    rec.fieldA = 2
 *    // fieldB is not used
 * }}}
 * is converted to:
 * {{{
 *    val rec = new Record { var fieldA: Int }
 *    rec.fieldA = 2
 *    // fieldB is not used
 * }}}
 */
class LBLowering(override val from: LoweringLegoBase, override val to: LoweringLegoBase, val removeUnusedFields: Boolean) extends Lowering[LoweringLegoBase, LoweringLegoBase](from, to) {
  import from._

  // override val lowerStructs: Boolean = false
  def stop = ("stop", true, unit(false))

  sealed trait Phase
  case object FieldExtractionPhase extends Phase
  case object FieldUsagePhase extends Phase
  case object OtherPhase extends Phase

  var phase: Phase = _

  val fieldsAccessed = collection.mutable.Map[StructTags.StructTag[_], ArrayBuffer[String]]()

  def getRegisteredFieldsOfType[A](t: PardisType[A]): List[String] = {
    val registeredFields = t match {
      case DynamicCompositeRecordType(l, r) =>
        manifestTags(getType(t)) match {
          case tag @ StructTags.CompositeTag(la, ra, ltag, rtag) =>
            getRegisteredFieldsOfType(l).map(la + _) ++ getRegisteredFieldsOfType(r).map(ra + _)
        }
      case _ =>
        manifestTags.get(getType(t)).flatMap(x => fieldsAccessed.get(x)) match {
          case Some(x) => x
          case None    => List()
        }
    }
    registeredFields.toList
  }

  def registerField[A](t: PardisType[A], field: String): Unit = {
    t match {
      case DynamicCompositeRecordType(l, r) =>
        manifestTags(getType(t)) match {
          case tag @ StructTags.CompositeTag(la, ra, ltag, rtag) =>
            val lstruct = structs(ltag)
            val rstruct = structs(rtag)
            if (field.startsWith(la)) {
              registerField(l, field.substring(la.size))
            }
            if (field.startsWith(ra)) {
              registerField(r, field.substring(ra.size))
            }
        }
      case _ =>
        manifestTags.get(getType(t)) match {
          case Some(tag) => structs.get(tag) match {
            case Some(s) =>
              val l = fieldsAccessed.getOrElseUpdate(tag, new ArrayBuffer())
              if (s.map(e => e.name).contains(field) && !l.contains(field)) l.append(field);
            case _ => throw new Exception(s"Tag $tag for type $t does not have corresponding struct")
          }
          case _ =>
        }
    }
  }

  override def traverseDef(node: Def[_]): Unit = node match {
    case Struct(tag, elems, methods) if phase == FieldUsagePhase =>
    case CaseClassNew(ccn) if phase == FieldUsagePhase           =>
    case StructDefault() if phase == FieldUsagePhase             =>
    case ImmutableField(self, f) if phase == FieldUsagePhase => {
      super.traverseDef(node)
      registerField(self.tp, f)
    }
    case FieldGetter(self, f) if phase == FieldUsagePhase => {
      super.traverseDef(node)
      registerField(self.tp, f)
    }
    case FieldSetter(self, f, _) if phase == FieldUsagePhase => {
      super.traverseDef(node)
      registerField(self.tp, f)
    }
    case ConcatDynamic(self, record2, leftAlias, rightAlias) if phase == FieldUsagePhase => {
      val Constant(la: String) = leftAlias
      val Constant(ra: String) = rightAlias
      val leftTag = getTag(getType(self.tp))
      val rightTag = getTag(getType(record2.tp))
      val concatTag = StructTags.CompositeTag[Any, Any](la, ra, leftTag, rightTag)
      val regFields = getRegisteredFieldsOfType(self.tp) ++ getRegisteredFieldsOfType(record2.tp)
      def fieldIsRegistered(f: StructElemInformation): Boolean = regFields.contains(f.name) || !removeUnusedFields
      val newElems = getStructElems(leftTag).filter(fieldIsRegistered).map(x => StructElemInformation(la + x.name, x.tpe, x.mutable)) ++ getStructElems(rightTag).filter(fieldIsRegistered).map(x => StructElemInformation(ra + x.name, x.tpe, x.mutable))
      structs += concatTag -> newElems
      manifestTags += getType(node.tp) -> concatTag
    }
    case _ => super.traverseDef(node)
  }

  override def lower[T: TypeRep](node: Block[T]): to.Block[T] = {
    phase = FieldExtractionPhase
    traverseBlock(node)
    phase = FieldUsagePhase
    traverseBlock(node)
    phase = OtherPhase
    val res = transformProgram(node)
    res
  }

  override def transformDef[T: TypeRep](node: Def[T]): to.Def[T] = node match {
    case an @ ArrayNew(size) =>
      ArrayNew(size)(apply(an.tp.typeArguments(0)))
    case CaseClassNew(ccn) if lowerStructs =>
      transformDef(super.transformDef(node))
    case sd @ StructDefault() if lowerStructs =>
      transformDef(super.transformDef(node))
    case ps @ PardisStruct(tag, elems, methods) =>
      val registeredFields = fieldsAccessed.get(tag)
      val newFields = registeredFields match {
        case Some(x) if removeUnusedFields => elems.filter(e => x.contains(e.name))
        case _                             => elems
      }
      val newTpe = ps.tp.asInstanceOf[TypeRep[Any]]
      super.transformDef(PardisStruct(tag, newFields, Nil)(ps.tp))(ps.tp)
    case ConcatDynamic(record1, record2, leftAlias, rightAlias) if lowerStructs => {
      val tp = node.tp.asInstanceOf[TypeRep[(Any, Any)]]
      val leftTag = getTag(getType(record1.tp))
      val rightTag = getTag(getType(record2.tp))
      val Constant(la: String) = leftAlias
      val Constant(ra: String) = rightAlias
      val concatTag = StructTags.CompositeTag[Any, Any](la, ra, leftTag, rightTag)
      def getElems[T](exp: Rep[T]): Seq[StructElemInformation] = getStructElems(manifestTags(getType(exp.tp)))
      val elems = getStructElems(concatTag)
      case class ElemInfo[T](name: String, rec: Rep[T], tp: TypeRep[Any])
      val regFields = getRegisteredFieldsOfType(record1.tp) ++ getRegisteredFieldsOfType(record2.tp)
      def fieldIsRegistered(f: StructElemInformation): Boolean = regFields.contains(f.name) || !removeUnusedFields
      val elemsRhs = getElems(record1).filter(fieldIsRegistered).map(x => ElemInfo(x.name, record1, x.tpe)) ++ getElems(record2).filter(fieldIsRegistered).map(x => ElemInfo(x.name, record2, x.tpe))
      // Amir: FIXME should handle both cases for mutable and immutable fields (immutable and getter)
      val structFields = elems.zip(elemsRhs).map(x => PardisStructArg(x._1.name, x._1.mutable, to.toAtom(StructImmutableField(x._2.rec, x._2.name)(x._2.tp))(x._2.tp)))
      val newTpe = new RecordType(concatTag, Some(tp))
      super.transformDef(PardisStruct(concatTag, structFields, Nil)(newTpe).asInstanceOf[to.Def[T]])
    }

    case ag: AggOpNew[_, _] => {
      val ma = ag.typeA
      val mb = ag.typeB
      val maa = ma.asInstanceOf[TypeRep[Any]]
      val marrDouble = implicitly[to.TypeRep[to.Array[to.Double]]]
      val magg = typeRep[AGGRecord[Any]].rebuild(mb).asInstanceOf[TypeRep[Any]]
      // val hm = to.__newHashMap4[Any, Any](ag.grp.asInstanceOf[Rep[Any => Any]], unit(1048576))(apply(mb), apply(magg.asInstanceOf[TypeRep[Any]]))
      val hm = to.__newHashMap[Any, Any]()(apply(mb), apply(magg.asInstanceOf[TypeRep[Any]]))
      to.__newDef[AggOp[Any, Any]](("hm", false, hm),
        // ("keySet", true, to.Set()(apply(mb), to.overloaded2)),
        stop).asInstanceOf[to.Def[T]]
    }
    case po: PrintOpNew[_] => {
      val ma = po.typeA
      val maa = ma.asInstanceOf[TypeRep[Any]]
      to.__newDef[PrintOp[Any]](("numRows", true, to.unit[Int](0)),
        stop).asInstanceOf[to.Def[T]]
    }
    case so: ScanOpNew[_] => {
      val ma = so.typeA
      val maa = ma.asInstanceOf[TypeRep[Any]]
      to.__newDef[ScanOp[Any]](("i", true, to.unit[Int](0)),
        stop).asInstanceOf[to.Def[T]]
    }
    case mo: MapOpNew[_] => {
      val ma = mo.typeA
      val maa = ma.asInstanceOf[TypeRep[Any]]
      to.__newDef[MapOp[Any]](
        stop).asInstanceOf[to.Def[T]]
    }
    case so: SelectOpNew[_] => {
      val ma = so.typeA
      val maa = ma.asInstanceOf[TypeRep[Any]]
      to.__newDef[SelectOp[Any]](
        stop).asInstanceOf[to.Def[T]]
    }
    case so: SortOpNew[_] => {
      val ma = so.typeA
      val maa = ma.asInstanceOf[TypeRep[Any]]
      to.__newDef[SortOp[Any]](
        ("sortedTree", false, to.__newTreeSet2(to.Ordering[Any](apply(so.orderingFunc.asInstanceOf[Rep[(Any, Any) => Int]]))(apply(maa)))(apply(maa))),
        stop).asInstanceOf[to.Def[T]]
    }
    case ho: HashJoinOpNew1[_, _, _] => {
      val ma = ho.typeA
      val mb = ho.typeB
      val mc = ho.typeC
      val mba = mb.asInstanceOf[TypeRep[Any]]
      type HashJoinOpTp = HashJoinOp[sc.pardis.shallow.Record, sc.pardis.shallow.Record, Any]
      val tp = ho.tp.asInstanceOf[TypeRep[HashJoinOpTp]]
      val marrBuffA = implicitly[TypeRep[ArrayBuffer[Any]]].rebuild(ma).asInstanceOf[TypeRep[Any]]
      val mCompRec = implicitly[TypeRep[DynamicCompositeRecord[sc.pardis.shallow.Record, sc.pardis.shallow.Record]]].rebuild(ma, mb).asInstanceOf[TypeRep[Any]]
      to.__newDef[HashJoinOpTp](
        ("hm", false, to.__newMultiMap[Any, Any]()(apply(mc), apply(ma.asInstanceOf[TypeRep[Any]]))),
        stop)(tp).asInstanceOf[to.Def[T]]

    }
    case wo: WindowOpNew[_, _, _] => {
      val ma = wo.typeA
      val mb = wo.typeB
      val mc = wo.typeC
      val maa = ma.asInstanceOf[TypeRep[Any]]
      val marrBuffA = implicitly[TypeRep[ArrayBuffer[Any]]].rebuild(ma).asInstanceOf[TypeRep[Any]]
      val mwinRecBC = implicitly[TypeRep[WindowRecord[Any, Any]]].rebuild(mb, mc).asInstanceOf[TypeRep[Any]]
      to.__newDef[WindowOp[Any, Any, Any]]( //("hm", false, to.__newHashMap()(to.overloaded2, apply(mb), apply(marrBuffA))),
        // ("hm", false, to.__newHashMap3[Any, Any](wo.grp.asInstanceOf[Rep[Any => Any]], newSize * 100)(apply(mb), apply(ma))),
        ("hm", false, to.__newMultiMap[Any, Any]()(apply(mb), apply(ma))),
        stop).asInstanceOf[to.Def[T]]
    }
    case lho: LeftHashSemiJoinOpNew[_, _, _] => {
      val ma = lho.typeA
      val mb = lho.typeB
      val mc = lho.typeC
      val maa = ma.asInstanceOf[TypeRep[Any]]
      val marrBuffB = implicitly[TypeRep[ArrayBuffer[Any]]].rebuild(mb).asInstanceOf[TypeRep[Any]]
      to.__newDef[LeftHashSemiJoinOp[Any, Any, Any]]( //("hm", false, to.__newHashMap()(to.overloaded2, apply(mc), apply(marrBuffB))),
        // ("hm", false, to.__newHashMap3[Any, Any](lho.rightHash.asInstanceOf[Rep[Any => Any]], newSize * 100)(apply(mc), apply(mb))),
        ("hm", false, to.__newMultiMap[Any, Any]()(apply(mc), apply(mb))),
        stop).asInstanceOf[to.Def[T]]
    }
    case nlo: NestedLoopsJoinOpNew[_, _] => {
      val ma = nlo.typeA
      val mb = nlo.typeB
      type NestedLoopsJoinOpTp = NestedLoopsJoinOp[sc.pardis.shallow.Record, sc.pardis.shallow.Record]
      val tp = nlo.tp.asInstanceOf[TypeRep[NestedLoopsJoinOpTp]]
      val mCompRec = implicitly[TypeRep[DynamicCompositeRecord[sc.pardis.shallow.Record, sc.pardis.shallow.Record]]].rebuild(ma, mb).asInstanceOf[TypeRep[Any]]
      to.__newDef[NestedLoopsJoinOpTp](("leftTuple", true, to.infix_asInstanceOf(to.unit[Any](null))(apply(ma))),
        stop)(tp).asInstanceOf[to.Def[T]]
    }
    case vo: ViewOpNew[_] => {
      val ma = vo.typeA
      to.__newDef[ViewOp[Any]](
        stop,
        ("size", true, to.unit[Int](0)),
        ("table", false, to.arrayNew(48000000)(apply(ma)))).asInstanceOf[to.Def[T]] // TODO-GEN: use statistics here as well
    }
    case sr: SubquerySingleResultNew[_] => {
      val ma = sr.typeA
      to.__newDef[SubquerySingleResult[Any]](("result", true, to.infix_asInstanceOf(to.unit[Any](null))(apply(ma))),
        stop).asInstanceOf[to.Def[T]]
    }
    case ho: HashJoinAntiNew[_, _, _] => {
      val ma = ho.typeA
      val mb = ho.typeB
      val mc = ho.typeC
      val mba = mb.asInstanceOf[TypeRep[Any]]
      val marrBuffA = implicitly[TypeRep[ArrayBuffer[Any]]].rebuild(ma).asInstanceOf[TypeRep[Any]]
      to.__newDef[HashJoinAnti[Any, Any, Any]]( //("hm", false, to.__newHashMap()(to.overloaded2, apply(mc), apply(marrBuffA))),
        // ("hm", false, to.__newHashMap3[Any, Any](ho.leftHash.asInstanceOf[Rep[Any => Any]], newSize)(apply(mc), apply(ma))),
        ("hm", false, to.__newMultiMap[Any, Any]()(apply(mc), apply(ma))),
        stop).asInstanceOf[to.Def[T]]
    }
    case loj: LeftOuterJoinOpNew[_, _, _] => {
      val ma = loj.typeA
      val mb = loj.typeB
      val mc = loj.typeC
      val maa = ma.asInstanceOf[TypeRep[Any]]
      val mba = mb.asInstanceOf[TypeRep[Any]]
      type LeftOuterJoinOpTp = LeftOuterJoinOp[sc.pardis.shallow.Record, sc.pardis.shallow.Record, Any]
      val tp = loj.tp.asInstanceOf[TypeRep[LeftOuterJoinOpTp]]
      val marrBuffB = implicitly[TypeRep[ArrayBuffer[Any]]].rebuild(mb).asInstanceOf[TypeRep[Any]]
      val mCompRec = implicitly[TypeRep[DynamicCompositeRecord[sc.pardis.shallow.Record, sc.pardis.shallow.Record]]].rebuild(ma, mb).asInstanceOf[TypeRep[Any]]
      val dflt = toAtom(transformDef(to.StructDefault()(mb))(mb))(mb)
      val hm = to.__newMultiMap[Any, Any]()(apply(mc), apply(mb))
      to.addLoweredSymbolOriginalDef(hm, loj)
      to.__newDef[LeftOuterJoinOpTp](
        ("hm", false, hm),
        stop,
        ("defaultB", false, dflt))(tp).asInstanceOf[to.Def[T]]
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
    def unapply[T](exp: Rep[T]): Option[Rep[T]] = exp.tp match {
      case x if x.isRecord => Some(exp)
      case LeftHashSemiJoinOpType(_, _, _) | HashJoinOpType(_, _, _) | WindowOpType(_, _, _) | AggOpType(_, _) | PrintOpType(_) | ScanOpType(_) | MapOpType(_) | SelectOpType(_) | SortOpType(_) | NestedLoopsJoinOpType(_, _) | SubquerySingleResultType(_) | ViewOpType(_) | HashJoinAntiType(_, _, _) | LeftOuterJoinOpType(_, _, _) => Some(exp)
      case _ => None
    }
  }
}

class LBLoweringPostProcess(override val IR: LoweringLegoBase, val lbLowering: LBLowering) extends RuleBasedTransformer[LoweringLegoBase](IR) {
  import IR._

  rewrite += statement {
    case sym -> StructImmutableField(obj, name) if sym.tp.isRecord && !sym.tp.isInstanceOf[RecordType[_]] =>
      import lbLowering.{ createTag, getType }
      val tag = createTag(getType(sym.tp))
      val newTp = new RecordType(tag, Some(sym.tp.asInstanceOf[TypeRep[Any]])).asInstanceOf[TypeRep[Any]]
      field(obj, name)(newTp)
  }
}