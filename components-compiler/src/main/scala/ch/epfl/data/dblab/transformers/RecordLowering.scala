package ch.epfl.data
package dblab
package transformers

import scala.reflect.runtime.universe.{ typeTag => tag }
import dblab.deep._
import utils.Logger
import scala.language.implicitConversions
import sc.pardis.ir._
import deep.dsls.QueryEngineExp
import sc.pardis.types._
import sc.pardis.types.PardisTypeImplicits._
import sc.pardis.optimization._
import config._

object RecordLowering {
  def apply(originalContext: QueryEngineExp, removeUnusedFields: Boolean, compliant: Boolean) = new TransformerHandler {
    val recordLowering: RecordLowering = new RecordLowering(originalContext, originalContext, removeUnusedFields, compliant)
    def apply[Lang <: Base, T: PardisType](context: Lang)(block: context.Block[T]): context.Block[T] = {
      assert(context == originalContext)
      val newBlock = recordLowering.lower(block)
      new RecordLoweringPostProcess(originalContext, recordLowering).optimize(newBlock)
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
class RecordLowering(override val from: QueryEngineExp, override val to: QueryEngineExp, val removeUnusedFields: Boolean, val compliant: Boolean) extends Lowering[QueryEngineExp, QueryEngineExp](from, to) with RecordUsageAnalysis[QueryEngineExp] {
  import from._
  val logger = Logger[RecordLowering]

  // override val lowerStructs: Boolean = false
  def stop = ("stop", true, unit(false))

  def getElems[T](tp: TypeRep[T], aliasing: String = ""): Seq[StructElemInformation] = {
    val tag = getTag(tp)
    val regFields = getRegisteredFieldsOfType(tp)
    def fieldIsRegistered(f: StructElemInformation): Boolean = regFields.contains(f.name) || !removeUnusedFields
    // logger.debug(s"getElems($tp) = ${getStructElems(tag)}\n*** $tag")
    getStructElems(tag).filter(fieldIsRegistered).map(x => StructElemInformation(aliasing + x.name, apply(x.tpe), x.mutable))
  }

  // FIXME HACK!
  override def createTag[T](tp: TypeRep[T], caseClassNew: Option[Def[T]] = None): StructTags.StructTag[Any] = {
    tp match {
      case AGGRecordType(v) if !Config.specializeEngine =>
        val elems = scala.Seq(StructElemInformation("key", v, false), StructElemInformation("aggs", typeRep[Array[Double]].asInstanceOf[TypeRep[Any]], false))

        val newTag = {
          def tpToString(tp: TypeRep[_]): String = tp match {
            case rt: ReflectionType[_] => {
              val tpe = rt.tpe
              if (tpe.typeArgs.isEmpty) tpe.typeSymbol.name.toString
              else tpe.typeSymbol.name.toString + "_" + rt.typeArguments.map(tpToString(_)).mkString("_")
            }
            case _ if tp.typeArguments.isEmpty => tp.name
            case _                             => ???
          }
          StructTags.ClassTag[Any](tpToString(tp))
        }
        structs += newTag -> elems
        manifestTags += tp -> newTag
        newTag
      case _ => super.createTag(tp, caseClassNew)
    }
  }

  def computeConcatTypeEtc(): Unit = {
    def types[T: TypeRep]: List[TypeRep[Any]] = {
      typeRep[T] match {
        case DynamicCompositeRecordType(l, r) => types(l) ++ types(r)
        case t: TypeRep[_]                    => List(t.asInstanceOf[TypeRep[Any]])
      }
    }

    val sorted = notSeenDynamicRecordTypes.toList.sortBy(x => types(x).size)
    // logger.debug(s"structs: ${structs.mkString("\n--")}")
    // logger.debug(s"notSeenDynamicRecordTypes: $notSeenDynamicRecordTypes")
    for (tp <- sorted) {
      import scala.language.existentials
      val dtp = tp.asInstanceOf[DynamicCompositeRecordType[_, _]]
      // logger.debug(s"dtp: ${dtp.rightType.asInstanceOf[ReflectionType[_]].tpe}")
      val (lt, rt) = dtp.leftType -> dtp.rightType
      val leftTag = getTag(lt)
      val rightTag = getTag(rt)
      System.err.println(s"${scala.Console.RED}Warning${scala.Console.RESET}: No tag found for type $tp. Hence assuming no aliasing!")
      val concatTag = StructTags.CompositeTag[Any, Any]("", "", leftTag, rightTag)
      val newElems = getElems(lt) ++ getElems(rt)
      // logger.debug(s"concatTag $tp -> $concatTag -> ${newElems.mkString("\n--")}")
      // logger.debug(s"lt $lt -> ${getElems(lt).mkString("\n--")}")
      // logger.debug(s"rt $rt -> ${getElems(rt).mkString("\n--")}")
      structs += concatTag -> newElems
      manifestTags += tp.asInstanceOf[TypeRep[Any]] -> concatTag
    }
    // System.out.println(s"sorted ${sorted.mkString("\n")}")
  }

  override def transformType[T: TypeRep]: TypeRep[Any] = {
    val tp = implicitly[TypeRep[T]].asInstanceOf[TypeRep[Any]]
    try {
      manifestTags.get(tp) match {
        case Some(tag) => new RecordType(tag, Some(tp)).asInstanceOf[TypeRep[Any]]
        case None if !Config.specializeEngine && tp.isRecord && !tp.isInstanceOf[RecordType[_]] => new RecordType(createTag(tp), Some(tp)).asInstanceOf[TypeRep[Any]]
        case None => super.transformType(tp)
      }
    } catch {
      case ex: NullPointerException => // this means that this type is already transformed
        super.transformType(tp)
    }

  }

  override def getTag(tp: TypeRep[_]): StructTags.StructTag[Any] = {
    manifestTags.get(tp) match {
      case Some(v) => v
      case None if !Config.specializeEngine && tp.isRecord && !tp.isInstanceOf[RecordType[_]] => createTag(tp)
      case None => super.getTag(tp)
    }
  }

  // override def createTag[T](tp: TypeRep[T], caseClassNew: Option[Def[T]] = None): StructTags.StructTag[Any] = {
  //   if (isOperatorType(tp)) {
  //     super.createTag(tp, caseClassNew)
  //   } else {
  //     super.createTag(tp, caseClassNew)
  //   }
  // }

  override def lower[T: TypeRep](node: Block[T]): to.Block[T] = {
    phase = FieldExtractionPhase
    traverseBlock(node)
    phase = FieldUsagePhase
    traverseBlock(node)
    phase = OtherPhase
    // logger.debug(s"tags: $manifestTags")
    // logger.debug(s"dtypes: $notSeenDynamicRecordTypes")
    computeConcatTypeEtc()
    val res = transformProgram(node)
    res
  }

  object OperatorTags {
    val AggOp = 1
    val PrintOp = 2
    val ScanOp = 3
    val SelectOp = 4
    val SortOp = 5
    val MapOp = 6
    val HashJoinOp = 7
    val MergeJoinOp = 8
    val WindowOp = 9
    val LeftHashSemiJoinOp = 10
  }

  // TODO refactor with StructProcessing
  // def getRegisteredFieldsOfType[A: TypeRep]: List[String] = {
  //   typeRep[A] match {
  //     case rt: RecordType[_] if rt.originalType.nonEmpty => getRegisteredFieldsOfType(rt.originalType.get)
  //     case t => getRegisteredFieldsOfType(t)
  //   }
  // }

  // TODO refactor with StructProcessing
  def concat_records[T: TypeRep, S: TypeRep, Res: TypeRep](elem1: Rep[T], elem2: Rep[S]): Rep[Res] = {
    val resultType = typeRep[Res]
    val regFields = getRegisteredFieldsOfType(elem1.tp) ++ getRegisteredFieldsOfType(elem2.tp)
    def getFields[TR: TypeRep] = getElems(typeRep[TR] match {
      case rt: RecordType[_] if rt.originalType.nonEmpty => rt.originalType.get
      case t => t
    })
    // System.out.println(s"regFields: $regFields")
    def fieldIsRegistered(f: StructElemInformation): Boolean = regFields.contains(f.name) || !removeUnusedFields
    val elems1 = getFields[T].filter(fieldIsRegistered).map(x => PardisStructArg(x.name, x.mutable, field(elem1, x.name)(x.tpe)))
    val elems2 = getFields[S].filter(fieldIsRegistered).map(x => PardisStructArg(x.name, x.mutable, field(elem2, x.name)(x.tpe)))
    val structFields = elems1 ++ elems2
    struct(structFields: _*)(resultType)
  }

  override def transformDef[T: TypeRep](node: Def[T]): to.Def[T] = node match {
    case an @ ArrayNew(size) =>
      ArrayNew(size)(apply(an.tp.typeArguments(0)))
    case CaseClassNew(ccn) if lowerStructs =>
      transformDef(super.transformDef(node))
    case sd @ StructDefault() if lowerStructs =>
      transformDef(super.transformDef(node))
    case ps @ PardisStruct(tag, elems, methods) =>
      // logger.debug(s"transforming PardisStruct for tag: $tag")
      val registeredFields = fieldsAccessed.get(tag)
      val newFields = registeredFields match {
        case Some(x) if removeUnusedFields && !compliant => elems.filter(e => x.contains(e.name))
        case _ => elems
      }
      val newTpe = ps.tp.asInstanceOf[TypeRep[Any]]
      super.transformDef(PardisStruct(tag, newFields, Nil)(ps.tp))(ps.tp)
    case ConcatDynamic(record1, record2, leftAlias, rightAlias) if lowerStructs => {
      val tp = node.tp.asInstanceOf[TypeRep[(Any, Any)]]
      val leftTag = getTag(record1.tp)
      val rightTag = getTag(record2.tp)
      val Constant(la: String) = leftAlias
      val Constant(ra: String) = rightAlias
      val concatTag = StructTags.CompositeTag[Any, Any](la, ra, leftTag, rightTag)
      // logger.debug(s"transforming ConcatDynamic for tag: $concatTag")
      def getElems[T](exp: Rep[T]): Seq[StructElemInformation] = getStructElems(getTag(exp.tp))
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
    case ag: AggOpNew[_, _] if !Config.specializeEngine => {
      class A
      val ma = ag.typeA
      implicit val mb = ag.typeB.asInstanceOf[TypeRep[A]]
      val maa = ma.asInstanceOf[TypeRep[Any]]
      val marrDouble = implicitly[to.TypeRep[to.Array[to.Double]]]

      // implicit val magg = {
      //   val t = typeRep[AGGRecord[Any]].rebuild(mb).asInstanceOf[TypeRep[AGGRecord[A]]]
      //   val tag = getClassTag(t)
      //   System.out.println(s"tag -> $tag -> ${t.name.replace('[', '_').replace("]", "").replace(" ", "").replace(',', '_')}")
      //   new RecordType(tag, Some(t))
      // }
      // System.out.println(s"magg -> $magg")
      implicit val magg = apply(typeRep[AGGRecord[Any]].rebuild(mb)).asInstanceOf[TypeRep[AGGRecord[A]]]
      def aggNew(key: Rep[A], values: Rep[Array[Double]]) =
        __new(("key", false, key), ("aggs", false, values))(magg)
      val hm = to.__newHashMap[Any, Any]()(apply(mb), apply(magg.asInstanceOf[TypeRep[Any]]))
      val aggNums = ag.aggFuncsOutput match {
        case Def(LiftedSeq(seq)) => unit(seq.length)
        case _                   => throw new Exception("Couldn't compute the number of agg functions")
      }
      to.__newDef[AggOp[Any, Any]](
        ("tag", false, unit(OperatorTags.AggOp)),
        ("parent", false, apply(ag.parent)(ag.parent.tp)),
        ("numAggs", false, aggNums),
        ("hm", false, hm),
        ("hm_keys", true, hm.keySet),
        ("hm_iter_counter", true, unit(0)),
        ("grp", false, ag.grp),
        ("agger", false, __lambda((x: Rep[A]) => aggNew(x, __newArray[Double](ag.numAggs)))),
        ("aggFuncsOutput", false, ag.aggFuncsOutput)).asInstanceOf[to.Def[T]]
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
    case po: PrintOpNew[_] if !Config.specializeEngine => {
      val ma = apply(po.typeA)
      val maa = ma.asInstanceOf[TypeRep[Any]]
      to.__newDef[PrintOp[Any]](
        ("tag", false, unit(OperatorTags.PrintOp)),
        ("parent", false, apply(po.parent)),
        ("printFunc", false, apply(po.printFunc)),
        ("limit", false, po.limit)).asInstanceOf[to.Def[T]]
    }
    case po: PrintOpNew[_] => {
      val ma = po.typeA
      val maa = ma.asInstanceOf[TypeRep[Any]]
      to.__newDef[PrintOp[Any]](("numRows", true, to.unit[Int](0)),
        stop).asInstanceOf[to.Def[T]]
    }
    case so: ScanOpNew[_] if !Config.specializeEngine => {
      val ma = so.typeA
      val maa = ma.asInstanceOf[TypeRep[Any]]
      to.__newDef[ScanOp[Any]](
        ("tag", false, unit(OperatorTags.ScanOp)),
        ("table_size", false, so.table.asInstanceOf[Rep[Array[Any]]].length),
        ("i", true, to.unit[Int](0)),
        ("record_size", false, to.sizeof()(maa)),
        ("table", false, so.table)).asInstanceOf[to.Def[T]]
    }
    case so: ScanOpNew[_] => {
      val ma = so.typeA
      val maa = ma.asInstanceOf[TypeRep[Any]]
      to.__newDef[ScanOp[Any]](("i", true, to.unit[Int](0)),
        stop).asInstanceOf[to.Def[T]]
    }
    case mo: MapOpNew[_] if !Config.specializeEngine => {
      val ma = mo.typeA
      val maa = ma.asInstanceOf[TypeRep[Any]]
      val mapNums = mo.mapFuncsOutput match {
        case Def(LiftedSeq(seq)) => unit(seq.length)
        case _                   => throw new Exception("Couldn't compute the number of map functions")
      }
      to.__newDef[MapOp[Any]](
        ("tag", false, unit(OperatorTags.MapOp)),
        ("parent", false, apply(mo.parent)),
        ("mapNums", false, mapNums),
        ("mapFuncs", false, apply(mo.mapFuncsOutput))).asInstanceOf[to.Def[T]]
      // throw new Exception("MapOp not supported yet for a non-specialized engine!")
    }
    case mo: MapOpNew[_] => {
      val ma = mo.typeA
      val maa = ma.asInstanceOf[TypeRep[Any]]
      to.__newDef[MapOp[Any]](
        stop).asInstanceOf[to.Def[T]]
    }
    case so: SelectOpNew[_] if !Config.specializeEngine => {
      val ma = so.typeA
      val maa = ma.asInstanceOf[TypeRep[Any]]
      to.__newDef[SelectOp[Any]](
        ("tag", false, unit(OperatorTags.SelectOp)),
        ("parent", false, apply(so.parent)),
        ("selectPred", false, so.selectPred)).asInstanceOf[to.Def[T]]
    }
    case so: SelectOpNew[_] => {
      val ma = so.typeA
      val maa = ma.asInstanceOf[TypeRep[Any]]
      to.__newDef[SelectOp[Any]](
        stop).asInstanceOf[to.Def[T]]
    }
    case so: SortOpNew[_] if !Config.specializeEngine => {
      val ma = so.typeA
      val maa = ma.asInstanceOf[TypeRep[Any]]
      to.__newDef[SortOp[Any]](
        ("tag", false, unit(OperatorTags.SortOp)),
        ("parent", false, apply(so.parent)),
        ("sortedTree", false, to.__newTreeSet2(to.Ordering[Any](apply(so.orderingFunc.asInstanceOf[Rep[(Any, Any) => Int]]))(apply(maa)))(apply(maa)))).asInstanceOf[to.Def[T]]
    }
    case so: SortOpNew[_] => {
      val ma = so.typeA
      val maa = ma.asInstanceOf[TypeRep[Any]]
      to.__newDef[SortOp[Any]](
        ("sortedTree", false, to.__newTreeSet2(to.Ordering[Any](apply(so.orderingFunc.asInstanceOf[Rep[(Any, Any) => Int]]))(apply(maa)))(apply(maa))),
        stop).asInstanceOf[to.Def[T]]
    }
    case ho: HashJoinOpNew1[_, _, _] if !Config.specializeEngine => {
      val ma = ho.typeA
      val mb = ho.typeB
      val mc = ho.typeC
      trait A extends sc.pardis.shallow.Record
      trait B extends sc.pardis.shallow.Record
      trait Res
      val mba = mb.asInstanceOf[TypeRep[Any]]
      type HashJoinOpTp = HashJoinOp[sc.pardis.shallow.Record, sc.pardis.shallow.Record, Any]
      val tp = ho.tp.asInstanceOf[TypeRep[HashJoinOpTp]]
      val marrBuffA = implicitly[TypeRep[ArrayBuffer[Any]]].rebuild(ma).asInstanceOf[TypeRep[Any]]
      val mCompRec = implicitly[TypeRep[DynamicCompositeRecord[sc.pardis.shallow.Record, sc.pardis.shallow.Record]]].rebuild(ma, mb).asInstanceOf[TypeRep[Any]]
      implicit val tpA = apply(ma).asInstanceOf[TypeRep[A]]
      implicit val tpB = apply(mb).asInstanceOf[TypeRep[B]]
      implicit val tpRes = apply(mCompRec).asInstanceOf[TypeRep[Res]]
      val hm = to.__newMultiMap[Any, Any]()(apply(mc), apply(ma.asInstanceOf[TypeRep[Any]]))
      System.out.println(s"tp for hm: ${hm.tp}")
      to.__newDef[HashJoinOpTp](
        ("tag", false, unit(OperatorTags.HashJoinOp)),
        ("leftParent", false, apply(ho.leftParent)),
        ("rightParent", false, apply(ho.rightParent)),
        ("hm", false, hm),
        ("joinCond", false, apply(ho.joinCond)),
        ("leftHash", false, apply(ho.leftHash)),
        ("rightHash", false, apply(ho.rightHash)),
        ("concatenator", false, __lambda((x: Rep[A], y: Rep[B]) => {
          concat_records[A, B, Res](x, y)
        })))(tp).asInstanceOf[to.Def[T]]

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
    case wo: WindowOpNew[_, _, _] if !Config.specializeEngine => {
      trait A
      trait B
      trait C
      trait Wnd
      implicit val ma = wo.typeA.asInstanceOf[TypeRep[A]]
      implicit val mb = apply(wo.typeB).asInstanceOf[TypeRep[B]]
      implicit val mc = apply(wo.typeC).asInstanceOf[TypeRep[C]]
      val mwinRecBC = implicitly[TypeRep[WindowRecord[Any, Any]]].rebuild(wo.typeB, wo.typeC).asInstanceOf[TypeRep[Any]]
      implicit val tpWnd = apply(mwinRecBC).asInstanceOf[TypeRep[Wnd]]
      def wndNew(key: Rep[B], wnd: Rep[C]): Rep[Wnd] =
        __new(("key", false, key), ("wnd", false, wnd))(apply(mwinRecBC)).asInstanceOf[Rep[Wnd]]
      to.__newDef[WindowOp[Any, Any, Any]](
        ("tag", false, unit(OperatorTags.WindowOp)),
        ("parent", false, apply(wo.parent)),
        ("grp", false, apply(wo.grp)),
        ("wndFunction", false, apply(wo.wndf)),
        ("wndFactory", false, __lambda((x: Rep[B], y: Rep[C]) => wndNew(x, y))),
        ("hm", false, to.__newMultiMap[Any, Any]()(apply(mb), apply(ma)))).asInstanceOf[to.Def[T]]
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
    case lho: LeftHashSemiJoinOpNew[_, _, _] if !Config.specializeEngine => {
      val ma = lho.typeA
      val mb = lho.typeB
      val mc = lho.typeC
      val maa = ma.asInstanceOf[TypeRep[Any]]
      val marrBuffB = implicitly[TypeRep[ArrayBuffer[Any]]].rebuild(mb).asInstanceOf[TypeRep[Any]]
      to.__newDef[LeftHashSemiJoinOp[Any, Any, Any]](
        ("tag", false, unit(OperatorTags.LeftHashSemiJoinOp)),
        ("leftParent", false, apply(lho.leftParent)),
        ("rightParent", false, apply(lho.rightParent)),
        ("hm", false, to.__newMultiMap[Any, Any]()(apply(mc), apply(mb))),
        ("joinCond", false, apply(lho.joinCond)),
        ("leftHash", false, apply(lho.leftHash)),
        ("rightHash", false, apply(lho.rightHash))).asInstanceOf[to.Def[T]]
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
        ("table", false, to.arrayNew(to.unit(48000000))(apply(ma)))).asInstanceOf[to.Def[T]] // TODO-GEN: use statistics here as well
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
    case mo: MergeJoinOpNew[_, _] if !Config.specializeEngine => {
      class A
      class B
      trait Res
      implicit val ma = apply(mo.typeA).asInstanceOf[TypeRep[A]]
      implicit val mb = apply(mo.typeB).asInstanceOf[TypeRep[B]]
      implicit val mRes = apply(implicitly[TypeRep[DynamicCompositeRecord[sc.pardis.shallow.Record, sc.pardis.shallow.Record]]].rebuild(mo.typeA, mo.typeB)).asInstanceOf[TypeRep[Res]]
      type MergeJoinOpTp = MergeJoinOp[sc.pardis.shallow.Record, sc.pardis.shallow.Record]
      val tp = mo.tp.asInstanceOf[TypeRep[MergeJoinOpTp]]
      import to._
      to.__newDef[MergeJoinOpTp](
        ("tag", false, unit[Int](OperatorTags.MergeJoinOp)),
        ("leftParent", false, apply(mo.leftParent)),
        ("rightParent", false, apply(mo.rightParent)),
        ("joinCond", false, apply(mo.joinCond)),
        ("leftRelation", false, to.__newArray[A](unit(1 << 25))),
        ("leftIndex", true, to.unit[Int](0)),
        ("leftSize", true, to.unit[Int](0)),
        ("concatenator", false, __lambda((x: Rep[A], y: Rep[B]) => {
          concat_records[A, B, Res](x, y)
        })))(tp).asInstanceOf[to.Def[T]]
    }
    case mo: MergeJoinOpNew[_, _] => {
      class A
      class B
      implicit val ma = apply(mo.typeA).asInstanceOf[TypeRep[A]]
      implicit val mb = apply(mo.typeB).asInstanceOf[TypeRep[B]]
      type MergeJoinOpTp = MergeJoinOp[sc.pardis.shallow.Record, sc.pardis.shallow.Record]
      val tp = mo.tp.asInstanceOf[TypeRep[MergeJoinOpTp]]
      import to._
      to.__newDef[MergeJoinOpTp](
        ("leftRelation", false, to.__newArray[A](unit(1 << 25))),
        ("leftIndex", true, to.unit[Int](0)),
        ("leftSize", true, to.unit[Int](0)),
        stop)(tp).asInstanceOf[to.Def[T]]

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
        // case _ if !Config.specializeEngine && isOperatorType(exp.tp) => Some(exp)
        case _                                       => None
      }
  }

  def isOperatorType[T](tp: PardisType[T]): Boolean =
    OperatorsType.unapply(tp).nonEmpty

  object OperatorsType {
    def unapply[T](tp: PardisType[T]): Option[PardisType[T]] = tp match {
      case LeftHashSemiJoinOpType(_, _, _) | HashJoinOpType(_, _, _) | WindowOpType(_, _, _) |
        AggOpType(_, _) | PrintOpType(_) | ScanOpType(_) | MapOpType(_) | SelectOpType(_) |
        SortOpType(_) | NestedLoopsJoinOpType(_, _) | SubquerySingleResultType(_) | ViewOpType(_) |
        HashJoinAntiType(_, _, _) | LeftOuterJoinOpType(_, _, _) | MergeJoinOpType(_, _) => Some(tp)
      case _ => None
    }
  }

  object LoweredNew extends RepExtractor {
    def unapply[T](exp: Rep[T]): Option[Rep[T]] = exp.tp match {
      case x if x.isRecord  => Some(exp)
      case OperatorsType(_) => Some(exp)
      case _                => None
    }
  }
}

trait RecordUsageAnalysis[Lang <: Base] extends Traverser[Lang] { this: Lowering[Lang, Lang] =>
  import IR._

  val removeUnusedFields: Boolean
  val compliant: Boolean

  sealed trait Phase
  case object FieldExtractionPhase extends Phase
  case object FieldUsagePhase extends Phase
  case object OtherPhase extends Phase

  var phase: Phase = _

  val fieldsAccessed = collection.mutable.Map[StructTags.StructTag[_], collection.mutable.ArrayBuffer[String]]()
  val notSeenDynamicRecordTypes = collection.mutable.Set[TypeRep[Any]]()

  def getRegisteredFieldsOfType[A](t: PardisType[A]): List[String] = {
    val registeredFields = t match {
      case DynamicCompositeRecordType(l, r) =>
        getTag(t) match {
          case tag @ StructTags.CompositeTag(la, ra, ltag, rtag) =>
            getRegisteredFieldsOfType(l).map(la + _) ++ getRegisteredFieldsOfType(r).map(ra + _)
        }
      case _ =>
        manifestTags.get(t.asInstanceOf[TypeRep[Any]]).flatMap(x => fieldsAccessed.get(x)) match {
          case Some(x) => x
          case None    => List()
        }
    }
    registeredFields.toList
  }

  def registerField[A](t: PardisType[A], field: String): Unit = {
    t match {
      case DynamicCompositeRecordType(l, r) =>
        manifestTags.get(t.asInstanceOf[TypeRep[Any]]) match {
          case Some(tag @ StructTags.CompositeTag(la, ra, ltag, rtag)) =>
            // val lstruct = structs(ltag)
            // val rstruct = structs(rtag)
            if (field.startsWith(la)) {
              registerField(l, field.substring(la.size))
            }
            if (field.startsWith(ra)) {
              registerField(r, field.substring(ra.size))
            }
          case _ =>
            notSeenDynamicRecordTypes += t.asInstanceOf[TypeRep[Any]]
            registerField(l, field)
            registerField(r, field)
        }
      case _ =>
        manifestTags.get(t.asInstanceOf[TypeRep[Any]]) match {
          case Some(tag) => structs.get(tag) match {
            case Some(s) =>
              val l = fieldsAccessed.getOrElseUpdate(tag, new collection.mutable.ArrayBuffer())
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
      val leftTag = getTag(self.tp)
      val rightTag = getTag(record2.tp)
      // TODO rewrite using getElems method
      val concatTag = StructTags.CompositeTag[Any, Any](la, ra, leftTag, rightTag)
      val regFields = getRegisteredFieldsOfType(self.tp) ++ getRegisteredFieldsOfType(record2.tp)
      def fieldIsRegistered(f: StructElemInformation): Boolean = regFields.contains(f.name) || !removeUnusedFields
      val newElems = getStructElems(leftTag).filter(fieldIsRegistered).map(x => StructElemInformation(la + x.name, x.tpe, x.mutable)) ++ getStructElems(rightTag).filter(fieldIsRegistered).map(x => StructElemInformation(ra + x.name, x.tpe, x.mutable))
      structs += concatTag -> newElems
      manifestTags += node.tp.asInstanceOf[TypeRep[Any]] -> concatTag
    }
    case _ => super.traverseDef(node)
  }
}

class RecordLoweringPostProcess(override val IR: QueryEngineExp, val recordLowering: RecordLowering) extends RuleBasedTransformer[QueryEngineExp](IR) {
  import IR._

  rewrite += statement {
    case sym -> StructImmutableField(obj, name) if sym.tp.isRecord && !sym.tp.isInstanceOf[RecordType[_]] =>
      import recordLowering.{ createTag }
      val tag = createTag(sym.tp)
      val newTp = new RecordType(tag, Some(sym.tp.asInstanceOf[TypeRep[Any]])).asInstanceOf[TypeRep[Any]]
      field(obj, name)(newTp)
  }
}
