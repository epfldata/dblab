package ch.epfl.data
package dblab.legobase
package optimization

import scala.language.implicitConversions
import sc.pardis.ir._
import reflect.runtime.universe.{ TypeTag, Type }
import sc.pardis.optimization._
import deep._
import sc.pardis.types._
import sc.pardis.types.PardisTypeImplicits._
import sc.pardis.shallow.utils.DefaultValue

// TODO there should be no need for queryNumber thanks to Schema information

/**
 * Transforms row layout representation to columnar layout representation.
 *
 * @param IR the polymorphic embedding trait which contains the reified program.
 * @param queryNumber specifies the TPCH query number (TODO should be removed)
 * @param settings the compiler settings provided as command line arguments (TODO should be removed)
 */
class ColumnStoreTransformer(override val IR: LoweringLegoBase, val queryNumber: Int, val settings: compiler.Settings) extends RuleBasedTransformer[LoweringLegoBase](IR) with StructCollector[LoweringLegoBase] {
  import IR._

  // TODO should be removed
  /** Specifies the list of records that were partitioned into a 1D array in the hashmap partitioning phase */
  val partitioned1D = queryNumber match {
    case 1  => Nil
    case 2  => List("REGIONRecord", "PARTRecord", "NATIONRecord", "SUPPLIERRecord")
    case 3  => List("CUSTOMERRecord")
    case 4  => Nil
    case 5  => List("SUPPLIERRecord", "REGIONRecord")
    case 6  => Nil
    case 7  => List("CUSTOMERRecord", "ORDERSRecord")
    case 8  => List("NATIONRecord", "SUPPLIERRecord", "REGIONRecord", "CUSTOMERRecord", "ORDERSRecord", "PARTRecord")
    case 9  => List("ORDERSRecord", "PARTRecord", "NATIONRecord")
    case 10 => List("NATIONRecord")
    case 11 => List("NATIONRecord")
    case 12 => List("ORDERSRecord")
    case 13 => Nil
    case 14 => List("PARTRecord")
    case 15 => List("SUPPLIERRecord")
    case 16 => List("SUPPLIERRecord", "PARTRecord")
    case 17 => Nil
    case 18 => List("CUSTOMERRecord", "ORDERSRecord")
    case 19 => Nil
    case 20 => List("NATIONRecord", "SUPPLIERRecord", "PARTRecord")
    case 21 => List("NATIONRecord", "ORDERSRecord")
    case 22 => Nil
  }

  // TODO should be removed
  val typeList = (queryNumber match {
    case 1  => List("LINEITEMRecord")
    case 2  => List("PARTSUPPRecord")
    case 3  => List("ORDERSRecord")
    case 4  => List("ORDERSRecord")
    case 5  => List("NATIONRecord")
    case 6  => List("LINEITEMRecord")
    case 7  => List("NATIONRecord")
    case 8  => List("LINEITEMRecord")
    case 9  => List("PARTRecord") //List("SUPPLIERRecord")
    case 10 => List("CUSTOMERRecord", "NATIONRecord")
    case 11 => List("SUPPLIERRecord")
    case 12 => List("LINEITEMRecord")
    case 13 => List("CUSTOMERRecord")
    case 14 => List("LINEITEMRecord")
    case 15 => List("LINEITEMRecord")
    case 16 => List("PARTSUPPRecord")
    case 17 => List("PARTRecord")
    case 18 => List("LINEITEMRecord")
    case 19 => List("PARTRecord")
    case 20 => List("PARTSUPPRecord")
    case 21 => List("SUPPLIERRecord", "NATIONRecord")
    case 22 => List("CUSTOMERRecord")
    case _  => throw new Exception(s"Column store not supported yet for $queryNumber")
  }) //++ (if (settings.hashMapPartitioning) partitioned1D else Nil)
  // For some unknown reason, if we apply column store over these fields, the result will get incorrect.

  // def shouldBeColumnarized[T](tp: PardisType[T]): Boolean = tp.name match {
  //   case name if typeList.contains(name) => true
  //   case _                               => false
  // }

  def shouldBeColumnarized[T](tp: PardisType[T]): Boolean =
    columnarTypes.contains(tp)

  def tableColumnStoreTag(oldTag: StructTags.StructTag[_]) =
    StructTags.ClassTag[Any](oldTag.typeName.columnStoreOf)

  def tableColumnStoreType(newTag: StructTags.StructTag[_]) =
    new RecordType(newTag, None).asInstanceOf[TypeRep[Any]]

  def elemColumnStoreType(oldTag: StructTags.StructTag[_]) = {
    val newTag = StructTags.ClassTag(oldTag.typeName.rowOf)
    new RecordType(newTag, None).asInstanceOf[TypeRep[Any]]
  }
  val ROW_OF_PREFIX = "RowOf"
  val CSTORE_OF_PREFIX = "ColumnStoreOf"
  val COLUMN_STORE_POINTER = "columnStorePointer"
  val INDEX = "index"
  implicit class FieldNameOps(fieldName: String) {
    def arrayOf: String = s"arrayOf$fieldName"
  }

  implicit class ClassNameOps(className: String) {
    def rowOf = s"$ROW_OF_PREFIX$className"
    def columnStoreOf = s"$CSTORE_OF_PREFIX$className"
  }

  // case class StructArrayUpdate[T](arr: Rep[Array[T]], index: Rep[Int], struct: Option[Struct[T]])

  // val structArrayUpdates = scala.collection.mutable.Map[Rep[Any], StructArrayUpdate[Any]]()

  // def structHasArrayUpdate[T](sym: Rep[T]): Boolean = structArrayUpdates.contains(sym.asInstanceOf[Rep[Any]])

  override def transformType[T: PardisType]: PardisType[Any] = ({
    val tp = typeRep[T]
    if (tp.isArray) {
      if (shouldBeColumnarized(tp.typeArguments(0))) {
        val structDef = getStructDef(tp.typeArguments(0)).get
        tableColumnStoreType(tableColumnStoreTag(structDef.tag))
      } else super.transformType[T]
    } else if (shouldBeColumnarized(tp)) {
      val structDef = getStructDef(tp).get
      elemColumnStoreType(structDef.tag)
    } else super.transformType[T]
  })

  // analysis += rule {
  //   case au @ ArrayUpdate(arr, idx, value) if shouldBeColumnarized(arr.tp.typeArguments(0)) =>
  //     val structNode = value match {
  //       case Def(struct: Struct[Any]) => Some(struct)
  //       case _                        => None
  //     }
  //     structArrayUpdates += value -> StructArrayUpdate(arr, idx, structNode)
  //     ()
  // }

  // rewrite += statement {
  //   case sym -> (ps @ PardisStruct(_, _, _)) if shouldBeColumnarized(ps.tp) && structHasArrayUpdate(sym) =>
  //     val StructArrayUpdate(arr, idx, _) = structArrayUpdates(sym)
  //     val tpe = sym.tp.asInstanceOf[RecordType[Any]]
  //     val newElems = List(PardisStructArg(COLUMN_STORE_POINTER, false, apply(arr)), PardisStructArg(INDEX, false, idx))
  //     val rTpe = elemColumnStoreType(tpe.tag)
  //     struct[Any](newElems: _*)(rTpe)
  // }

  val potentialTypes = scala.collection.mutable.Set[TypeRep[_]]()
  val forbiddenTypes = scala.collection.mutable.Set[TypeRep[_]]()
  val columnarTypes = scala.collection.mutable.Set[TypeRep[_]]()

  analysis += statement {
    case sym -> ArrayNew(_) =>
      val innerType = sym.tp.typeArguments(0)
      if (innerType.isRecord)
        potentialTypes += innerType
      if (innerType.isArray && innerType.typeArguments(0).isRecord)
        forbiddenTypes += innerType.typeArguments(0)
      ()
  }

  def computeColumnarTypes(): Unit = {
    for (
      tpe <- (potentialTypes diff forbiddenTypes) if getTable(tpe.name).nonEmpty
    ) {
      columnarTypes += tpe
    }
  }

  override def optimize[T: TypeRep](node: Block[T]): Block[T] = {
    traverseBlock(node)
    System.out.println(s"CStore potentialTypes: ${potentialTypes}")
    System.out.println(s"CStore forbiddenTypes: ${forbiddenTypes}")
    computeColumnarTypes()
    System.out.println(s">>>CStore columnarTypes: ${columnarTypes}<<<")
    transformProgram(node)
  }

  rewrite += remove {
    case (ps @ PardisStruct(_, _, _)) if shouldBeColumnarized(ps.tp) =>
      ()
  }

  rewrite += rule {
    case an @ ArrayNew(size) if shouldBeColumnarized(an.tp.typeArguments(0)) => {
      val elemType = an.tp.typeArguments(0).asInstanceOf[PardisType[Any]]
      val structDef = getStructDef(elemType).get
      val newElems = structDef.fields.map(el => PardisStructArg(el.name.arrayOf, true, arrayNew(size)(el.tpe)))
      val newTag = tableColumnStoreTag(structDef.tag)
      val newType = tableColumnStoreType(newTag)
      // PardisStruct(newTag, newElems, Nil)(newType)
      struct(newElems: _*)(newType)
    }
  }

  rewrite += rule {
    case au @ ArrayUpdate(arr, idx, value) if shouldBeColumnarized(arr.tp.typeArguments(0)) =>
      val elemType = arr.tp.typeArguments(0).asInstanceOf[PardisType[Any]]
      // System.out.println(s"${scala.Console.RED}$value$structArrayUpdates${scala.Console.RESET}")
      val structDef = getStructDef(elemType).get
      for (el <- structDef.fields) {
        class ColumnType
        implicit val columnType = el.tpe.asInstanceOf[TypeRep[ColumnType]]
        val column = field[Array[ColumnType]](apply(arr), el.name.arrayOf)
        // val v = apply(value) match {

        //   // case Def(struct: Struct[_]) if struct.elems.exists(e => e.name == el.name) =>
        //   //   struct.elems.find(e => e.name == el.name).get.init.asInstanceOf[Rep[ColumnType]]
        //   case _ if structHasArrayUpdate(value) && structArrayUpdates(value).struct.nonEmpty =>
        //     val StructArrayUpdate(_, _, struct) = structArrayUpdates(value)
        //     struct.get.elems.find(e => e.name == el.name).get.init.asInstanceOf[Rep[ColumnType]]
        //   case sym @ Def(struct) if sym.tp.name.startsWith(ROW_OF_PREFIX) =>
        //     val newTag = StructTags.ClassTag[Any](sym.tp.name.substring(ROW_OF_PREFIX.length).columnStoreOf)
        //     class ColumnStorePointerType
        //     implicit val newType = tableColumnStoreType(newTag).asInstanceOf[TypeRep[ColumnStorePointerType]]
        //     val columnStorePointer = field[ColumnStorePointerType](apply(value), COLUMN_STORE_POINTER)
        //     val column = field[Array[ColumnType]](columnStorePointer, el.name.arrayOf)
        //     val index = if (structArrayUpdates.contains(value.asInstanceOf[Rep[Any]])) {
        //       idx
        //     } else {
        //       field[Int](apply(value), INDEX)
        //     }
        //     column(index)
        //   case s @ Def(node) =>
        //     throw new Exception(s"Cannot handle the node $node for column store")
        // }
        val Def(s) = apply(value)
        val struct = s.asInstanceOf[PardisStruct[Any]]
        val v = struct.elems.find(e => e.name == el.name) match {
          case Some(e) => e.init.asInstanceOf[Rep[ColumnType]]
          case None if struct.tag.typeName.startsWith(ROW_OF_PREFIX) =>
            val newTag = StructTags.ClassTag[Any](structDef.tag.typeName.substring(ROW_OF_PREFIX.length).columnStoreOf)
            class ColumnStorePointerType
            implicit val newType = tableColumnStoreType(newTag).asInstanceOf[TypeRep[ColumnStorePointerType]]
            val columnStorePointer = field[ColumnStorePointerType](apply(value), COLUMN_STORE_POINTER)
            val column = field[Array[ColumnType]](columnStorePointer, el.name.arrayOf)
            val idx = field[Int](apply(value), INDEX)
            column(idx)
          // case Some(s @ Def(node)) =>
          //   throw new Exception(s"Cannot handle the node $node for column store")
        }

        // val v = apply(value) match {
        //   case Def(s: Struct[_]) => s.elems.find(e => e.name == el.name) match {
        //     case Some(e) => e.init.asInstanceOf[Rep[ColumnType]]
        //     case None    => throw new Exception(s"could not find any element for $s with name `${el.name}`")
        //   }
        //   case va @ Def(ReadVar(_)) => {
        //     val res = apply(StructImmutableField[ColumnType](va, el.name))
        //     toAtom(res)
        //   }
        // }
        column(idx) = v
      }
      unit(())
  }

  rewrite += statement {
    /* Nodes returning struct nodes -- convert them to a reference to column store */
    case sym -> (aa @ ArrayApply(arr, idx)) if shouldBeColumnarized(sym.tp) =>
      val tpe = sym.tp.asInstanceOf[RecordType[Any]]
      val newElems = List(PardisStructArg(COLUMN_STORE_POINTER, false, apply(arr)), PardisStructArg(INDEX, false, apply(idx)))
      val rTpe = elemColumnStoreType(tpe.tag)
      struct[Any](newElems: _*)(rTpe)
  }

  rewrite += rule {
    case psif @ PardisStructImmutableField(s, f) if shouldBeColumnarized(s.tp) =>
      val structDef = getStructDef(s.tp).get
      class ColumnType
      implicit val columnElemType = structDef.fields.find(el => el.name == f).get.tpe.asInstanceOf[TypeRep[ColumnType]]
      val newTag = tableColumnStoreTag(structDef.tag)
      class ColumnStorePointerType
      implicit val newType = tableColumnStoreType(newTag).asInstanceOf[TypeRep[ColumnStorePointerType]]
      val columnStorePointer = field[ColumnStorePointerType](apply(s), COLUMN_STORE_POINTER)
      val column = field[Array[ColumnType]](columnStorePointer, f.arrayOf)
      val idx = field[Int](apply(s), INDEX)
      column(idx)
  }

  rewrite += rule {
    case psif @ PardisStructFieldGetter(s, f) if shouldBeColumnarized(s.tp) =>
      val structDef = getStructDef(s.tp).get
      class ColumnType
      implicit val columnElemType = structDef.fields.find(el => el.name == f).get.tpe.asInstanceOf[TypeRep[ColumnType]]
      val newTag = tableColumnStoreTag(structDef.tag)
      class ColumnStorePointerType
      implicit val newType = tableColumnStoreType(newTag).asInstanceOf[TypeRep[ColumnStorePointerType]]
      val columnStorePointer = field[ColumnStorePointerType](apply(s), COLUMN_STORE_POINTER)
      val column = field[Array[ColumnType]](columnStorePointer, f.arrayOf)
      val idx = field[Int](apply(s), INDEX)
      column(idx)
  }

  rewrite += rule {
    case psif @ PardisStructFieldSetter(s, f, v) if shouldBeColumnarized(s.tp) =>
      val structDef = getStructDef(s.tp).get
      class ColumnType
      implicit val columnElemType = structDef.fields.find(el => el.name == f).get.tpe.asInstanceOf[TypeRep[ColumnType]]
      val value = v.asInstanceOf[Rep[ColumnType]]
      val newTag = tableColumnStoreTag(structDef.tag)
      class ColumnStorePointerType
      implicit val newType = tableColumnStoreType(newTag).asInstanceOf[TypeRep[ColumnStorePointerType]]

      val columnStorePointer = field[ColumnStorePointerType](apply(s), COLUMN_STORE_POINTER)
      val column = field[Array[ColumnType]](columnStorePointer, f.arrayOf)
      val idx = field[Int](apply(s), INDEX)
      column(idx) = value
  }
}
