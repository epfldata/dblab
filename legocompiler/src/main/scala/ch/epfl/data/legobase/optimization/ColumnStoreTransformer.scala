package ch.epfl.data
package legobase
package optimization

import scala.language.implicitConversions
import pardis.ir._
import reflect.runtime.universe.{ TypeTag, Type }
import pardis.optimization._
import deep._
import pardis.types._
import pardis.types.PardisTypeImplicits._
import pardis.shallow.utils.DefaultValue

class ColumnStoreTransformer(override val IR: LoweringLegoBase, val queryNumber: Int) extends RuleBasedTransformer[LoweringLegoBase](IR) with StructCollector[LoweringLegoBase] {
  import IR._

  val typeList = queryNumber match {
    case 1  => List("LINEITEMRecord")
    case 2  => List("PARTSUPPRecord")
    case 3  => List("ORDERSRecord")
    case 4  => List("ORDERSRecord")
    case 5  => List("NATIONRecord")
    case 6  => List("LINEITEMRecord")
    case 7  => List("NATIONRecord")
    case 8  => List("LINEITEMRecord")
    case 9  => List("SUPPLIERRecord")
    case 10 => List("CUSTOMERRecord")
    case 11 => List("SUPPLIERRecord")
    case 12 => List("LINEITEMRecord")
    case 14 => List("LINEITEMRecord")
    case 15 => List("LINEITEMRecord")
    case 19 => List("LINEITEMRecord")
    case _  => throw new Exception(s"Column store not supported yet for $queryNumber")
  }

  def shouldBeColumnarized[T](tp: PardisType[T]): Boolean = tp.name match {
    case name if typeList.contains(name) => true
    case _                               => false
  }

  def tableColumnStoreTag(oldTag: StructTags.StructTag[_]) =
    StructTags.ClassTag[Any]("ColumnStoreOf" + oldTag.typeName)

  def tableColumnStoreType(newTag: StructTags.StructTag[_]) =
    new RecordType(newTag, None).asInstanceOf[TypeRep[Any]]

  def elemColumnStoreType(oldTag: StructTags.StructTag[_]) = {
    val newTag = StructTags.ClassTag(s"RowOf${oldTag.typeName}")
    new RecordType(newTag, None).asInstanceOf[TypeRep[Any]]
  }

  val COLUMN_STORE_POINTER = "columnStorePointer"
  val INDEX = "index"
  implicit class FieldNameOps(fieldName: String) {
    def arrayOf: String = s"arrayOf$fieldName"
  }

  override def transformType[T: PardisType]: PardisType[Any] = ({
    val tp = typeRep[T]
    if (tp.isArray) {
      if (shouldBeColumnarized(tp.typeArguments(0))) {
        val structDef = getStructDef(tp.typeArguments(0)).get
        tableColumnStoreType(tableColumnStoreTag(structDef.tag))
      } else super.transformType[T]
    } else super.transformType[T]
  })

  rewrite += remove {
    case ps @ PardisStruct(_, _, _) if shouldBeColumnarized(ps.tp) => ()
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
      val structDef = getStructDef(elemType).get
      for (el <- structDef.fields) {
        class ColumnType
        implicit val columnType = el.tpe.asInstanceOf[TypeRep[ColumnType]]
        val column = field[Array[ColumnType]](apply(arr), el.name.arrayOf)
        val Def(s) = value
        val v = s.asInstanceOf[PardisStruct[Any]].elems.find(e => e.name == el.name) match {
          case Some(e) => e.init.asInstanceOf[Rep[ColumnType]]
          case None    => throw new Exception(s"could not find any element for $s with name `${el.name}`")
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
      val tpe = sym.tp.asInstanceOf[PardisType[Any]].asInstanceOf[RecordType[Any]]
      val newElems = List(PardisStructArg(COLUMN_STORE_POINTER, false, apply(arr)), PardisStructArg(INDEX, false, idx))
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
      val idx = field[Int](s, INDEX)
      column(idx) = value
  }
}
