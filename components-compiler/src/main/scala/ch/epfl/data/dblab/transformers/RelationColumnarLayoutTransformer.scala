package ch.epfl.data
package dblab
package transformers

import ch.epfl.data.sc.pardis.quasi.TypeParameters._
import utils.Logger
import scala.language.implicitConversions
import sc.pardis.ir._
import deep.dsls.QueryEngineExp
import reflect.runtime.universe.{ TypeTag, Type }
import sc.pardis.optimization._
import deep._
import sc.pardis.types._
import sc.pardis.types.PardisTypeImplicits._
import sc.pardis.shallow.utils.DefaultValue
import quasi._

/**
 * Transforms row layout representation of the source relations to columnar layout representation.
 *
 * Example:
 * {{{
 *    // RecordA { fieldA: Int, fieldB: String }
 *    val array = new Array[RecordA](size)
 *    for(i <- 0 until size) {
 *      val elem = array(i)
 *      process(elem.fieldA, elem.fieldB)
 *    }
 * }}}
 * is converted to:
 * {{{
 *    // ColumnStoreOfRecordA {
 *    //   arrayOfFieldA: Int,
 *    //   arrayOfFieldB: String
 *    // }
 *    val csArray = ColumnStoreOfRecordA {
 *      val arrayOfFieldA = new Array[Int](size)
 *      val arrayOfFieldB = new Array[String](size)
 *    }
 *    // RowOfRecordA {
 *    //   columnStorePointer: ColumnStoreOfRecordA,
 *    //   index: Int
 *    // }
 *    for(i <- 0 until size) {
 *      val csElem = RowOfRecordA {
 *        val columnStorePointer = csArray
 *        val index = i
 *      }
 *      val elemFieldA = csElem.columnStorePointer.arrayOfFieldA(csElem.index)
 *      val elemFieldB = csElem.columnStorePointer.arrayOfFieldB(csElem.index)
 *      process(elemFieldA, elemFieldB)
 *    }
 * }}}
 *
 * If this optimization is combined with ParameterPromotion, the abstraction of
 * `ColumnStoreOfRecordA` and `RowOfRecordA` will be removed.
 * The previous example is converted to the following program:
 * {{{
 *    val arrayOfFieldA = new Array[Int](size)
 *    val arrayOfFieldB = new Array[String](size)
 *    for(i <- 0 until size) {
 *      val elemFieldA = arrayOfFieldA(i)
 *      val elemFieldB = arrayOfFieldB(i)
 *      process(elemFieldA, elemFieldB)
 *    }
 * }}}
 *
 * Precondition:
 * The elements of the array of records that we would like to convert, should not
 * be set to a null value.
 * Also, the elements of such array should be set to the value of a mutable variable.
 *
 *
 * @param IR the polymorphic embedding trait which contains the reified program.
 */
class RelationColumnarLayoutTransformer(override val IR: QueryEngineExp)
  extends RuleBasedTransformer[QueryEngineExp](IR)
  with StructCollector[QueryEngineExp] {
  import IR._

  val logger = Logger[RelationColumnarLayoutTransformer]

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

  // override def transformType[T: PardisType]: PardisType[Any] = {
  //   val tp = typeRep[T]
  //   if (tp.isArray) {
  //     if (shouldBeColumnarized(tp.typeArguments(0))) {
  //       val structDef = getStructDef(tp.typeArguments(0)).get
  //       tableColumnStoreType(tableColumnStoreTag(structDef.tag))
  //     } else super.transformType[T]
  //   } else if (shouldBeColumnarized(tp)) {
  //     val structDef = getStructDef(tp).get
  //     elemColumnStoreType(structDef.tag)
  //   } else super.transformType[T]
  // }

  case class ColumnarArray(originalSymbol: Rep[_], fields: List[(String, Rep[Array[_]])]) {
    def getField(field: String)(idx: Rep[Int]): Rep[Any] = {
      val arr = fields.find(_._1 == field).get._2.asInstanceOf[Rep[Array[Any]]]
      implicit val tp: TypeRep[Any] = arr.tp.typeArguments(0).asInstanceOf[TypeRep[Any]]
      val res = arrayApply(arr, idx)(tp)
      logger.debug(s"get field of $res with tp ${res.tp}")
      res
    }
    def setField(field: String)(idx: Rep[Int])(value: Rep[Any]): Rep[Unit] = {
      val arr = fields.find(_._1 == field).get._2.asInstanceOf[Rep[Array[Any]]]
      implicit val tp: TypeRep[Any] = arr.tp.typeArguments(0).asInstanceOf[TypeRep[Any]]
      arr.update(idx, value)
    }
  }

  val potentialTypes = scala.collection.mutable.Set[TypeRep[_]]()
  val relationArrays = scala.collection.mutable.Set[Rep[_]]()
  val relationRecords = scala.collection.mutable.Set[Rep[_]]()
  val relationColumnarArrays = scala.collection.mutable.Set[ColumnarArray]()
  val forbiddenTypes = scala.collection.mutable.Set[TypeRep[_]]()
  val columnarTypes = scala.collection.mutable.Set[TypeRep[_]]()

  analysis += statement {
    case sym -> dsl"new Array[Any](${ Def(LoaderFileLineCountObject(_)) })" =>
      val innerType = sym.tp.typeArguments(0)
      relationArrays += sym
      // if (innerType.isRecord)
      //   potentialTypes += innerType
      // if (innerType.isArray && innerType.typeArguments(0).isRecord)
      //   forbiddenTypes += innerType.typeArguments(0)
      ()
  }

  /**
   * Restricts the types that should be converted to columnar layout
   * based on the pattern of the write accesses on their arrays.
   * Maybe these contraints should be relaxed.
   */
  object UnacceptableUpdateValue {
    def unapply[T](exp: Rep[T]): Option[Rep[T]] = exp match {
      case Constant(null)  => Some(exp)
      case Def(ReadVar(v)) => Some(exp)
      case _               => None
    }
  }

  {
    analysis += rule {
      case dsl"($arr: Array[Any])($index) = $rhs" if relationArrays.contains(arr) =>
        relationRecords += rhs
        ()
    }
  }

  def computeColumnarTypes(): Unit = {
    for (
      tpe <- (potentialTypes diff forbiddenTypes) if getTable(tpe.name).nonEmpty
    ) {
      columnarTypes += tpe
    }
  }

  override def postAnalyseProgram[T: TypeRep](node: Block[T]): Unit = {
    computeColumnarTypes()
    logger.debug(s">>>CStore columnarTypes: ${columnarTypes}<<<")
    logger.debug(s"Relation arrays: ${relationArrays}")
    logger.debug(s"Relation records: ${relationRecords}")
  }

  rewrite += statement {
    case sym -> dsl"new Array[Any]($size)" if relationArrays.contains(sym) => {
      val elemType = sym.tp.typeArguments(0).asInstanceOf[PardisType[Any]]
      val structDef = getStructDef(elemType).get
      val fields = structDef.fields.map(el =>
        el.name -> arrayNew(size)(el.tpe)).toList
      relationColumnarArrays += ColumnarArray(sym, fields)
      logger.debug(s"fields: $fields")
      unit()
    }
  }

  rewrite += statement {
    case sym -> (aa @ dsl"($arr: Array[Any])($idx)") if relationArrays.contains(arr) =>
      val elemType = sym.tp.asInstanceOf[PardisType[Any]]
      val columnarArray = relationColumnarArrays.find(_.originalSymbol == arr).get
      // val newElems = List(PardisStructArg(COLUMN_STORE_POINTER, false, apply(arr)), PardisStructArg(INDEX, false, apply(idx)))
      // val rTpe = elemColumnStoreType(tpe.tag)
      logger.debug(s"apply elemType: $elemType --> $columnarArray")
      val newElems = columnarArray.fields.map(f => PardisStructArg(f._1, false, columnarArray.getField(f._1)(idx)))
      struct[Any](newElems: _*)(elemType)
    // unit()
  }

  rewrite += rule {
    case dsl"(($arr: Array[Any])($idx) = $value)" if relationArrays.contains(arr) =>
      val elemType = arr.tp.typeArguments(0).asInstanceOf[PardisType[Any]]
      val columnarArray = relationColumnarArrays.find(_.originalSymbol == arr).get
      // val newElems = List(PardisStructArg(COLUMN_STORE_POINTER, false, apply(arr)), PardisStructArg(INDEX, false, apply(idx)))
      // val rTpe = elemColumnStoreType(tpe.tag)
      for ((fieldName, fieldArray) <- columnarArray.fields) {
        columnarArray.setField(fieldName)(idx)(field(apply(value), fieldName))
      }
      logger.debug(s"update elemType: $elemType --> $columnarArray")
      // val newElems = columnarArray.fields.map(f => PardisStructArg(f._1, false, columnarArray.getField(f._1)(idx)))
      // struct[Any](newElems: _*)(elemType)
      unit()
  }

  // rewrite += remove {
  //   case (ps @ Struct(_, _, _)) if shouldBeColumnarized(ps.tp) =>
  //     ()
  // }

  // rewrite += rule {
  //   //    case an @ ArrayNew(size) if shouldBeColumnarized(an.tp.typeArguments(0)) => {
  //   //    case dsl"new Array[Any]($size) as $an" if shouldBeColumnarized(an.tp.typeArguments(0)) => {
  //   case an @ dsl"new Array[Any]($size)" if shouldBeColumnarized(an.tp.typeArguments(0)) => {
  //     val elemType = an.tp.typeArguments(0).asInstanceOf[PardisType[Any]]
  //     val structDef = getStructDef(elemType).get
  //     val newElems = structDef.fields.map(el =>
  //       PardisStructArg(el.name.arrayOf, true, arrayNew(size)(el.tpe)))
  //     val newTag = tableColumnStoreTag(structDef.tag)
  //     val newType = tableColumnStoreType(newTag)
  //     struct(newElems: _*)(newType)
  //   }
  // }

  // rewrite += rule {
  //   case au @ dsl"(($arr: Array[Any])($idx) = $value)" if shouldBeColumnarized(arr.tp.typeArguments(0)) =>
  //     val elemType = arr.tp.typeArguments(0).asInstanceOf[PardisType[Any]]
  //     val structDef = getStructDef(elemType).get
  //     for (el <- structDef.fields) {
  //       class ColumnType
  //       implicit val columnType = el.tpe.asInstanceOf[TypeRep[ColumnType]]
  //       val column = field[Array[ColumnType]](apply(arr), el.name.arrayOf)
  //       val struct = apply(value) match {
  //         case Def(s: Struct[Any]) => s
  //         case v =>
  //           val nodeType = v match {
  //             case Def(node) => s", node of type $node,"
  //             case _         => ""
  //           }
  //           throw new Exception(s"Cannot handle column-store for `$arr($idx) = $v: $nodeType` with the element type of $elemType")
  //       }
  //       val v = struct.elems.find(e => e.name == el.name) match {
  //         case Some(e) => e.init.asInstanceOf[Rep[ColumnType]]
  //         case None if struct.tag.typeName.startsWith(ROW_OF_PREFIX) =>
  //           val newTag = StructTags.ClassTag[Any](structDef.tag.typeName.substring(ROW_OF_PREFIX.length).columnStoreOf)
  //           class ColumnStorePointerType
  //           implicit val newType = tableColumnStoreType(newTag).asInstanceOf[TypeRep[ColumnStorePointerType]]
  //           val columnStorePointer = field[ColumnStorePointerType](apply(value), COLUMN_STORE_POINTER)
  //           val column = field[Array[ColumnType]](columnStorePointer, el.name.arrayOf)
  //           val idx = field[Int](apply(value), INDEX)
  //           column(idx)
  //       }
  //       column(idx) = v
  //     }
  //     dsl"()"
  // }

  // rewrite += statement {
  //    Nodes returning struct nodes -- convert them to a reference to column store 
  //   case sym -> (aa @ dsl"($arr: Array[Any])($idx)") if shouldBeColumnarized(sym.tp) =>
  //     val tpe = sym.tp.asInstanceOf[RecordType[Any]]
  //     val newElems = List(PardisStructArg(COLUMN_STORE_POINTER, false, apply(arr)), PardisStructArg(INDEX, false, apply(idx)))
  //     val rTpe = elemColumnStoreType(tpe.tag)
  //     struct[Any](newElems: _*)(rTpe)
  // }

  // rewrite += rule {
  //   case psif @ StructImmutableField(s, f) if shouldBeColumnarized(s.tp) =>
  //     val structDef = getStructDef(s.tp).get
  //     class ColumnType
  //     implicit val columnElemType = structDef.fields.find(el => el.name == f).get.tpe.asInstanceOf[TypeRep[ColumnType]]
  //     val newTag = tableColumnStoreTag(structDef.tag)
  //     class ColumnStorePointerType
  //     implicit val newType = tableColumnStoreType(newTag).asInstanceOf[TypeRep[ColumnStorePointerType]]
  //     val columnStorePointer = field[ColumnStorePointerType](apply(s), COLUMN_STORE_POINTER)
  //     val column = field[Array[ColumnType]](columnStorePointer, f.arrayOf)
  //     val idx = field[Int](apply(s), INDEX)
  //     column(idx)
  // }

  // rewrite += rule {
  //   case psif @ StructFieldGetter(s, f) if shouldBeColumnarized(s.tp) =>
  //     val structDef = getStructDef(s.tp).get
  //     class ColumnType
  //     implicit val columnElemType = structDef.fields.find(el => el.name == f).get.tpe.asInstanceOf[TypeRep[ColumnType]]
  //     val newTag = tableColumnStoreTag(structDef.tag)
  //     class ColumnStorePointerType
  //     implicit val newType = tableColumnStoreType(newTag).asInstanceOf[TypeRep[ColumnStorePointerType]]
  //     val columnStorePointer = field[ColumnStorePointerType](apply(s), COLUMN_STORE_POINTER)
  //     val column = field[Array[ColumnType]](columnStorePointer, f.arrayOf)
  //     val idx = field[Int](apply(s), INDEX)
  //     column(idx)
  // }

  // rewrite += rule {
  //   case psif @ StructFieldSetter(s, f, v) if shouldBeColumnarized(s.tp) =>
  //     val structDef = getStructDef(s.tp).get
  //     class ColumnType
  //     implicit val columnElemType = structDef.fields.find(el => el.name == f).get.tpe.asInstanceOf[TypeRep[ColumnType]]
  //     val value = v.asInstanceOf[Rep[ColumnType]]
  //     val newTag = tableColumnStoreTag(structDef.tag)
  //     class ColumnStorePointerType
  //     implicit val newType = tableColumnStoreType(newTag).asInstanceOf[TypeRep[ColumnStorePointerType]]

  //     val columnStorePointer = field[ColumnStorePointerType](apply(s), COLUMN_STORE_POINTER)
  //     val column = field[Array[ColumnType]](columnStorePointer, f.arrayOf)
  //     val idx = field[Int](apply(s), INDEX)
  //     column(idx) = value
  // }
}
