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
 *    val arrayFieldA = new Array[Int](size)
 *    val arrayFieldB = new Array[String](size)
 *    for(i <- 0 until size) {
 *      val elemFieldA = arrayFieldA(i)
 *      val elemFieldB = arrayFieldB(i)
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
 * @param settings the compiler settings provided as command line arguments (TODO should be removed)
 */
class ColumnStoreTransformer(override val IR: LoweringLegoBase, val settings: compiler.Settings) extends RuleBasedTransformer[LoweringLegoBase](IR) with StructCollector[LoweringLegoBase] {
  import IR._

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

  analysis += rule {
    case ArrayUpdate(arr, _, UnacceptableUpdateValue(_)) =>
      val innerType = arr.tp.typeArguments(0)
      if (innerType.isRecord)
        forbiddenTypes += innerType
      ()
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
    System.out.println(s">>>CStore columnarTypes: ${columnarTypes}<<<")
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
        val struct = apply(value) match {
          case Def(s: Struct[Any]) => s
          case v =>
            val nodeType = v match {
              case Def(node) => s", node of type $node,"
              case _         => ""
            }
            throw new Exception(s"Cannot handle column-store for `$arr($idx) = $v: $nodeType` with the element type of $elemType")
        }
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
