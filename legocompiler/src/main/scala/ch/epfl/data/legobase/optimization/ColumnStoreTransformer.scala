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

class ColumnStoreTransformer(override val IR: LoweringLegoBase) extends Optimizer[LoweringLegoBase](IR) with StructCollector[LoweringLegoBase] {
  import IR._
  import CNodes._
  import CTypes._

  def optimize[T: TypeRep](node: Block[T]): to.Block[T] = {
    traverseBlock(node)
    transformProgram(node)
  }

  override def traverseDef(node: Def[_]): Unit = node match {
    case _ => super.traverseDef(node)
  }

  def tableColumnStoreTag(oldTag: StructTags.StructTag[_]) =
    StructTags.ClassTag[Any]("ColumnStoreOf" + oldTag.typeName)

  def tableColumnStoreType(newTag: StructTags.StructTag[_]) =
    new RecordType(newTag).asInstanceOf[TypeRep[Any]]

  override def transformType[T: PardisType]: PardisType[Any] = ({
    val tp = typeRep[T]
    if (tp.isArray) {
      if (tp.typeArguments(0).isRecord) {
        val structDef = getStructDef(tp.typeArguments(0)).get
        tableColumnStoreType(tableColumnStoreTag(structDef.tag))
      } else super.transformType[T]
    } else super.transformType[T]
  })

  override def transformStmToMultiple(stm: Stm[_]): List[to.Stm[_]] = stm match {
    case Stm(s, PardisStruct(_, _, _)) => Nil
    case _                             => super.transformStmToMultiple(stm)
  }

  override def transformDef[T: PardisType](node: Def[T]): to.Def[T] = (node match {
    case an @ ArrayNew(size) => {
      // Get type of elements stored in array
      val elemType = an.tp.typeArguments(0)
      // Convert to column store only if this an array of record 
      if (elemType.isRecord) {
        val structDef = getStructDef(elemType).get
        val newElems = structDef.fields.map(el => { System.out.println(el.tpe); PardisStructArg("arrayOf" + el.name, true, arrayNew(size)(el.tpe)) })
        val newTag = tableColumnStoreTag(structDef.tag)
        val newType = tableColumnStoreType(newTag)
        PardisStruct(newTag, newElems, structDef.methods)(newType)
      } else super.transformDef(node)
    }
    case au @ ArrayUpdate(arr, idx, value) =>
      // Get type of elements stored in array
      val elemType = arr.tp.typeArguments(0)
      if (elemType.isRecord) {
        val structDef = getStructDef(elemType).get
        structDef.fields.map(el => {
          val column = field(arr, "arrayOf" + el.name)(typeArray(el.tpe))
          val Def(s) = value
          val v = s.asInstanceOf[PardisStruct[Any]].elems.find(e => e.name == el.name).get.init
          arrayUpdate(column, idx, v)
        })
        ReadVal(unit(()))
      } else super.transformDef(node)

    case psif @ PardisStructImmutableField(clm @ Def(ArrayApply(arr, idx)), f) => {
      if (clm.tp.isRecord) {
        val structDef = getStructDef(clm.tp).get
        val columnElemType = structDef.fields.find(el => el.name == f).get.tpe
        val column = field(arr, "arrayOf" + f)(typeArray(columnElemType))
        ArrayApply(column.asInstanceOf[Expression[Array[Any]]], idx)
      } else super.transformDef(node)
    }

    case psif @ PardisStructImmutableField(s, f) => {
      System.out.println("zzzz->" + s + "/" + f)
      super.transformDef(node)
    }

    case _ => super.transformDef(node)
  }).asInstanceOf[to.Def[T]]
}
