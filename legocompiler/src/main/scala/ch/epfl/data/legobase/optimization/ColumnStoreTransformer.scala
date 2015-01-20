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

class ColumnStoreTransformer(override val IR: LoweringLegoBase) extends RuleBasedTransformer[LoweringLegoBase](IR) with StructCollector[LoweringLegoBase] {
  import IR._

  def shouldBeColumnarized[T](tp: PardisType[T]): Boolean = tp.name match {
    case "LINEITEMRecord" => true
    case _                => false
  }

  def tableColumnStoreTag(oldTag: StructTags.StructTag[_]) =
    StructTags.ClassTag[Any]("ColumnStoreOf" + oldTag.typeName)

  def tableColumnStoreType(newTag: StructTags.StructTag[_]) =
    new RecordType(newTag, None).asInstanceOf[TypeRep[Any]]

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
      val newElems = structDef.fields.map(el => PardisStructArg("arrayOf" + el.name, true, arrayNew(size)(el.tpe)))
      val newTag = tableColumnStoreTag(structDef.tag)
      val newType = tableColumnStoreType(newTag)
      //val newMethods = List(PardisStructMethod("equal", eqMethod), PardisStructMethod("hash", hashMethod))
      // val newMethods = structDef.methods.map(m => m.copy(body =
      //   transformDef(m.body.asInstanceOf[Def[Any]]).asInstanceOf[PardisLambdaDef])) //)(s.tp)))
      val newMethods = Nil

      //structsDefMap(newTag) =
      //  PardisStructDef(newTag.asInstanceOf[StructTags.StructTag[Any]],
      //  newElems.map(x => StructElemInformation(x.name, x.init.tp, x.mutable)),
      //  newMethods)(node.tp.asInstanceOf[TypeRep[Any]])
      PardisStruct(newTag, newElems, newMethods)(newType)
    }
  }

  rewrite += rule {
    case au @ ArrayUpdate(arr, idx, value) if shouldBeColumnarized(arr.tp.typeArguments(0)) =>
      val elemType = arr.tp.typeArguments(0).asInstanceOf[PardisType[Any]]
      val structDef = getStructDef(elemType).get
      structDef.fields.map(el => {
        val column = field(apply(arr), "arrayOf" + el.name)(typeArray(el.tpe))
        val Def(s) = value
        val v = s.asInstanceOf[PardisStruct[Any]].elems.find(e => e.name == el.name) match {
          case Some(e) => e.init
          case None    => throw new Exception(s"could not find any element for $s with name `${el.name}`")
        }
        arrayUpdate(column, idx, v)
      })
      unit(())
  }

  rewrite += rule {
    /* Nodes returning struct nodes -- convert them to a reference to column store */
    case aa @ ArrayApply(arr, idx) if shouldBeColumnarized(aa.tp) =>
      val tpe = aa.tp.asInstanceOf[PardisType[Any]]

      // val hashMethod = doLambdaDef((x: Rep[Any]) => {
      //   unit(0)
      //   //infix_hashCode(field(x, "index")(IntType))(IntType)
      // })(tpe, IntType).asInstanceOf[PardisLambdaDef]

      // val eqMethod = doLambda2Def((x: Rep[Any], y: Rep[Any]) => {
      //   printf(unit("EQUALS\n"))
      //   val structDef = getStructDef(arr.tp.typeArguments(0)).get
      //   val newTag = tableColumnStoreTag(structDef.tag)
      //   val newType = tableColumnStoreType(newTag)

      //   val fx = field(x, "columnStorePointer")(newType)

      //   //val fy = field(y, "columnStorePointer")(newType) //(arr.tp)
      //   val ix = field(x, "index")(IntType)
      //   System.out.println(new RecordType(structDef.tag))
      //   System.out.println(structsDefMap.map(sd => sd._1))
      //   System.out.println(getStructDef(new RecordType(structDef.tag)).get.fields.map(f => f.name))

      //   //val iy = field(y, "index")(IntType)
      //   /*System.out.println(x.tp)
      //     System.out.println(structsDefMap.map(s => s._1).mkString("\n"))
      //     System.out.println(fx.tp)
      //     val eq = getStructEqualsFunc()(fx.tp, typeLambda3(fx.tp, fy.tp, typeInt, typeBoolean))
      //     //__app(eq).apply(fx, fy, 0)*/
      //   printf(unit("EQUALS\n"))

      //   infix_==(fx, fx)
      //   //inlineFunction(eq.asInstanceOf[Rep[(Any, Any, Int) => Boolean]], fx, fy, 0)
      //   //__eqMethod(fx, fy, ix, iy)(x.tp)
      //   //unit(true)
      // })(tpe, tpe, BooleanType).asInstanceOf[PardisLambdaDef]

      // val newMethods = List(PardisStructMethod("equal", eqMethod), PardisStructMethod("hash", hashMethod))
      val newMethods = Nil
      //val newMethods = List()
      val newElems = List(PardisStructArg("columnStorePointer", false, apply(arr)), PardisStructArg("index", false, idx))
      val newTag = StructTags.ClassTag(structName(aa.tp))
      // structsDefMap(newTag) = PardisStructDef(newTag.asInstanceOf[StructTags.StructTag[Any]],
      // newElems.map(x => StructElemInformation(x.name, x.init.tp, x.mutable)),
      // newMethods)(node.tp.asInstanceOf[TypeRep[Any]])
      PardisStruct(newTag, newElems, newMethods)(aa.tp)
  }

  rewrite += rule {
    case psif @ PardisStructImmutableField(s, f) if shouldBeColumnarized(s.tp) =>
      val structDef = getStructDef(s.tp).get
      //System.out.println(structDef.fields.mkString("\n"))
      //System.out.println(f)
      val columnElemType = structDef.fields.find(el => el.name == f).get.tpe
      val newTag = tableColumnStoreTag(structDef.tag)
      val newType = tableColumnStoreType(newTag)
      val columnStorePointer = field(s, "columnStorePointer")(newType)
      val column = field(columnStorePointer, "arrayOf" + f)(typeArray(columnElemType))
      val idx = field(s, "index")(typeInt)
      ArrayApply(column.asInstanceOf[Expression[Array[Any]]], idx)
  }

  rewrite += rule {
    case psif @ PardisStructFieldSetter(s, f, v) if shouldBeColumnarized(s.tp) =>
      val structDef = getStructDef(s.tp).get
      //System.out.println(structDef.fields)
      //System.out.println(f)
      val columnElemType = structDef.fields.find(el => el.name == f).get.tpe
      val newTag = tableColumnStoreTag(structDef.tag)
      val newType = tableColumnStoreType(newTag)
      val columnStorePointer = field(s, "columnStorePointer")(newType)
      val column = field(columnStorePointer, "arrayOf" + f)(typeArray(columnElemType))
      val idx = field(s, "index")(typeInt)
      ArrayUpdate(column.asInstanceOf[Expression[Array[Any]]], idx, v)
  }
}
