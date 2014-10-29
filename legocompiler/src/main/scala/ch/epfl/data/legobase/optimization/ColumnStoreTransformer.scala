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

object ColumnStoreTransformer extends TransformerHandler {
  def apply[Lang <: Base, T: PardisType](context: Lang)(block: context.Block[T]): context.Block[T] = {
    new ColumnStoreTransformer(context.asInstanceOf[LoweringLegoBase]).optimize(block)
  }
}

class ColumnStoreTransformer(override val IR: LoweringLegoBase) extends Optimizer[LoweringLegoBase](IR) with StructCollector[LoweringLegoBase] {
  import IR._
  val enabled = true

  def optimize[T: TypeRep](node: Block[T]): to.Block[T] = {
    if (enabled) {
      traverseBlock(node)
      transformProgram(node)
    } else node
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
      val elemType = an.tp.typeArguments(0).asInstanceOf[PardisType[Any]]

      /*  val hashMethod = doLambdaDef((x: Rep[Any]) => {
        //unit(0)
        infix_hashCode(field(x, "index")(IntType))(IntType)
      })(elemType, IntType).asInstanceOf[PardisLambdaDef]

      def eqMethod = doLambda3Def((x: Rep[Any], y: Rep[Any], z: Rep[Int]) => {
        val structDef = getStructDef(elemType).get
        structDef.fields.foldLeft(unit(true)) { (acc, f) =>
          val fx = field(x, f.name)(f.tpe)
          val fy = field(y, f.name)(f.tpe)
          acc && infix_==(fx, fy)
        }
      })(elemType, elemType, IntType, BooleanType).asInstanceOf[PardisLambdaDef]*/

      // Convert to column store only if this an array of record 
      if (elemType.isRecord) {
        val structDef = getStructDef(elemType).get
        val newElems = structDef.fields.map(el => PardisStructArg("arrayOf" + el.name, true, arrayNew(size)(el.tpe)))
        val newTag = tableColumnStoreTag(structDef.tag)
        val newType = tableColumnStoreType(newTag)
        //val newMethods = List(PardisStructMethod("equals", eqMethod), PardisStructMethod("hash", hashMethod))
        val newMethods = structDef.methods.map(m => m.copy(body =
          transformDef(m.body.asInstanceOf[Def[Any]]).asInstanceOf[PardisLambdaDef])) //)(s.tp)))

        //structsDefMap(newTag) =
        //  PardisStructDef(newTag.asInstanceOf[StructTags.StructTag[Any]],
        //  newElems.map(x => StructElemInformation(x.name, x.init.tp, x.mutable)),
        //  newMethods)(node.tp.asInstanceOf[TypeRep[Any]])
        PardisStruct(newTag, newElems, newMethods)(newType)
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

    /* Nodes returning struct nodes -- convert them to a reference to column store */
    case aa @ ArrayApply(arr, idx) =>
      if (aa.tp.isRecord) {
        val tpe = aa.tp.asInstanceOf[PardisType[Any]]

        val hashMethod = doLambdaDef((x: Rep[Any]) => {
          unit(0)
          //infix_hashCode(field(x, "index")(IntType))(IntType)
        })(tpe, IntType).asInstanceOf[PardisLambdaDef]

        val eqMethod = doLambda2Def((x: Rep[Any], y: Rep[Any]) => {
          printf(unit("EQUALS\n"))
          val structDef = getStructDef(arr.tp.typeArguments(0)).get
          val newTag = tableColumnStoreTag(structDef.tag)
          val newType = tableColumnStoreType(newTag)

          val fx = field(x, "columnStorePointer")(newType)

          //val fy = field(y, "columnStorePointer")(newType) //(arr.tp)
          val ix = field(x, "index")(IntType)
          System.out.println(new RecordType(structDef.tag))
          System.out.println(structsDefMap.map(sd => sd._1))
          System.out.println(getStructDef(new RecordType(structDef.tag)).get.fields.map(f => f.name))

          //val iy = field(y, "index")(IntType)
          /*System.out.println(x.tp)
          System.out.println(structsDefMap.map(s => s._1).mkString("\n"))
          System.out.println(fx.tp)
          val eq = getStructEqualsFunc()(fx.tp, typeLambda3(fx.tp, fy.tp, typeInt, typeBoolean))
          //__app(eq).apply(fx, fy, 0)*/
          printf(unit("EQUALS\n"))

          infix_==(fx, fx)
          //inlineFunction(eq.asInstanceOf[Rep[(Any, Any, Int) => Boolean]], fx, fy, 0)
          //__eqMethod(fx, fy, ix, iy)(x.tp)
          //unit(true)
        })(tpe, tpe, BooleanType).asInstanceOf[PardisLambdaDef]

        val newMethods = List(PardisStructMethod("equals", eqMethod), PardisStructMethod("hash", hashMethod))
        //val newMethods = List()
        val newElems = List(PardisStructArg("columnStorePointer", false, arr), PardisStructArg("index", false, idx))
        val newTag = StructTags.ClassTag(structName(aa.tp))
        // structsDefMap(newTag) = PardisStructDef(newTag.asInstanceOf[StructTags.StructTag[Any]],
        // newElems.map(x => StructElemInformation(x.name, x.init.tp, x.mutable)),
        // newMethods)(node.tp.asInstanceOf[TypeRep[Any]])
        PardisStruct(newTag, newElems, newMethods)(aa.tp)
      } else super.transformDef(node)

    /* case ps @ PardisStruct(tag, elems, methods) =>
      PardisStruct(StructTags.ClassTag(structName(ps.tp)),
        List(PardisStructArg("columnStorePointer", false, arr), PardisStructArg("index", false, idx)),
        List())(ps.tp)*/
    /* ----------------------------------------------------------------------------*/

    case psif @ PardisStructImmutableField(s, f) => {
      if (s.tp.isRecord) {
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
      } else super.transformDef(node)
    }
    case psif @ PardisStructFieldSetter(s, f, v) => {
      if (s.tp.isRecord) {
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
      } else super.transformDef(node)
    }
    //  case hmgeu @ HashMapGetOrElseUpdate(hm, k, v) =>
    //  HashMapGetOrElseUpdate(apply(hm), apply(k), apply(v))

    case _ => super.transformDef(node)
  }).asInstanceOf[to.Def[T]]
}
