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
        val v = s.asInstanceOf[PardisStruct[Any]].elems.find(e => e.name == el.name).get.init
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

class PartitionTransformer(override val IR: LoweringLegoBase) extends RuleBasedTransformer[LoweringLegoBase](IR) with StructCollector[LoweringLegoBase] {
  import IR._
  sealed trait AnalysePhase
  case object FindLoopLength extends AnalysePhase
  case object FindColumnArray extends AnalysePhase
  case object FindPartitionedLoop extends AnalysePhase

  var phase: AnalysePhase = FindLoopLength
  var insideLoopCondition = false
  val arrayLengthSymbols = scala.collection.mutable.Set[Rep[Any]]()
  val columnArraySymbols = scala.collection.mutable.Set[(Rep[Any], Rep[Any])]()
  val columnStorePartitionedSymbols = scala.collection.mutable.Set[Rep[Any]]()
  val partitionedArrayLengthSymbols = scala.collection.mutable.Map[Rep[Any], Rep[Any]]()
  val partitionedLoopSymbols = scala.collection.mutable.Map[Rep[Any], Rep[Any]]()
  val partitionedFunctionSymbols = scala.collection.mutable.Map[Rep[Any], Rep[Any]]()
  val partitionedUpdateVars = scala.collection.mutable.Map[Rep[Any], Rep[Any]]()

  val partitionedObjects = scala.collection.mutable.Set[PartitionObject]()

  val partitionedObjectsArray = scala.collection.mutable.Map[PartitionObject, Rep[Array[Any]]]()
  val partitionedObjectsCount = scala.collection.mutable.Map[PartitionObject, Rep[Array[Int]]]()

  def extractOriginalType[T](tp: PardisType[T]): Option[String] = {
    val columnStorePrefix = "ColumnStoreOf"
    if (tp.name.startsWith(columnStorePrefix)) {
      Some(tp.name.substring(columnStorePrefix.size))
    } else
      None
  }

  def shouldBePartitioned[T](tp: PardisType[T]): Boolean = {
    extractOriginalType(tp) match {
      case Some("LINEITEMRecord") => true
      case _                      => false
    }
  }

  def numBuckets[T](tp: PardisType[T]): Rep[Int] = extractOriginalType(tp) match {
    case Some("LINEITEMRecord") => unit(8)
    case _                      => ???
  }

  def bucketFunctionField[T](tp: PardisType[T]): String = "arrayOf" + {
    extractOriginalType(tp) match {
      case Some("LINEITEMRecord") => "L_SHIPDATE"
      case _                      => ???
    }
  }

  def bucketFunction[T](tp: PardisType[T], sym: Rep[Int]): Rep[Int] = extractOriginalType(tp) match {
    case Some("LINEITEMRecord") => (sym / unit(10000)) - unit(1992)
    case _                      => ???
  }

  case class PartitionObject(loopSymbol: Rep[Any], columnArray: Rep[Any], arrayLength: Rep[Any], partitionFunctionSymbol: Rep[Any])

  override def optimize[T: TypeRep](node: Block[T]): Block[T] = {
    phase = FindLoopLength
    traverseBlock(node)
    phase = FindColumnArray
    traverseBlock(node)
    // System.out.println(s"columnStorePartitionedSymbols: $columnStorePartitionedSymbols, partitionedArrayLengthSymbols: $partitionedArrayLengthSymbols")
    phase = FindPartitionedLoop
    traverseBlock(node)
    partitionedObjects ++= partitionedLoopSymbols.map({
      case (lengthSymbol, loopSymbol) => {
        val columnArray = partitionedArrayLengthSymbols(lengthSymbol)
        val pfs = partitionedFunctionSymbols(columnArray)
        PartitionObject(loopSymbol, columnArray, lengthSymbol, pfs)
      }
    })
    // System.out.println(s"partitionedLoopSymbols: $partitionedLoopSymbols")
    // System.out.println(s"partitionedObjects: $partitionedObjects")

    transformProgram(node)
  }

  analysis += rule {
    case While(cond, body) if phase == FindLoopLength => {
      // System.out.println(s"while $cond")
      insideLoopCondition = true
      traverseBlock(cond)
      insideLoopCondition = false
    }
  }

  analysis += statement {
    case sym -> Int$less3(x, y) if phase == FindLoopLength && insideLoopCondition => {
      arrayLengthSymbols += y
      ()
      // System.out.println(s"found $y: ${arrayLengthSymbols}")
    }
  }

  analysis += statement {
    case sym -> ArrayNew(s) if phase == FindColumnArray && arrayLengthSymbols.contains(s) => {
      columnArraySymbols += sym -> s
      ()
    }
  }

  analysis += statement {
    case sym -> Struct(_, elems, _) if phase == FindColumnArray && elems.exists(e => columnArraySymbols.exists(x => x._1 == e.init)) && shouldBePartitioned(sym.tp) => {
      columnStorePartitionedSymbols += sym
      val elem = elems.flatMap(e => columnArraySymbols.find(x => x._1 == e.init))
      partitionedArrayLengthSymbols += elem.head._2 -> sym
      ()
    }
  }

  var currentWhileSymbol: Rep[Any] = _

  analysis += statement {
    case sym -> While(cond, body) if phase == FindPartitionedLoop => {
      // System.out.println(s"while $cond")
      currentWhileSymbol = sym
      insideLoopCondition = true
      traverseBlock(cond)
      insideLoopCondition = false
    }
  }

  analysis += statement {
    case sym -> Int$less3(x, y) if phase == FindPartitionedLoop && insideLoopCondition && partitionedArrayLengthSymbols.exists(sym => sym._1 == y) => {
      partitionedLoopSymbols += y -> currentWhileSymbol
      ()
    }
  }

  analysis += rule {
    case ArrayUpdate(Def(StructImmutableField(s, name)), Def(ReadVar(idxVar)), v) if phase == FindPartitionedLoop && shouldBePartitioned(s.tp) && bucketFunctionField(s.tp) == name => {
      partitionedFunctionSymbols(s) = v
      partitionedUpdateVars(s) = idxVar.e
      // System.out.println(s"var $idxVar")
      ()
    }
  }

  def recreateNode[T: TypeRep](exp: Rep[T]): Rep[T] = exp match {
    case Def(node) => toAtom(node)(exp.tp)
    case _         => ???
  }

  def recreateArg(arg: PardisStructArg): PardisStructArg = arg.copy(init = recreateNode(arg.init))

  rewrite += statement {
    case sym -> Struct(tag, elems, methods) if partitionedObjects.exists(p => sym == p.columnArray) =>
      val partitionedObject = partitionedObjects.find(p => sym == p.columnArray).get
      class StructType
      implicit val typeS = sym.tp.asInstanceOf[PardisType[StructType]]
      val buckets = numBuckets(sym.tp)
      val partitionedArray = __newArray[StructType](buckets)
      val partitionedCount = __newArray[Int](buckets)
      partitionedObjectsArray += partitionedObject -> partitionedArray.asInstanceOf[Rep[Array[Any]]]
      partitionedObjectsCount += partitionedObject -> partitionedCount
      Range(unit(0), buckets).foreach {
        __lambda { i =>
          partitionedArray(i) = Struct[StructType](tag.asInstanceOf[StructTags.StructTag[StructType]], elems.map(recreateArg), methods).asInstanceOf[Def[StructType]]
        }
      }
      // System.out.println(s"partitionedArray $partitionedArray, sym $sym")
      sym
  }

  rewrite += rule {
    case ArrayUpdate(self @ Def(StructImmutableField(s, name)), _, v) if partitionedObjects.exists(p => s == p.columnArray) => {
      class StructType
      implicit val typeS = s.tp.asInstanceOf[PardisType[StructType]]
      val partitionedObject = partitionedObjects.find(p => s == p.columnArray).get
      val pfs = partitionedObject.partitionFunctionSymbol.asInstanceOf[Rep[Int]]
      val bucket = bucketFunction(s.tp, pfs)
      val lineitemCounts = partitionedObjectsCount(partitionedObject)
      val partitionedArray = partitionedObjectsArray(partitionedObject).asInstanceOf[Rep[Array[StructType]]]
      val idx = lineitemCounts(bucket)
      // lineitemCounts(bucket) += unit(1)
      val newS = partitionedArray(bucket)
      val newA = field(newS, name)(self.tp)
      newA(idx) = v
    }
  }

  rewrite += rule {
    case Assign(v, _) if partitionedUpdateVars.exists(x => x._2 == v.e) => {
      val columnArray = partitionedUpdateVars.find(x => x._2 == v.e).get._1
      val partitionedObject = partitionedObjects.find(p => columnArray == p.columnArray).get
      // System.out.println(s"assign $partitionedObject")
      val lineitemCounts = partitionedObjectsCount(partitionedObject)
      val pfs = partitionedObject.partitionFunctionSymbol.asInstanceOf[Rep[Int]]
      val bucket = bucketFunction(columnArray.tp, pfs)
      lineitemCounts(bucket) += unit(1)
    }
  }

  val partitionedObjectsLoopCount = scala.collection.mutable.Map[PartitionObject, Var[Int]]()
  val partitionedObjectsForeachCount = scala.collection.mutable.Map[PartitionObject, Rep[Int]]()

  rewrite += statement {
    case sym -> While(cond, body) if partitionedObjects.exists(p => sym == p.loopSymbol) => {
      // System.out.println(s"while $cond")
      val partitionedObject = partitionedObjects.find(p => sym == p.loopSymbol).get
      val buckets = numBuckets(partitionedObject.columnArray.tp)
      Range(unit(0), buckets).foreach {
        __lambda { i =>
          partitionedObjectsForeachCount(partitionedObject) = i
          currentWhileSymbol = sym
          insideLoopCondition = true
          val newCond = apply(cond)
          insideLoopCondition = false
          __assign(partitionedObjectsLoopCount(partitionedObject), unit(0))
          toAtom(While(newCond, apply(body)))
        }
      }
    }
  }

  rewrite += statement {
    case sym -> Int$less3(vSym @ Def(ReadVar(v)), y) if insideLoopCondition && partitionedObjects.exists(sym => sym.arrayLength == y) => {
      val partitionedObject = partitionedObjects.find(p => y == p.arrayLength).get
      partitionedObjectsLoopCount += partitionedObject -> v
      // System.out.println(s"here $partitionedObject $v")
      val lb = partitionedObjectsForeachCount(partitionedObject)
      val lineitemCounts = partitionedObjectsCount(partitionedObject)
      vSym < lineitemCounts(lb)
    }
  }

  rewrite += rule {
    case ArrayApply(self @ Def(StructImmutableField(s, name)), idx) if partitionedObjects.exists(p => s == p.columnArray) => {
      // System.out.println(s"lowering $self ${s.tp} ${s} ${self.correspondingNode}")
      class StructType
      implicit val typeS = s.tp.asInstanceOf[PardisType[StructType]]
      val partitionedObject = partitionedObjects.find(p => s == p.columnArray).get
      val lb = partitionedObjectsForeachCount(partitionedObject)
      val partitionedArray = partitionedObjectsArray(partitionedObject).asInstanceOf[Rep[Array[StructType]]]
      val newS = partitionedArray(lb)
      class SelfType
      implicit val typeSelf = self.tp.typeArguments(0).asInstanceOf[PardisType[SelfType]]
      val newA = field[Array[SelfType]](newS, name)

      val res = newA(idx)
      // System.out.println(s"array apply $newA, $idx, $res, ${res.tp}}")
      res
    }
  }

}
