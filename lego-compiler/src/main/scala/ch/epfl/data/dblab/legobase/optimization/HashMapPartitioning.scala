package ch.epfl.data
package dblab.legobase
package optimization

import schema._
import scala.language.implicitConversions
import sc.pardis.ir._
import reflect.runtime.universe.{ TypeTag, Type }
import sc.pardis.optimization._
import deep._
import sc.pardis.types._
import sc.pardis.types.PardisTypeImplicits._
import sc.pardis.shallow.utils.DefaultValue
import sc.pardis.quasi.anf._
import quasi._

/**
 * A transformer for partitioning and indexing the MultiMaps. As a result, this transformation
 * converts a MultiMap and the corresponding operations into an Array (either one dimensional or
 * two dimensional).
 *
 * TODO maybe add an example
 *
 * @param IR the polymorphic embedding trait which contains the reified program.
 */
class HashMapPartitioningTransformer(override val IR: LoweringLegoBase, val schema: Schema) extends RuleBasedTransformer[LoweringLegoBase](IR) with StructCollector[LoweringLegoBase] {
  import IR._
  val allMaps = scala.collection.mutable.Set[Rep[Any]]()
  val partitionedMaps = scala.collection.mutable.Set[Rep[Any]]()
  val leftPartFunc = scala.collection.mutable.Map[Rep[Any], String]()
  val rightPartFunc = scala.collection.mutable.Map[Rep[Any], String]()
  val leftPartArr = scala.collection.mutable.Map[Rep[Any], Rep[Array[Any]]]()
  val rightPartArr = scala.collection.mutable.Map[Rep[Any], Rep[Array[Any]]]()
  val leftLoopSymbol = scala.collection.mutable.Map[Rep[Any], While]()
  val rightLoopSymbol = scala.collection.mutable.Map[Rep[Any], While]()
  val hashJoinAntiMaps = scala.collection.mutable.Set[Rep[Any]]()
  val hashJoinAntiForeachLambda = scala.collection.mutable.Map[Rep[Any], Lambda[Any, Unit]]()
  var hashJoinAntiRetainVar = scala.collection.mutable.Map[Rep[Any], Var[Boolean]]()
  val setForeachLambda = scala.collection.mutable.Map[Rep[Any], Rep[Any => Unit]]()

  val windowOpMaps = scala.collection.mutable.Set[Rep[Any]]()

  def multiMapHasDefaultHandling[T](mm: Rep[T]): Boolean = getLoweredSymbolOriginalDef(mm) match {
    case Some(loj: LeftOuterJoinOpNew[_, _, _]) => true
    case _                                      => false
  }

  val ONE_D_ENABLED = true

  def isPrimaryKey[T](tp: TypeRep[T], field: String): Boolean =
    schema.findTableByType(tp).exists(table => table.primaryKey.exists(pk => pk.attributes.forall(att => att.name == field)))

  var transformedMapsCount = 0

  case class HashMapPartitionObject(mapSymbol: Rep[Any], left: Option[PartitionObject], right: Option[PartitionObject]) {
    def partitionedObject: PartitionObject = (left, right) match {
      case _ if isAnti        => right.get
      case (Some(v), None)    => v
      case (None, Some(v))    => v
      case (Some(l), Some(r)) => l
      case _                  => throw new Exception(s"$this doesn't have partitioned object")
    }
    def hasLeft: Boolean =
      left.nonEmpty
    def isAnti: Boolean = hashJoinAntiMaps.contains(mapSymbol)
    def antiLambda: Lambda[Any, Unit] = hashJoinAntiForeachLambda(mapSymbol)
  }
  case class PartitionObject(arr: Rep[Array[Any]], fieldFunc: String, loopSymbol: While) {
    def tpe = arr.tp.typeArguments(0).asInstanceOf[TypeRep[Any]]
    def buckets = numBuckets(this)
    def count = partitionedObjectsCount(this)
    def parArr = partitionedObjectsArray(this).asInstanceOf[Rep[Array[Array[Any]]]]
    def is1D: Boolean = if (ONE_D_ENABLED) isPrimaryKey(tpe, fieldFunc) else false
    def table: Table = schema.findTableByType(tpe).get
    def reuseOriginal1DArray: Boolean = table.continuous.nonEmpty
    def arraySize: Rep[Int] = arr match {
      // case Def(ArrayNew(s)) => s
      case dsl"new Array[Any]($s)" => s
    }
  }
  val partitionedHashMapObjects = scala.collection.mutable.Set[HashMapPartitionObject]()
  val partitionedObjectsArray = scala.collection.mutable.Map[PartitionObject, Rep[Array[Any]]]()
  val partitionedObjectsCount = scala.collection.mutable.Map[PartitionObject, Rep[Array[Int]]]()

  // TODO should be `|| ?.right.nonEmpty`
  def shouldBePartitioned[T: TypeRep](hm: Rep[T]): Boolean = {
    val isMultiMap = partitionedMaps.exists(x => x == hm)
    isMultiMap && {
      (getPartitionedObject(hm).left.nonEmpty || getPartitionedObject(hm).right.nonEmpty)
    }
  }
  def getPartitionedObject[T: TypeRep](hm: Rep[T]): HashMapPartitionObject = partitionedHashMapObjects.find(x => x.mapSymbol == hm).get

  override def optimize[T: TypeRep](node: Block[T]): Block[T] = {
    val res = super.optimize(node)
    System.out.println(s"${scala.Console.GREEN}[$transformedMapsCount] MultiMaps partitioned!${scala.Console.RESET}")
    res
  }

  override def postAnalyseProgram[T: TypeRep](node: Block[T]): Unit = {
    val realHashJoinAntiMaps = hashJoinAntiMaps intersect windowOpMaps
    windowOpMaps --= realHashJoinAntiMaps
    hashJoinAntiMaps.clear()
    hashJoinAntiMaps ++= realHashJoinAntiMaps
    // val supportedMaps = partitionedMaps diff windowOpMaps
    partitionedHashMapObjects ++= partitionedMaps.map({ hm =>
      val left = leftPartArr.get(hm).map(v => PartitionObject(v, leftPartFunc(hm), leftLoopSymbol(hm)))
      val right = rightPartArr.get(hm).map(v => PartitionObject(v, rightPartFunc(hm), rightLoopSymbol(hm)))
      HashMapPartitionObject(hm, left, right)
    })
  }

  analysis += statement {
    // TODO `new MultiMap` is synthetic and does not exist in shallow
    case sym -> (node @ MultiMapNew()) if node.typeB.isRecord =>
      allMaps += sym
      ()
  }

  analysis += rule {
    // case node @ MultiMapAddBinding(nodeself, Def(StructImmutableField(struct, field)), nodev) if allMaps.contains(nodeself) =>
    case node @ dsl"($nodeself : MultiMap[Any, Any]).addBinding(__struct_field($struct, $fieldNode), $nodev)" if allMaps.contains(nodeself) =>
      val Constant(field) = fieldNode
      partitionedMaps += nodeself
      leftPartFunc += nodeself -> field
      leftLoopSymbol += nodeself -> currentLoopSymbol
      def addPartArray(exp: Rep[Any]): Unit =
        exp match {
          // case Def(ArrayApply(arr, ind)) => 
          case dsl"($arr: Array[Any]).apply($index)" =>
            leftPartArr += nodeself -> arr
          case _ =>
        }
      addPartArray(struct)
  }

  val leftPartKey = scala.collection.mutable.Map[Rep[Any], Var[Any]]()

  analysis += rule {
    // TODO needs struct immutable field support from qq
    // case node @ MultiMapGet(nodeself, Def(StructImmutableField(struct, field))) if allMaps.contains(nodeself) =>
    case node @ dsl"($nodeself : MultiMap[Any, Any]).get(__struct_field($struct, $fieldNode))" if allMaps.contains(nodeself) =>
      val Constant(field) = fieldNode
      partitionedMaps += nodeself
      rightPartFunc += nodeself -> field
      rightLoopSymbol += nodeself -> currentLoopSymbol
      struct match {
        // case Def(ArrayApply(arr, ind)) => 
        case dsl"($arr: Array[Any]).apply($index)" =>
          rightPartArr += nodeself -> arr
        case _ =>
      }
      ()
  }

  // TODO when WindowOp is supported, this case should be removed
  analysis += rule {
    case dsl"($mm : MultiMap[Any, Any]).foreach($f)" if allMaps.contains(mm) =>
      // case node @ MultiMapForeach(mm, f) if allMaps.contains(mm) =>
      windowOpMaps += mm
      f match {
        case Def(fun @ Lambda(_, _, _)) => hashJoinAntiForeachLambda += mm -> fun.asInstanceOf[Lambda[Any, Unit]]
        case _                          => ()
      }
      ()
  }

  analysis += rule {
    case dsl"($mm : MultiMap[Any, Any]).get($elem).get" if allMaps.contains(mm) =>
      // case OptionGet(Def(MultiMapGet(mm, elem))) if allMaps.contains(mm) =>
      // At this phase it's potentially anti hash join multimap, it's not specified for sure
      hashJoinAntiMaps += mm
      ()
  }

  var currentLoopSymbol: While = _

  analysis += statement {
    case sym -> (node @ dsl"while($cond) $body") =>
      // case sym -> (node @ While(_, _)) =>
      currentLoopSymbol = node.asInstanceOf[While]
  }

  analysis += rule {
    case dsl"($mm : MultiMap[Any, Any]).get($elem).get.foreach($f)" => {
      // case SetForeach(Def(OptionGet(Def(MultiMapGet(mm, elem)))), f) => {
      setForeachLambda(mm) = f.asInstanceOf[Rep[Any => Unit]]
    }
  }

  def numBuckets(partitionedObject: PartitionObject): Rep[Int] =
    unit(schema.stats.getDistinctAttrValues(partitionedObject.fieldFunc))

  def bucketSize(partitionedObject: PartitionObject): Rep[Int] =
    schema.stats.getConflictsAttr(partitionedObject.fieldFunc) match {
      case Some(v) => unit(v)
      case None    => unit(1 << 10)
    }

  def recreateNode[T: TypeRep](exp: Rep[T]): Rep[T] = exp match {
    case Def(node) => toAtom(node)(exp.tp)
    case _         => ???
  }

  def array_foreach[T: TypeRep](arr: Rep[Array[T]], f: Rep[T] => Rep[Unit]): Rep[Unit] = {
    Range(unit(0), arr.length).foreach {
      __lambda { i =>
        val e = arr(i)
        f(e)
      }
    }
  }

  def findBucketFunction(key: Rep[Int], partitionedObject: PartitionObject): Rep[Int] = key % partitionedObject.buckets

  def par_array_foreach[T: TypeRep](partitionedObject: PartitionObject, key: Rep[Int], f: Rep[T] => Rep[Unit]): Rep[Unit] = {
    if (partitionedObject.is1D) {
      val parArr = partitionedObject.parArr.asInstanceOf[Rep[Array[T]]]
      val bucket = partitionedObject.table.continuous match {
        case Some(continuous) => key - unit(continuous.offset)
        case None             => key
      }
      val e = parArr(bucket)
      // System.out.println(s"part foreach for val $e=$parArr($bucket) ")
      f(e)
    } else {
      val bucket = findBucketFunction(key, partitionedObject)
      val count = partitionedObject.count(bucket)
      val parArrWhole = partitionedObject.parArr.asInstanceOf[Rep[Array[Array[T]]]]
      val parArr = parArrWhole(bucket)
      Range(unit(0), count).foreach {
        __lambda { i =>
          val e = parArr(i)
          f(e)
        }
      }
    }
  }

  def createPartitionArray(partitionedObject: PartitionObject): Unit = {
    System.out.println(scala.Console.GREEN + "Table " + partitionedObject.arr.tp.typeArguments(0) + " was partitioned on field " + partitionedObject.fieldFunc + scala.Console.RESET)

    class InnerType
    implicit val typeInner = partitionedObject.arr.tp.typeArguments(0).asInstanceOf[TypeRep[InnerType]]
    val originalArray = {
      val origArray =
        if (!seenArrays.contains(partitionedObject.arr)) {
          seenArrays += partitionedObject.arr
          recreateNode(partitionedObject.arr)
        } else {
          partitionedObject.arr
        }
      origArray.asInstanceOf[Rep[Array[InnerType]]]
    }
    val buckets = partitionedObject.buckets
    if (partitionedObject.is1D) {
      //System.out.println(s"${scala.Console.BLUE}1D Array!!!${scala.Console.RESET}")
      if (partitionedObject.reuseOriginal1DArray) {
        partitionedObjectsArray += partitionedObject -> originalArray.asInstanceOf[Rep[Array[Any]]]
      } else {
        val partitionedArray = __newArray[InnerType](buckets)
        partitionedObjectsArray += partitionedObject -> partitionedArray.asInstanceOf[Rep[Array[Any]]]
        array_foreach(originalArray, {
          (e: Rep[InnerType]) =>
            val pkey = field[Int](e, partitionedObject.fieldFunc) % buckets
            partitionedArray(pkey) = e
        })
      }
    } else {
      val partitionedObjectAlreadyExists = {
        partitionedObjectsArray.find({
          case (po, _) =>
            po.fieldFunc == partitionedObject.fieldFunc && po.tpe == partitionedObject.tpe
        })
      }
      if (partitionedObjectAlreadyExists.nonEmpty) {
        System.out.println(s"${scala.Console.BLUE}2D Array already exists!${scala.Console.RESET}")
        partitionedObjectsArray += partitionedObject -> partitionedObjectAlreadyExists.get._1.parArr.asInstanceOf[Rep[Array[Any]]]
        partitionedObjectsCount += partitionedObject -> partitionedObjectAlreadyExists.get._1.count
      } else {
        val partitionedArray = __newArray[Array[InnerType]](buckets)
        val partitionedCount = __newArray[Int](buckets)
        partitionedObjectsArray += partitionedObject -> partitionedArray.asInstanceOf[Rep[Array[Any]]]
        partitionedObjectsCount += partitionedObject -> partitionedCount
        Range(unit(0), buckets).foreach {
          __lambda { i =>
            partitionedArray(i) = __newArray[InnerType](bucketSize(partitionedObject))
          }
        }
        val index = __newVarNamed[Int](unit(0), "partIndex")
        array_foreach(originalArray, {
          (e: Rep[InnerType]) =>
            // TODO needs a better way of computing the index of each object
            val pkey = field[Int](e, partitionedObject.fieldFunc) % buckets
            val currIndex = partitionedCount(pkey)
            val partitionedArrayBucket = partitionedArray(pkey)
            partitionedArrayBucket(currIndex) = e
            partitionedCount(pkey) = currIndex + unit(1)
            __assign(index, readVar(index) + unit(1))
        })
      }
    }
  }

  val seenArrays = scala.collection.mutable.Set[Rep[Any]]()

  rewrite += statement {
    case sym -> (node @ dsl"new Array[Any]($size)") /*if !seenArrays.contains(sym)*/ =>
      seenArrays += sym
      reflectStm(Stm(sym, node))
      sym
  }

  rewrite += statement {
    case sym -> (node @ MultiMapNew()) if shouldBePartitioned(sym) /* && !windowOpMaps.contains(sym)*/ => {
      val hmParObj = getPartitionedObject(sym)(sym.tp)

      createPartitionArray(hmParObj.partitionedObject)

      sym
    }
  }

  // rewrite += removeStatement {
  //   case sym -> (node @ MultiMapNew()) if shouldBePartitioned(sym) && windowOpMaps.contains(sym) =>
  //     ()
  // }

  rewrite += remove {
    case dsl"($mm: MultiMap[Any, Any]).get($elem)" if shouldBePartitioned(mm) => {
      ()
    }
  }

  // The case for HashJoinAnti
  rewrite += rule {
    // case MultiMapAddBinding(mm, elem, nodev) if shouldBePartitioned(mm) && getPartitionedObject(mm).isAnti =>
    case dsl"($mm: MultiMap[Any, Any]).addBinding($elem, $nodev)" if shouldBePartitioned(mm) && getPartitionedObject(mm).isAnti =>
      class ElemType
      implicit val elemType = nodev.tp.asInstanceOf[TypeRep[ElemType]]
      val value = apply(nodev).asInstanceOf[Rep[ElemType]]
      val hmParObj = getPartitionedObject(mm)
      val rightArray = hmParObj.partitionedObject
      val key = apply(elem).asInstanceOf[Rep[Int]]
      val antiLambda = hmParObj.antiLambda
      val foreachFunction = antiLambda.body.stmts.collect({ case Statement(sym, SetForeach(_, f)) => f }).head.asInstanceOf[Rep[ElemType => Unit]]
      val resultRetain = __newVarNamed[Boolean](unit(false), "resultRetain")
      hashJoinAntiRetainVar += mm -> resultRetain
      class ElemType2
      implicit val elemType2 = rightArray.arr.tp.typeArguments(0).asInstanceOf[TypeRep[ElemType2]]
      par_array_foreach[ElemType2](rightArray, key, (e: Rep[ElemType2]) => {
        fillingElem(mm) = e
        fillingFunction(mm) = () => apply(nodev)
        fillingHole(mm) = loopDepth
        loopDepth += 1
        val res = inlineBlock2(rightArray.loopSymbol.body)
        fillingHole.remove(mm)
        loopDepth -= 1
        res
      })
      transformedMapsCount += 1
      __ifThenElse(!readVar(resultRetain), {
        inlineFunction(foreachFunction, value)
      }, unit())
  }

  rewrite += remove {
    case dsl"($mm: MultiMap[Any, Any]).addBinding($elem, $nodev)" if shouldBePartitioned(mm) && getPartitionedObject(mm).hasLeft && fillingHole.get(mm).isEmpty =>
      ()
  }

  rewrite += rule {
    case dsl"($mm: MultiMap[Any, Any]).addBinding($elem, $nodev)" if shouldBePartitioned(mm) && getPartitionedObject(mm).hasLeft && fillingHole.get(mm).nonEmpty =>
      fillingFunction(mm)()
  }

  rewrite += rule {
    case dsl"($mm: MultiMap[Any, Any]).addBinding($elem, $nodev)" if shouldBePartitioned(mm) && !getPartitionedObject(mm).hasLeft =>
      val hmParObj = getPartitionedObject(mm)
      val leftArray = hmParObj.partitionedObject
      val key = apply(elem).asInstanceOf[Rep[Int]]
      val whileLoop = leftArray.loopSymbol
      class InnerType
      implicit val typeInner = leftArray.tpe.asInstanceOf[TypeRep[InnerType]]
      par_array_foreach[InnerType](leftArray, key, (e: Rep[InnerType]) => {
        fillingElem(mm) = e
        fillingFunction(mm) = () => apply(nodev)
        fillingHole(mm) = loopDepth
        loopDepth += 1
        val res1 = inlineBlock2(whileLoop.body)
        fillingHole.remove(mm)
        loopDepth -= 1
        transformedMapsCount += 1
        res1
      })
  }

  rewrite += rule {
    case dsl"($arr: Array[Any]).apply($index)" if partitionedHashMapObjects.exists(obj => shouldBePartitioned(obj.mapSymbol) && /*!obj.isWindow &&*/ obj.partitionedObject.arr == arr && fillingHole.get(obj.mapSymbol).nonEmpty) =>
      val allObjs = partitionedHashMapObjects.filter(obj => shouldBePartitioned(obj.mapSymbol) && /*!obj.isWindow &&*/ obj.partitionedObject.arr == arr && fillingHole.get(obj.mapSymbol).nonEmpty)
      val sortedObjs = allObjs.toList.sortBy(obj => fillingHole(obj.mapSymbol))
      val hmPartitionedObject = sortedObjs.last
      // System.out.println(s"filling array apply hole with ${fillingElem(hmPartitionedObject.mapSymbol)}: ${hmPartitionedObject.partitionedObject.tpe}, all: ${partitionedHashMapObjects.map(x => x.mapSymbol -> fillingHole.get(x.mapSymbol) -> x.partitionedObject.tpe).mkString("\n")}")
      fillingElem(hmPartitionedObject.mapSymbol)
  }

  rewrite += remove {
    case node @ dsl"while($cond) $body" if partitionedHashMapObjects.exists(obj => shouldBePartitioned(obj.mapSymbol) && /*!obj.isWindow &&*/ obj.partitionedObject.loopSymbol == node) =>
      ()
  }

  rewrite += rule {
    // case OptionNonEmpty(Def(MultiMapGet(mm, elem))) if shouldBePartitioned(mm) =>
    case dsl"($mm: MultiMap[Any, Any]).get($elem).nonEmpty" if shouldBePartitioned(mm) =>
      unit(true)
  }

  rewrite += remove {
    // case OptionGet(Def(MultiMapGet(mm, elem))) if shouldBePartitioned(mm) => 
    case dsl"($mm: MultiMap[Any, Any]).get($elem).get" if shouldBePartitioned(mm) =>
      ()
  }

  var loopDepth: Int = 0
  // associates each multimap with a level which specifies the depth of the corresponding for loop
  val fillingHole = scala.collection.mutable.Map[Rep[Any], Int]()
  var fillingFunction = scala.collection.mutable.Map[Rep[Any], () => Rep[Any]]()
  var fillingElem = scala.collection.mutable.Map[Rep[Any], Rep[Any]]()

  rewrite += removeStatement {
    case sym -> Lambda(_, _, _) if setForeachLambda.exists(_._2 == sym) =>
      ()
  }

  rewrite += rule {
    // case SetForeach(Def(OptionGet(Def(MultiMapGet(mm, elem)))), f) if shouldBePartitioned(mm) && getPartitionedObject(mm).hasLeft => {
    case dsl"($mm: MultiMap[Any, Any]).get($elem).get.foreach($f)" if shouldBePartitioned(mm) && getPartitionedObject(mm).hasLeft => {
      val hmParObj = getPartitionedObject(mm)
      val leftArray = hmParObj.partitionedObject
      val key = apply(elem).asInstanceOf[Rep[Int]]
      val whileLoop = leftArray.loopSymbol
      leftOuterJoinExistsVarDefine(mm)
      class InnerType
      implicit val typeInner = leftArray.tpe.asInstanceOf[TypeRep[InnerType]]
      par_array_foreach[InnerType](leftArray, key, (e: Rep[InnerType]) => {
        fillingElem(mm) = e
        fillingFunction(mm) = () => {
          def ifThenBody = {
            leftOuterJoinExistsVarSet(mm)
            inlineFunction(f.asInstanceOf[Rep[InnerType => Unit]], e)
          }
          __ifThenElse[Unit](field[Int](e, leftArray.fieldFunc) __== apply(elem), {
            ifThenBody
          }, {
            unit(())
          })
        }
        // System.out.println(s"STARTED setforeach for the key $key $e.${leftArray.fieldFunc} mm: $mm")
        fillingHole(mm) = loopDepth
        loopDepth += 1
        val res1 = inlineBlock2(whileLoop.body)
        // System.out.println(s"FINISH setforeach for the key $key")
        fillingHole.remove(mm)
        loopDepth -= 1
        transformedMapsCount += 1
        res1
      })
      leftOuterJoinDefaultHandling(mm, key, leftArray)
    }
  }

  rewrite += rule {
    // case SetExists(Def(OptionGet(Def(MultiMapGet(mm, elem)))), p) if shouldBePartitioned(mm) && getPartitionedObject(mm).hasLeft => {
    case dsl"($mm: MultiMap[Any, Any]).get($elem).get.exists($p)" if shouldBePartitioned(mm) &&
      getPartitionedObject(mm).hasLeft => {
      val hmParObj = getPartitionedObject(mm)
      val leftArray = hmParObj.partitionedObject
      val key = apply(elem).asInstanceOf[Rep[Int]]
      val whileLoop = leftArray.loopSymbol
      val result = __newVarNamed[Boolean](unit(false), "existsResult")
      class InnerType
      implicit val typeInner = leftArray.tpe.asInstanceOf[TypeRep[InnerType]]
      par_array_foreach[InnerType](leftArray, key, (e: Rep[InnerType]) => {
        fillingElem(mm) = e
        fillingFunction(mm) = () => {
          __ifThenElse[Unit]((field[Int](e, leftArray.fieldFunc) __== elem) && inlineFunction(p, e), {
            __assign(result, unit(true))
          }, {
            unit(())
          })
        }
        fillingHole(mm) = loopDepth
        loopDepth += 1
        val res1 = inlineBlock2(whileLoop.body)
        fillingHole.remove(mm)
        loopDepth -= 1
        transformedMapsCount += 1
        res1
      })
      readVar(result)
    }
  }

  rewrite += remove {
    // case SetForeach(Def(OptionGet(Def(MultiMapGet(mm, elem)))), f) if shouldBePartitioned(mm) && !getPartitionedObject(mm).hasLeft && fillingHole.get(mm).isEmpty =>
    case dsl"($mm: MultiMap[Any, Any]).get($elem).get.foreach($f)" if shouldBePartitioned(mm) &&
      !getPartitionedObject(mm).hasLeft &&
      fillingHole.get(mm).isEmpty =>
      ()
  }

  rewrite += remove {
    // case MultiMapForeach(mm, f) if shouldBePartitioned(mm) && getPartitionedObject(mm).isAnti =>
    case dsl"($mm: MultiMap[Any, Any]).foreach($f)" if shouldBePartitioned(mm) &&
      getPartitionedObject(mm).isAnti =>
      ()
  }

  rewrite += rule {
    case IfThenElse(Def(OptionNonEmpty(Def(MultiMapGet(mm, elem)))), thenp, elsep) if shouldBePartitioned(mm) &&
      getPartitionedObject(mm).isAnti &&
      fillingHole.get(mm).nonEmpty =>
      class ElemType
      val retainPredicate = thenp.stmts.collect({
        case Statement(sym, SetRetain(_, p)) => p
      }).head.asInstanceOf[Rep[ElemType => Boolean]]
      val typedElem = fillingFunction(mm)().asInstanceOf[Rep[ElemType]]
      implicit val elemType = typedElem.tp.asInstanceOf[TypeRep[ElemType]]
      val resultRetain = hashJoinAntiRetainVar(mm)
      __ifThenElse[Unit](!inlineFunction(retainPredicate, typedElem), {
        __assign(resultRetain, unit(true))
      }, {
        unit()
      })
  }

  rewrite += rule {
    // case SetForeach(Def(OptionGet(Def(MultiMapGet(mm, elem)))), f) if shouldBePartitioned(mm) && !getPartitionedObject(mm).hasLeft && fillingHole.get(mm).nonEmpty =>
    case dsl"($mm: MultiMap[Any, Any]).get($elem).get.foreach($f)" if shouldBePartitioned(mm) &&
      !getPartitionedObject(mm).hasLeft &&
      fillingHole.get(mm).nonEmpty =>
      inlineFunction(f, fillingFunction(mm)())
  }

  /* The parts dedicated to left outer join handling */
  def leftOuterJoinDefaultHandling(mm: Rep[MultiMap[Any, Any]], key: Rep[Int], partitionedObject: PartitionObject): Rep[Unit] = if (multiMapHasDefaultHandling(mm)) {
    __ifThenElse(!readVar(leftOuterJoinExistsVar(mm)), {
      inlineBlock[Unit](leftOuterJoinDefault(mm))
    }, unit(()))
  } else unit(())

  def leftOuterJoinExistsVarDefine(mm: Rep[MultiMap[Any, Any]]): Unit =
    if (multiMapHasDefaultHandling(mm)) {
      val exists = __newVarNamed[Boolean](unit(false), "exists")
      leftOuterJoinExistsVar(mm) = exists
      ()
    } else ()

  def leftOuterJoinExistsVarSet(mm: Rep[MultiMap[Any, Any]]): Unit =
    if (multiMapHasDefaultHandling(mm)) {
      val exists = leftOuterJoinExistsVar(mm)
      __assign(exists, unit(true))
      ()
    } else ()

  val leftOuterJoinDefault = scala.collection.mutable.Map[Rep[Any], Block[Unit]]()
  val leftOuterJoinExistsVar = scala.collection.mutable.Map[Rep[Any], Var[Boolean]]()

  analysis += rule {
    case IfThenElse(Def(OptionNonEmpty(Def(MultiMapGet(mm, elem)))), thenp, elsep) if multiMapHasDefaultHandling(mm) => {
      // System.out.println(s"elsep: $elsep")
      leftOuterJoinDefault += mm -> elsep.asInstanceOf[Block[Unit]]
      ()
    }
  }
}
