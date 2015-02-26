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

class HashMapPartitioningTransformer(override val IR: LoweringLegoBase, val queryNumber: Int, val scalingFactor: Double) extends RuleBasedTransformer[LoweringLegoBase](IR) with StructCollector[LoweringLegoBase] {
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
  // TODO when WindowOp is supported will be removed
  val windowOpMaps = scala.collection.mutable.Set[Rep[Any]]()

  case class WindowOpMetaInfo[T <: Def[_]](arr: Rep[Array[Any]], node: T)
  val windowOpFoldLefts = scala.collection.mutable.Map[Rep[Any], scala.collection.mutable.Set[WindowOpMetaInfo[SetFoldLeft[_, _]]]]()
  val windowOpMins = scala.collection.mutable.Map[Rep[Any], scala.collection.mutable.Set[WindowOpMetaInfo[SetMinBy[_, _]]]]()

  val SIZE_ORDER = List("REGIONRecord", "NATIONRecord", "SUPPLIERRecord", "CUSTOMERRecord", "PARTRecord", "PARTSUPPRecord", "ORDERSRecord", "LINEITEMRecord")

  def getSizeOrder[T](tp: TypeRep[T]): Int = {
    val name = tp.name
    SIZE_ORDER.zipWithIndex.find(x => x._1 == name).get._2
  }

  val ONE_D_ENABLED = true

  def isPrimaryKey[T](tp: TypeRep[T], field: String): Boolean = (tp.name, field) match {
    case ("REGIONRecord", "R_REGIONKEY") => true
    case ("NATIONRecord", "N_NATIONKEY") => true
    case ("SUPPLIERRecord", "S_SUPPKEY") => true
    case ("CUSTOMERRecord", "C_CUSTKEY") => true
    case ("PARTRecord", "P_PARTKEY")     => true
    // case ("PARTSUPPRecord", _) => false
    case ("ORDERSRecord", "O_ORDERKEY")  => true
    // case ("LINEITEMRecord", "L_ORDERKEY") => true
    case _                               => false
  }

  var transformedMapsCount = 0

  case class HashMapPartitionObject(mapSymbol: Rep[Any], left: Option[PartitionObject], right: Option[PartitionObject]) {
    def partitionedObject: PartitionObject = (left, right) match {
      case _ if isAnti        => right.get
      case (Some(v), None)    => v
      case (None, Some(v))    => v
      // case (Some(l), Some(r)) => if (getSizeOrder(l.tpe) < getSizeOrder(r.tpe)) l else r
      case (Some(l), Some(r)) => l
      case _                  => throw new Exception(s"$this doesn't have partitioned object")
    }
    def hasLeft: Boolean = //left.exists(l => l == partitionedObject)
      left.nonEmpty
    def isAnti: Boolean = hashJoinAntiMaps.contains(mapSymbol)
    def antiLambda: Lambda[Any, Unit] = hashJoinAntiForeachLambda(mapSymbol)
    def isWindow: Boolean = windowOpMaps.contains(mapSymbol)
  }
  case class PartitionObject(arr: Rep[Array[Any]], fieldFunc: String, loopSymbol: While) {
    def tpe = arr.tp.typeArguments(0).asInstanceOf[TypeRep[Any]]
    def buckets = if (is1D) numBucketsFull(this) else numBuckets(this)
    def count = partitionedObjectsCount(this)
    def parArr = partitionedObjectsArray(this).asInstanceOf[Rep[Array[Array[Any]]]]
    def is1D: Boolean = if (ONE_D_ENABLED) isPrimaryKey(tpe, fieldFunc) else false
    def reuseOriginal1DArray: Boolean = List("REGIONRecord", "NATIONRecord", "SUPPLIERRecord", "CUSTOMERRecord", "PARTRecord").contains(tpe.name)
    def arraySize: Rep[Int] = arr match {
      case Def(ArrayNew(s)) => s
    }
  }
  val partitionedHashMapObjects = scala.collection.mutable.Set[HashMapPartitionObject]()
  val partitionedObjectsArray = scala.collection.mutable.Map[PartitionObject, Rep[Array[Any]]]()
  val partitionedObjectsCount = scala.collection.mutable.Map[PartitionObject, Rep[Array[Int]]]()

  // TODO uncomment
  def supportsWindowOp(): Boolean = //(queryNumber == 2) || (queryNumber == 11)
    false

  // TODO should be `|| ?.right.nonEmpty`
  def shouldBePartitioned[T: TypeRep](hm: Rep[T]): Boolean = {
    val isWindowOp = windowOpMaps.exists(x => x == hm)
    val isMultiMap = partitionedMaps.exists(x => x == hm)
    isMultiMap && {
      if (isWindowOp)
        supportsWindowOp()
      else
        (getPartitionedObject(hm).left.nonEmpty || getPartitionedObject(hm).right.nonEmpty)
    }
  }
  def getPartitionedObject[T: TypeRep](hm: Rep[T]): HashMapPartitionObject = partitionedHashMapObjects.find(x => x.mapSymbol == hm).get

  override def optimize[T: TypeRep](node: Block[T]): Block[T] = {
    traverseBlock(node)
    val realHashJoinAntiMaps = hashJoinAntiMaps intersect windowOpMaps
    windowOpMaps --= realHashJoinAntiMaps // TODO should be uncommented
    hashJoinAntiMaps.clear()
    hashJoinAntiMaps ++= realHashJoinAntiMaps
    val supportedMaps = partitionedMaps diff windowOpMaps
    partitionedHashMapObjects ++= partitionedMaps.map({ hm =>
      val left = leftPartArr.get(hm).map(v => PartitionObject(v, leftPartFunc(hm), leftLoopSymbol(hm)))
      val right = rightPartArr.get(hm).map(v => PartitionObject(v, rightPartFunc(hm), rightLoopSymbol(hm)))
      HashMapPartitionObject(hm, left, right)
    })
    val res = {
      if (supportsWindowOp()) {
        val windowOpMap = partitionedHashMapObjects.find(_.isWindow).get
        reifyBlock {
          analyseWindowLambda(windowOpMap)

          transformProgram(node)
        }
      } else {
        transformProgram(node)
      }
    }
    System.out.println(s"[${scala.Console.BLUE}$transformedMapsCount${scala.Console.RESET}] MultiMaps partitioned!")
    res
  }

  def analyseWindowLambda(windowOpMap: HashMapPartitionObject) {
    val mm = windowOpMap.mapSymbol
    val foldLefts = windowOpMap.antiLambda.body.stmts.collect({
      case Statement(sym, s @ SetFoldLeft(_, _, _)) => sym.asInstanceOf[Rep[Any]] -> s
    })
    val mins = windowOpMap.antiLambda.body.stmts.collect({
      case Statement(sym, s @ SetMinBy(_, _)) => sym.asInstanceOf[Rep[Any]] -> s
    })

    windowOpFoldLefts(mm) = scala.collection.mutable.Set()
    for (fl <- foldLefts) {
      val arr = __newArray(NUM_AGGS)(fl._1.tp)
      windowOpFoldLefts(mm) += WindowOpMetaInfo(arr, fl._2)
    }

    windowOpMins(mm) = scala.collection.mutable.Set()
    for (m <- mins) {
      val arr = __newArray(NUM_AGGS)(m._1.tp)
      windowOpMins(mm) += WindowOpMetaInfo(arr, m._2)
    }
  }

  val NUM_AGGS = unit(160000)

  analysis += statement {
    case sym -> (node @ MultiMapNew()) if node.typeB.isRecord =>
      allMaps += sym
      ()
  }

  analysis += rule {
    case node @ MultiMapAddBinding(nodeself, Def(StructImmutableField(struct, field)), nodev) if allMaps.contains(nodeself) =>
      partitionedMaps += nodeself
      leftPartFunc += nodeself -> field
      leftLoopSymbol += nodeself -> currentLoopSymbol
      def addPartArray(exp: Rep[Any]): Unit =
        exp match {
          case Def(ArrayApply(arr, ind)) => leftPartArr += nodeself -> arr
          // case Def(Struct(_, elems, _)) if elems.exists(e => e.name == field) =>
          //   val init = elems.find(_.name == field).get.init
          //   init match {
          //     case Def(StructImmutableField(struct, field)) =>
          //       System.out.println(s"${scala.Console.RED}WINDOWOP ${struct.tp}${scala.Console.RESET}")
          //       addPartArray(struct)
          //     case _ =>
          //   }
          case _                         =>
        }
      addPartArray(struct)
  }

  analysis += rule {
    case node @ MultiMapGet(nodeself, Def(StructImmutableField(struct, field))) if allMaps.contains(nodeself) =>
      partitionedMaps += nodeself
      rightPartFunc += nodeself -> field
      rightLoopSymbol += nodeself -> currentLoopSymbol
      struct match {
        case Def(ArrayApply(arr, ind)) => rightPartArr += nodeself -> arr
        case _                         =>
      }
      ()
  }

  // TODO when WindowOp is supported, this case should be removed
  analysis += rule {
    case node @ MultiMapForeach(nodeself, f) if allMaps.contains(nodeself) =>
      windowOpMaps += nodeself
      f match {
        case Def(fun @ Lambda(_, _, _)) => hashJoinAntiForeachLambda += nodeself -> fun.asInstanceOf[Lambda[Any, Unit]]
        case _                          => ()
      }
      ()
  }

  analysis += rule {
    case OptionGet(Def(MultiMapGet(mm, elem))) if allMaps.contains(mm) =>
      // At this phase it's potentially anti hash join multimap, it's not specified for sure
      hashJoinAntiMaps += mm
      ()
  }

  var currentLoopSymbol: While = _

  analysis += statement {
    case sym -> (node @ While(_, _)) =>
      currentLoopSymbol = node
  }

  def numBuckets(partitionedObject: PartitionObject): Rep[Int] =
    (partitionedObject.tpe.name, partitionedObject.fieldFunc) match {
      case ("LINEITEMRecord", "L_ORDERKEY") => partitionedObject.arraySize
      case ("LINEITEMRecord", "L_SUPPKEY") => unit((10000 * scalingFactor).toInt)
      case ("LINEITEMRecord", "L_PARTKEY") => unit((250000 * scalingFactor).toInt)
      case ("CUSTOMERRecord", "C_NATIONKEY") | ("SUPPLIERRecord", "S_NATIONKEY") => unit(25)
      case _ => partitionedObject.arraySize / unit(4)
    }
  def numBucketsFull(partitionedObject: PartitionObject): Rep[Int] = partitionedObject.arr match {
    case Def(ArrayNew(l)) => partitionedObject.tpe.name match {
      case "ORDERSRecord" => l * unit(5)
      case _              => l
    }
    case sym => throw new Exception(s"setting default value for $sym")
  }
  def bucketSize(partitionedObject: PartitionObject): Rep[Int] = //unit(100)
    // numBuckets(partitionedObject)
    (partitionedObject.tpe.name, partitionedObject.fieldFunc) match {
      case ("LINEITEMRecord", "L_ORDERKEY") => unit(16)
      // case ("LINEITEMRecord", "L_SUPPKEY") => unit(1 << 10)
      case ("CUSTOMERRecord", "C_NATIONKEY") | ("SUPPLIERRecord", "S_NATIONKEY") => partitionedObject.arraySize / unit(25 - 5)
      case _ => unit(1 << 10)
    }

  // def numBuckets(partitionedObject: PartitionObject): Rep[Int] = unit(1 << 9)
  // def bucketSize(partitionedObject: PartitionObject): Rep[Int] = partitionedObject.arr match {
  //   case Def(ArrayNew(l)) => l / unit(4)
  //   case sym              => System.out.println(s"setting default value for $sym"); numBuckets(sym.tp.typeArguments(0))
  // }
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
      val bucket = partitionedObject.tpe.name match {
        case "CUSTOMERRecord" | "PARTRecord" | "SUPPLIERRecord" => key - unit(1)
        case _ => key
      }
      val e = parArr(bucket)
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
    System.out.println(scala.Console.RED + partitionedObject.arr.tp.typeArguments(0) + " Partitioned on field " + partitionedObject.fieldFunc + scala.Console.RESET)

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
      System.out.println(s"${scala.Console.BLUE}1D Array!!!${scala.Console.RESET}")
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
    case sym -> (node @ ArrayNew(size)) if !seenArrays.contains(sym) =>
      seenArrays += sym
      reflectStm(Stm(sym, node))
      sym
  }

  rewrite += statement {
    case sym -> (node @ MultiMapNew()) if shouldBePartitioned(sym) && !windowOpMaps.contains(sym) => {
      val hmParObj = getPartitionedObject(sym)(sym.tp)

      createPartitionArray(hmParObj.partitionedObject)

      sym
    }
  }

  rewrite += removeStatement {
    case sym -> (node @ MultiMapNew()) if shouldBePartitioned(sym) && windowOpMaps.contains(sym) =>
      ()
  }

  rewrite += remove {
    case MultiMapGet(mm, elem) if shouldBePartitioned(mm) => {
      ()
    }
  }

  // The case for HashJoinAnti
  rewrite += rule {
    case MultiMapAddBinding(mm, elem, nodev) if shouldBePartitioned(mm) && getPartitionedObject(mm).isAnti =>
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
    case MultiMapAddBinding(mm, _, nodev) if shouldBePartitioned(mm) && getPartitionedObject(mm).hasLeft && fillingHole.get(mm).isEmpty =>
      ()
  }

  rewrite += rule {
    case MultiMapAddBinding(mm, _, nodev) if shouldBePartitioned(mm) && getPartitionedObject(mm).hasLeft && fillingHole.get(mm).nonEmpty =>
      fillingFunction(mm)()
  }

  // The case for WindowOp
  rewrite += rule {
    case MultiMapAddBinding(mm, nodekey, nodev) if shouldBePartitioned(mm) && getPartitionedObject(mm).isWindow =>
      class ElemType
      implicit val elemType = nodev.tp.asInstanceOf[TypeRep[ElemType]]
      val key = nodekey.asInstanceOf[Rep[Int]]
      val value = nodev.asInstanceOf[Rep[ElemType]]
      windowOpFoldLefts(mm).toList match {
        case List(WindowOpMetaInfo(arr, node)) => {
          val tArr = apply(arr).asInstanceOf[Rep[Array[Double]]]
          val oldValue = tArr(key)
          val newValue = inlineFunction(node.op.asInstanceOf[Rep[(Double, ElemType) => Double]], oldValue, value)
          tArr(key) = newValue
        }
        case _ =>
      }
      windowOpMins(mm).toList match {
        case List(WindowOpMetaInfo(arr, node)) => {
          val fun = node.f.asInstanceOf[Rep[ElemType => Double]]
          val newValue = inlineFunction(fun, value)
          val tArr = arr.asInstanceOf[Rep[Array[ElemType]]]
          val elem = tArr(key)
          __ifThenElse[Unit]((elem __== unit(null)) || (newValue < inlineFunction(fun, elem)), {
            tArr(key) = value
          }, unit())
          ()
        }
        case _ =>
      }
      unit()
  }

  rewrite += rule {
    case MultiMapAddBinding(mm, elem, nodev) if shouldBePartitioned(mm) && !getPartitionedObject(mm).isWindow && !getPartitionedObject(mm).hasLeft =>
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
    case ArrayApply(arr, _) if partitionedHashMapObjects.exists(obj => shouldBePartitioned(obj.mapSymbol) && !obj.isWindow && obj.partitionedObject.arr == arr && fillingHole.get(obj.mapSymbol).nonEmpty) =>
      val allObjs = partitionedHashMapObjects.filter(obj => shouldBePartitioned(obj.mapSymbol) && !obj.isWindow && obj.partitionedObject.arr == arr && fillingHole.get(obj.mapSymbol).nonEmpty)
      val sortedObjs = allObjs.toList.sortBy(obj => fillingHole(obj.mapSymbol))
      fillingElem(sortedObjs.last.mapSymbol)
  }

  rewrite += remove {
    case node @ While(_, _) if partitionedHashMapObjects.exists(obj => shouldBePartitioned(obj.mapSymbol) && !obj.isWindow && obj.partitionedObject.loopSymbol == node) =>
      ()
  }

  rewrite += rule {
    case OptionNonEmpty(Def(MultiMapGet(mm, elem))) if shouldBePartitioned(mm) =>
      unit(true)
  }

  rewrite += remove {
    case OptionGet(Def(MultiMapGet(mm, elem))) if shouldBePartitioned(mm) => ()
  }

  var loopDepth: Int = 0
  // associates each multimap with a level which specifies the depth of the corresponding for loop
  val fillingHole = scala.collection.mutable.Map[Rep[Any], Int]()
  var fillingFunction = scala.collection.mutable.Map[Rep[Any], () => Rep[Any]]()
  var fillingElem = scala.collection.mutable.Map[Rep[Any], Rep[Any]]()

  rewrite += rule {
    case SetForeach(Def(OptionGet(Def(MultiMapGet(mm, elem)))), f) if shouldBePartitioned(mm) && getPartitionedObject(mm).hasLeft => {
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
          __ifThenElse[Unit](field[Int](e, leftArray.fieldFunc) __== apply(elem), {
            leftOuterJoinExistsVarSet(mm)
            inlineFunction(f.asInstanceOf[Rep[InnerType => Unit]], e)
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
      leftOuterJoinDefaultHandling(mm, key, leftArray)
    }
  }

  rewrite += rule {
    case SetExists(Def(OptionGet(Def(MultiMapGet(mm, elem)))), p) if shouldBePartitioned(mm) && getPartitionedObject(mm).hasLeft => {
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
    case SetForeach(Def(OptionGet(Def(MultiMapGet(mm, elem)))), f) if shouldBePartitioned(mm) && !getPartitionedObject(mm).hasLeft && fillingHole.get(mm).isEmpty =>
      ()
  }

  rewrite += remove {
    case MultiMapForeach(mm, f) if shouldBePartitioned(mm) && getPartitionedObject(mm).isAnti =>
      ()
  }

  var windowOpForeachMode = false

  // The case for WindowOp
  rewrite += rule {
    case MultiMapForeach(mm, f) if shouldBePartitioned(mm) && getPartitionedObject(mm).isWindow =>
      class ElemType
      windowOpForeachMode = true
      windowOpFoldLefts(mm).toList match {
        case List(WindowOpMetaInfo(arr, node)) => {
          Range(unit(0), NUM_AGGS).foreach {
            __lambda { i =>
              implicit val elemType = arr.tp.typeArguments(0).asInstanceOf[TypeRep[ElemType]]
              val value = arr.asInstanceOf[Rep[Array[Double]]](i)
              val fun = f.asInstanceOf[Rep[((Double, Set[Any])) => Unit]]
              val res = __ifThenElse[Unit](value __!= unit(0.0), {
                inlineFunction(fun, Tuple2(value, i.asInstanceOf[Rep[Set[Any]]]))
              }, unit())
              res
            }
          }
          ()
        }
        case _ =>
      }

      windowOpMins(mm).toList match {
        case List(WindowOpMetaInfo(arr: Rep[Array[ElemType]], node: SetMinBy[Any, Double])) => {
          Range(unit(0), NUM_AGGS).foreach {
            __lambda { i =>
              implicit val elemType = arr.tp.typeArguments(0).asInstanceOf[TypeRep[ElemType]]
              val elem = arr.asInstanceOf[Rep[Array[ElemType]]](i)
              System.out.println(s"elem tp: ${elem}:${elem.tp}!${elemType}")
              val fun = f.asInstanceOf[Rep[((ElemType, Set[Any])) => Unit]]
              val res = __ifThenElse[Unit](elem __!= unit(null), {
                inlineFunction(fun, Tuple2(elem, i.asInstanceOf[Rep[Set[Any]]]))
              }, unit())
              res
            }
          }
          ()
        }
        case _ =>
      }
      windowOpForeachMode = false
      unit()
  }

  rewrite += rule {
    case SetMinBy(TDef(Tuple2_Field__2(tup @ TDef(Tuple2ApplyObject(_1, _2)))), _) if windowOpForeachMode => {
      _1
    }
  }

  rewrite += rule {
    case SetFoldLeft(TDef(Tuple2_Field__2(tup @ TDef(Tuple2ApplyObject(_1, _2)))), _, _) if windowOpForeachMode => {
      _1
    }
  }

  rewrite += rule {
    case SetHead(TDef(Tuple2_Field__2(tup @ TDef(Tuple2ApplyObject(_1, _2))))) if windowOpForeachMode => {
      _2
    }
  }

  rewrite += rule {
    case StructImmutableField(s, _) if windowOpForeachMode => {
      s
    }
  }

  rewrite += rule {
    case IfThenElse(Def(OptionNonEmpty(Def(MultiMapGet(mm, elem)))), thenp, elsep) if shouldBePartitioned(mm) && getPartitionedObject(mm).isAnti && fillingHole.get(mm).nonEmpty =>
      class ElemType
      val retainPredicate = thenp.stmts.collect({ case Statement(sym, SetRetain(_, p)) => p }).head.asInstanceOf[Rep[ElemType => Boolean]]
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
    case SetForeach(Def(OptionGet(Def(MultiMapGet(mm, elem)))), f) if shouldBePartitioned(mm) && !getPartitionedObject(mm).hasLeft && fillingHole.get(mm).nonEmpty =>
      inlineFunction(f, fillingFunction(mm)())
  }

  /* The parts dedicated to left outer join handling */
  def leftOuterJoinDefaultHandling(mm: Rep[MultiMap[Any, Any]], key: Rep[Int], partitionedObject: PartitionObject): Rep[Unit] = queryNumber match {
    case 13 =>
      __ifThenElse(!readVar(leftOuterJoinExistsVar(mm)), {
        inlineBlock[Unit](leftOuterJoinDefault(mm))
      }, unit(()))
    // printf(unit("query 13!"))
    case _ => unit(())
  }

  def leftOuterJoinExistsVarDefine(mm: Rep[MultiMap[Any, Any]]): Unit = queryNumber match {
    case 13 =>
      val exists = __newVarNamed[Boolean](unit(false), "exists")
      leftOuterJoinExistsVar(mm) = exists
      ()
    case _ =>
  }

  def leftOuterJoinExistsVarSet(mm: Rep[MultiMap[Any, Any]]): Unit = queryNumber match {
    case 13 =>
      val exists = leftOuterJoinExistsVar(mm)
      __assign(exists, unit(true))
      ()
    case _ =>
  }

  val leftOuterJoinDefault = scala.collection.mutable.Map[Rep[Any], Block[Unit]]()
  val leftOuterJoinExistsVar = scala.collection.mutable.Map[Rep[Any], Var[Boolean]]()

  analysis += rule {
    case IfThenElse(Def(OptionNonEmpty(Def(MultiMapGet(mm, elem)))), thenp, elsep) if queryNumber == 13 => {
      // System.out.println(s"elsep: $elsep")
      leftOuterJoinDefault += mm -> elsep.asInstanceOf[Block[Unit]]
      ()
    }
  }
}
