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

class HashMapPartitioningTransformer(override val IR: LoweringLegoBase, val queryNumber: Int) extends RuleBasedTransformer[LoweringLegoBase](IR) with StructCollector[LoweringLegoBase] {
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
    def buckets = numBuckets(this)
    def count = partitionedObjectsCount(this)
    def parArr = partitionedObjectsArray(this).asInstanceOf[Rep[Array[Array[Any]]]]
  }
  val partitionedHashMapObjects = scala.collection.mutable.Set[HashMapPartitionObject]()
  val partitionedObjectsArray = scala.collection.mutable.Map[PartitionObject, Rep[Array[Any]]]()
  val partitionedObjectsCount = scala.collection.mutable.Map[PartitionObject, Rep[Array[Int]]]()

  def supportsWindowOp(): Boolean = (queryNumber == 2) || (queryNumber == 11)

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
    // phase = FindLoopLength
    // traverseBlock(node)
    // phase = FindColumnArray
    // traverseBlock(node)
    // // System.out.println(s"columnStorePartitionedSymbols: $columnStorePartitionedSymbols, partitionedArrayLengthSymbols: $partitionedArrayLengthSymbols")
    // phase = FindPartitionedLoop
    traverseBlock(node)
    // System.out.println(s"leftPartFunc: $leftPartFunc")
    // System.out.println(s"rightPartFunc: $rightPartFunc")
    // System.out.println(s"leftPartArr: $leftPartArr")
    // System.out.println(s"rightPartArr: $rightPartArr")
    val realHashJoinAntiMaps = hashJoinAntiMaps intersect windowOpMaps
    // System.out.println(s"hashJoinAntiMaps: $hashJoinAntiMaps")
    windowOpMaps --= realHashJoinAntiMaps // TODO should be uncommented
    hashJoinAntiMaps.clear()
    hashJoinAntiMaps ++= realHashJoinAntiMaps
    val supportedMaps = partitionedMaps diff windowOpMaps
    partitionedHashMapObjects ++= partitionedMaps.map({ hm =>
      val left = leftPartArr.get(hm).map(v => PartitionObject(v, leftPartFunc(hm), leftLoopSymbol(hm)))
      val right = rightPartArr.get(hm).map(v => PartitionObject(v, rightPartFunc(hm), rightLoopSymbol(hm)))
      HashMapPartitionObject(hm, left, right)
    })
    // System.out.println(s"partitionedHashMapObjects: $partitionedHashMapObjects")
    // System.out.println(s"windowOpMaps: $windowOpMaps")
    // System.out.println(s"supportedMaps: $supportedMaps")
    System.out.println(s"${scala.Console.RED}hashJoinAntiMaps: $hashJoinAntiMaps${scala.Console.BLACK}")
    // System.out.println(s"${scala.Console.RED}hashJoinAntiMaps: ${hashJoinAntiMaps.map(x => getPartitionedObject(x).antiLambda)}${scala.Console.BLACK}")
    System.out.println(s"partitionedHashMapObjects: ${partitionedHashMapObjects.map(x => x.mapSymbol -> shouldBePartitioned(x.mapSymbol))}")

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
    System.out.println(s"[${scala.Console.BLUE}$transformedMapsCount${scala.Console.BLACK}] MultiMaps partitioned!")
    res
  }

  def analyseWindowLambda(windowOpMap: HashMapPartitionObject) {
    // System.out.println(s"lambda: ${windowOpMap.antiLambda}")
    val mm = windowOpMap.mapSymbol
    val foldLefts = windowOpMap.antiLambda.body.stmts.collect({
      case Statement(sym, s @ SetFoldLeft(_, _, _)) => sym.asInstanceOf[Rep[Any]] -> s
    })
    val mins = windowOpMap.antiLambda.body.stmts.collect({
      case Statement(sym, s @ SetMinBy(_, _)) => sym.asInstanceOf[Rep[Any]] -> s
    })
    // System.out.println(s"foldLefts: ${foldLefts}")
    // System.out.println(s"mins: ${mins}")
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
          //       System.out.println(s"${scala.Console.RED}WINDOWOP ${struct.tp}${scala.Console.BLACK}")
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

  def numBuckets(partitionedObject: PartitionObject): Rep[Int] = partitionedObject.arr match {
    case Def(ArrayNew(l)) => l / unit(4)
    case sym              => throw new Exception(s"setting default value for $sym")
  }
  def bucketSize(partitionedObject: PartitionObject): Rep[Int] = //unit(100)
    // numBuckets(partitionedObject)
    unit(1 << 10)

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

  def createPartitionArray(partitionedObject: PartitionObject): Unit = {
    class InnerType
    implicit val typeInner = partitionedObject.arr.tp.typeArguments(0).asInstanceOf[TypeRep[InnerType]]
    val buckets = numBuckets(partitionedObject)
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

  val seenArrays = scala.collection.mutable.Set[Rep[Any]]()

  // rewrite += removeStatement {
  //   case sym -> ArrayNew(size) if seenArrays.contains(sym) =>
  //     ()
  // }

  rewrite += statement {
    case sym -> (node @ ArrayNew(size)) if !seenArrays.contains(sym) =>
      seenArrays += sym
      reflectStm(Stm(sym, node))
      sym
  }

  rewrite += statement {
    case sym -> (node @ MultiMapNew()) if shouldBePartitioned(sym) && !windowOpMaps.contains(sym) => {
      // if (shouldBePartitioned(sym)) {
      val hmParObj = getPartitionedObject(sym)(sym.tp)
      // hmParObj.left.foreach { l =>
      //   createPartitionArray(l)
      // }
      // hmParObj.right.foreach { r =>
      //   createPartitionArray(r)
      // }
      System.out.println(hmParObj.partitionedObject.arr.tp.typeArguments(0) + " Partitioned")

      createPartitionArray(hmParObj.partitionedObject)

      sym
      // } else if (windowOpMaps.contains(sym)) {
      //   val hmParObj = getPartitionedObject(sym)(sym.tp)
      //   // hmParObj.left.foreach { l =>
      //   //   createPartitionArray(l)
      //   // }
      //   // hmParObj.right.foreach { r =>
      //   //   createPartitionArray(r)
      //   // }
      //   createPartitionArray(hmParObj.partitionedObject)
      //   recreateNode(node)
      // } else {
      //   recreateNode(node)
      // }
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

  // The case for HashJoinAnit
  rewrite += rule {
    case MultiMapAddBinding(mm, elem, nodev) if shouldBePartitioned(mm) && getPartitionedObject(mm).isAnti =>
      class ElemType
      implicit val elemType = nodev.tp.asInstanceOf[TypeRep[ElemType]]
      val value = apply(nodev).asInstanceOf[Rep[ElemType]]
      val hmParObj = getPartitionedObject(mm)
      val rightArray = hmParObj.partitionedObject
      val key = apply(elem).asInstanceOf[Rep[Int]] % rightArray.buckets
      val antiLambda = hmParObj.antiLambda
      val foreachFunction = antiLambda.body.stmts.collect({ case Statement(sym, SetForeach(_, f)) => f }).head.asInstanceOf[Rep[ElemType => Unit]]
      val resultRetain = __newVarNamed[Boolean](unit(false), "resultRetain")
      hashJoinAntiRetainVar += mm -> resultRetain
      val count = rightArray.count(key)
      class ElemType2
      implicit val elemType2 = rightArray.arr.tp.typeArguments(0).asInstanceOf[TypeRep[ElemType2]]
      val parArrWhole = rightArray.parArr.asInstanceOf[Rep[Array[Array[ElemType2]]]]
      val parArr = parArrWhole(key)
      Range(unit(0), count).foreach {
        __lambda { i =>
          val e = parArr(i)
          System.out.println(s"par arr elem for anti $e")
          fillingElem(mm) = e
          fillingFunction(mm) = () => apply(nodev)
          fillingHole(mm) = loopDepth
          loopDepth += 1
          val res = inlineBlock2(rightArray.loopSymbol.body)
          fillingHole.remove(mm)
          loopDepth -= 1
          res
        }
      }
      System.out.println(s"addb: ${foreachFunction.correspondingNode} with $value")
      transformedMapsCount += 1
      __ifThenElse(!readVar(resultRetain), {
        inlineFunction(foreachFunction, value)
      }, unit())
    // System.out.println(s"addb: ${foreachFunction.correspondingNode}")
    // printf(unit("TODO"))
  }

  rewrite += remove {
    case MultiMapAddBinding(mm, _, nodev) if shouldBePartitioned(mm) && getPartitionedObject(mm).hasLeft && fillingHole.get(mm).isEmpty =>
      ()
  }

  rewrite += rule {
    case MultiMapAddBinding(mm, _, nodev) if shouldBePartitioned(mm) && getPartitionedObject(mm).hasLeft && fillingHole.get(mm).nonEmpty =>
      // System.out.println(s"came here! for $mm")
      fillingFunction(mm)()
  }

  rewrite += rule {
    case MultiMapAddBinding(mm, nodekey, nodev) if shouldBePartitioned(mm) && getPartitionedObject(mm).isWindow =>
      class ElemType
      implicit val elemType = nodev.tp.asInstanceOf[TypeRep[ElemType]]
      val key = nodekey.asInstanceOf[Rep[Int]]
      val value = nodev.asInstanceOf[Rep[ElemType]]
      windowOpFoldLefts(mm).toList match {
        case List(WindowOpMetaInfo(arr, node)) => {
          val tArr = arr.asInstanceOf[Rep[Array[Double]]]
          val oldValue = tArr(key)
          tArr(key) = inlineFunction(node.op.asInstanceOf[Rep[(Double, ElemType) => Double]], oldValue, value)

        }
        case _ =>
      }
      // System.out.println(s"here for window op addBinding with mins ${windowOpMins(mm)}")
      windowOpMins(mm).toList match {
        case List(WindowOpMetaInfo(arr: Rep[Array[ElemType]], node: SetMinBy[ElemType, Double])) => {

          val newValue = inlineFunction(node.f, value)
          val elem = arr.asInstanceOf[Rep[Array[ElemType]]](key)
          __ifThenElse((elem __== unit(null)) || (newValue < inlineFunction(node.f, elem)), {
            arr(key) = value
          }, unit())
          // printf(unit("%s"), newValue)
          // System.out.println(s"inlined function with $newValue")
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
      val key = apply(elem).asInstanceOf[Rep[Int]] % leftArray.buckets
      val whileLoop = leftArray.loopSymbol
      // System.out.println(s"loop: ${leftArray.loopSymbol}")

      val res = Range(unit(0), leftArray.count(key)).foreach {
        __lambda { i =>
          class InnerType
          implicit val typeInner = leftArray.parArr.tp.typeArguments(0).typeArguments(0).asInstanceOf[TypeRep[InnerType]]
          val leftParArray = leftArray.parArr.asInstanceOf[Rep[Array[Array[InnerType]]]]
          val e = leftParArray(key)(i)
          fillingElem(mm) = e
          fillingFunction(mm) = () => apply(nodev)
          // fillingFunction(mm) = () => {
          //   __ifThenElse(field[Int](e, leftArray.fieldFunc) __== elem, {
          //     inlineFunction(f, e)
          //   }, {
          //     unit(())
          //   })
          // }
          fillingHole(mm) = loopDepth
          loopDepth += 1
          // System.out.println(s"inlining block ${whileLoop.body}")
          val res1 = inlineBlock2(whileLoop.body)
          // System.out.println(s"inlining block done")
          // System.out.println(s"fillingHole $fillingHole")
          fillingHole.remove(mm)
          loopDepth -= 1
          transformedMapsCount += 1
          // System.out.println(s"fillingHole $fillingHole")
          res1
        }
      }
      // System.out.println(s"foreach done $res")

      res
  }

  rewrite += rule {
    case ArrayApply(arr, _) if partitionedHashMapObjects.exists(obj => shouldBePartitioned(obj.mapSymbol) && !obj.isWindow && obj.partitionedObject.arr == arr && fillingHole.get(obj.mapSymbol).nonEmpty) // {
    //   val res = partitionedHashMapObjects.find(obj => shouldBePartitioned(obj.mapSymbol) /* && obj.hasLeft */ && obj.partitionedObject.arr == arr && fillingHole.get(obj.mapSymbol).nonEmpty) match {
    //     case Some(obj) => true
    //     case _         => false
    //   }
    //   if (arr.asInstanceOf[Sym[Any]].id == 3) {
    //     System.out.println(s"$arr is here with res $res, $fillingHole, my map ${partitionedHashMapObjects.find(obj => shouldBePartitioned(obj.mapSymbol) /* && obj.hasLeft */ && obj.partitionedObject.arr == arr).get.mapSymbol}")
    //   }
    //   res
    // }
    =>
      val allObjs = partitionedHashMapObjects.filter(obj => shouldBePartitioned(obj.mapSymbol) && !obj.isWindow && obj.partitionedObject.arr == arr && fillingHole.get(obj.mapSymbol).nonEmpty)
      val sortedObjs = allObjs.toList.sortBy(obj => fillingHole(obj.mapSymbol))
      fillingElem(sortedObjs.last.mapSymbol)
  }

  rewrite += remove {
    case node @ While(_, _) if {
      partitionedHashMapObjects.find(obj => shouldBePartitioned(obj.mapSymbol) && !obj.isWindow && obj.partitionedObject.loopSymbol == node) match {
        case Some(obj) => true
        case _         => false
      }
    } =>
      ()
  }

  rewrite += rule {
    case OptionNonEmpty(Def(MultiMapGet(mm, elem))) if shouldBePartitioned(mm) => {
      unit(true)
    }
  }

  rewrite += remove {
    case OptionGet(Def(MultiMapGet(mm, elem))) if shouldBePartitioned(mm) => {
      ()
    }
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
      val key = apply(elem).asInstanceOf[Rep[Int]] % leftArray.buckets
      val whileLoop = leftArray.loopSymbol
      // System.out.println(s"loop: ${leftArray.loopSymbol}")

      val res = Range(unit(0), leftArray.count(key)).foreach {
        __lambda { i =>
          class InnerType
          implicit val typeInner = leftArray.parArr.tp.typeArguments(0).typeArguments(0).asInstanceOf[TypeRep[InnerType]]
          val leftParArray = leftArray.parArr.asInstanceOf[Rep[Array[Array[InnerType]]]]
          val e = leftParArray(key)(i)
          fillingElem(mm) = e
          fillingFunction(mm) = () => {
            __ifThenElse[Unit](field[Int](e, leftArray.fieldFunc) __== apply(elem), {
              inlineFunction(f.asInstanceOf[Rep[InnerType => Unit]], e)
            }, {
              unit(())
            })
          }
          fillingHole(mm) = loopDepth
          loopDepth += 1
          // System.out.println(s"inlining block ${whileLoop.body}")
          val res1 = inlineBlock2(whileLoop.body)
          // System.out.println(s"inlining block done")
          // System.out.println(s"fillingHole $fillingHole")
          fillingHole.remove(mm)
          loopDepth -= 1
          transformedMapsCount += 1
          // System.out.println(s"fillingHole $fillingHole")
          res1
        }
      }
      // System.out.println(s"foreach done $res")

      res
    }
  }

  rewrite += rule {
    case SetExists(Def(OptionGet(Def(MultiMapGet(mm, elem)))), p) if shouldBePartitioned(mm) && getPartitionedObject(mm).hasLeft => {
      val hmParObj = getPartitionedObject(mm)
      val leftArray = hmParObj.partitionedObject
      val key = apply(elem).asInstanceOf[Rep[Int]] % leftArray.buckets
      val whileLoop = leftArray.loopSymbol
      // System.out.println(s"loop: ${leftArray.loopSymbol}")

      val result = __newVarNamed[Boolean](unit(false), "existsResult")
      Range(unit(0), leftArray.count(key)).foreach {
        __lambda { i =>
          class InnerType
          implicit val typeInner = leftArray.parArr.tp.typeArguments(0).typeArguments(0).asInstanceOf[TypeRep[InnerType]]
          val leftParArray = leftArray.parArr.asInstanceOf[Rep[Array[Array[InnerType]]]]
          val e = leftParArray(key)(i)
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
          // System.out.println(s"inlining block ${whileLoop.body} for hashmap ${mm}")
          val res1 = inlineBlock2(whileLoop.body)
          // System.out.println(s"inlining block done")
          // System.out.println(s"fillingHole $fillingHole")
          fillingHole.remove(mm)
          loopDepth -= 1
          transformedMapsCount += 1
          // System.out.println(s"fillingHole $fillingHole")
          res1
        }
      }
      // System.out.println(s"foreach done $res")

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
              // System.out.println(s"elem tp: ${elem}:${elem.tp}!${elemType}")

              val res = __ifThenElse(value __!= unit(0.0), {
                inlineFunction(f, Tuple2(value, i.asInstanceOf[Rep[Set[Any]]]))
              }, unit())

              res
            }
          }
          // printf(unit("%s"), newValue)
          // System.out.println(s"inlined function with $newValue")
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

              val res = __ifThenElse(elem __!= unit(null), {
                inlineFunction(f, Tuple2(elem, i.asInstanceOf[Rep[Set[Any]]]))
              }, unit())

              res
            }
          }
          // printf(unit("%s"), newValue)
          // System.out.println(s"inlined function with $newValue")
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

  // rewrite += remove {
  //   case SetHead(TDef(Tuple2_Field__2(tup @ TDef(Tuple2ApplyObject(_1, _2)))), _) if windowOpForeachMode => {
  //     _1
  //   }
  // }

  rewrite += rule {
    case IfThenElse(Def(OptionNonEmpty(Def(MultiMapGet(mm, elem)))), thenp, elsep) if shouldBePartitioned(mm) && getPartitionedObject(mm).isAnti && fillingHole.get(mm).nonEmpty =>
      class ElemType
      val retainPredicate = thenp.stmts.collect({ case Statement(sym, SetRetain(_, p)) => p }).head.asInstanceOf[Rep[ElemType => Boolean]]
      // System.out.println(s"came here! for $mm")
      // val typedElem = (elem match {
      //   case Def(StructImmutableField(s, _)) => apply(s)
      // }).asInstanceOf[Rep[ElemType]]
      // val typedElem = fillingElem(mm).asInstanceOf[Rep[ElemType]]
      val typedElem = fillingFunction(mm)().asInstanceOf[Rep[ElemType]]
      implicit val elemType = typedElem.tp.asInstanceOf[TypeRep[ElemType]]
      val resultRetain = hashJoinAntiRetainVar(mm)
      System.out.println(s"typedElem: $typedElem, fun: ${retainPredicate.correspondingNode}")
      __ifThenElse(!inlineFunction(retainPredicate, typedElem), {
        __assign(resultRetain, unit(true))
      }, {
        unit()
      })
    // fillingFunction(mm)()
  }

  rewrite += rule {
    case SetForeach(Def(OptionGet(Def(MultiMapGet(mm, elem)))), f) if shouldBePartitioned(mm) && !getPartitionedObject(mm).hasLeft && fillingHole.get(mm).nonEmpty =>
      // System.out.println(s"came here! $mm, ${fillingFunction(mm)()}")
      inlineFunction(f, fillingFunction(mm)())
  }
}
