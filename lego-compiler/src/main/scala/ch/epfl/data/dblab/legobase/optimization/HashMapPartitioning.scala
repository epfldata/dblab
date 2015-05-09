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

/**
 * A transformer for partitioning and indexing the MultiMaps. As a result, this transformation
 * converts a MultiMap and the corresponding operations into an Array (either one dimensional or
 * two dimensional).
 *
 * TODO maybe add an example
 *
 * @param IR the polymorphic embedding trait which contains the reified program.
 * @param queryNumber specifies the TPCH query number (TODO should be removed)
 * @param scalingFactor specifies the scaling factor used for TPCH queries (TODO should be removed)
 */
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
  val setForeachLambda = scala.collection.mutable.Map[Rep[Any], Rep[Any => Unit]]()

  val windowOpMaps = scala.collection.mutable.Set[Rep[Any]]()

  // val SIZE_ORDER = List("REGIONRecord", "NATIONRecord", "SUPPLIERRecord", "CUSTOMERRecord", "PARTRecord", "PARTSUPPRecord", "ORDERSRecord", "LINEITEMRecord")

  // def getSizeOrder[T](tp: TypeRep[T]): Int = {
  //   val name = tp.name
  //   SIZE_ORDER.zipWithIndex.find(x => x._1 == name).get._2
  // }

  val ONE_D_ENABLED = true

  val QUERY_18_DUMMY_FIELD = "DUM"

  def isPrimaryKey[T](tp: TypeRep[T], field: String): Boolean = (tp.name, field) match {
    // case ("REGIONRecord", "R_REGIONKEY") => true
    // case ("NATIONRecord", "N_NATIONKEY") => true
    // case ("SUPPLIERRecord", "S_SUPPKEY") => true
    // case ("CUSTOMERRecord", "C_CUSTKEY") => true
    // case ("PARTRecord", "P_PARTKEY") => true
    // // case ("PARTSUPPRecord", _) => false
    // case ("ORDERSRecord", "O_ORDERKEY") => true
    // // case ("LINEITEMRecord", "L_ORDERKEY") => true
    case (tableName, _) if getTable(tableName).exists(table => table.primaryKey.exists(pk => pk.attributes.forall(att => att.name == field))) => true
    case ("Double", QUERY_18_DUMMY_FIELD) if queryNumber == 18 => true
    case _ => false
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
  }
  case class PartitionObject(arr: Rep[Array[Any]], fieldFunc: String, loopSymbol: While) {
    def tpe = arr.tp.typeArguments(0).asInstanceOf[TypeRep[Any]]
    def buckets = if (is1D) numBucketsFull(this) else numBuckets(this)
    def count = partitionedObjectsCount(this)
    def parArr = partitionedObjectsArray(this).asInstanceOf[Rep[Array[Array[Any]]]]
    def is1D: Boolean = if (ONE_D_ENABLED) isPrimaryKey(tpe, fieldFunc) else false
    def reuseOriginal1DArray: Boolean = List("REGIONRecord", "NATIONRecord", "SUPPLIERRecord", "CUSTOMERRecord", "PARTRecord").contains(tpe.name) || (queryNumber == 18 && tpe == DoubleType)
    def arraySize: Rep[Int] = arr match {
      case Def(ArrayNew(s)) => s
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
    traverseBlock(node)
    val realHashJoinAntiMaps = hashJoinAntiMaps intersect windowOpMaps
    windowOpMaps --= realHashJoinAntiMaps // TODO should be uncommented
    hashJoinAntiMaps.clear()
    hashJoinAntiMaps ++= realHashJoinAntiMaps
    // val supportedMaps = partitionedMaps diff windowOpMaps
    partitionedHashMapObjects ++= partitionedMaps.map({ hm =>
      val left = leftPartArr.get(hm).map(v => PartitionObject(v, leftPartFunc(hm), leftLoopSymbol(hm)))
      val right = rightPartArr.get(hm).map(v => PartitionObject(v, rightPartFunc(hm), rightLoopSymbol(hm)))
      HashMapPartitionObject(hm, left, right)
    })
    val res =
      transformProgram(node)
    // System.out.println(s"[${scala.Console.BLUE}DEBUG${scala.Console.RESET}]${allTables.map(t => t.name -> t.primaryKey).mkString("\n")}")
    // System.out.println(s"[${scala.Console.BLUE}DEBUG${scala.Console.RESET}]$allTables")
    System.out.println(s"[${scala.Console.BLUE}$transformedMapsCount${scala.Console.RESET}] MultiMaps partitioned!")
    res
  }

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
          // case Def(node)                 => System.out.println(s"leftPartArr (ArrayApply) couldn't be found for $nodeself, instead $node found")
          case _                         =>
        }
      addPartArray(struct)
  }

  val leftPartKey = scala.collection.mutable.Map[Rep[Any], Var[Any]]()

  analysis += rule {
    case node @ MultiMapAddBinding(nodeself, nodekey, nodev) if allMaps.contains(nodeself) && queryNumber == 18 =>
      partitionedMaps += nodeself
      leftPartFunc += nodeself -> QUERY_18_DUMMY_FIELD
      leftLoopSymbol += nodeself -> currentLoopSymbol
      globalDefs.values.collect({
        case Stm(_, ArrayApply(arr, ind)) if ind == nodekey => arr
      }).headOption.foreach(arr => {
        // System.out.println(s"addbinding 18: ${arr}, $nodekey")
        nodekey match {
          case Def(ReadVar(v)) => leftPartKey += nodeself -> v
          case _               => ()
        }
        leftPartArr += nodeself -> arr
      })

    // System.out.println(s"addbinding 18: ${nodekey.correspondingNode}")
    // def addPartArray(exp: Rep[Any]): Unit =
    //   exp match {
    //     case Def(ArrayApply(arr, ind)) =>
    //       System.out.println(s"leftPartArr (ArrayApply) FOUND for $nodeself, instead $node found"); leftPartArr += nodeself -> arr
    //     // case Def(Struct(_, elems, _)) if elems.exists(e => e.name == field) =>
    //     //   val init = elems.find(_.name == field).get.init
    //     //   init match {
    //     //     case Def(StructImmutableField(struct, field)) =>
    //     //       System.out.println(s"${scala.Console.RED}WINDOWOP ${struct.tp}${scala.Console.RESET}")
    //     //       addPartArray(struct)
    //     //     case _ =>
    //     //   }
    //     // case Def(node)                 => System.out.println(s"leftPartArr (ArrayApply) couldn't be found for $nodeself, instead $node found")
    //     case _ =>
    //   }
    // addPartArray(nodekey)
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

  analysis += rule {
    case SetForeach(Def(OptionGet(Def(MultiMapGet(mm, elem)))), f) => {
      setForeachLambda(mm) = f.asInstanceOf[Rep[Any => Unit]]
    }
  }

  // TODO use Schema instead of manual cases for TPCH
  def numBuckets(partitionedObject: PartitionObject): Rep[Int] =
    (partitionedObject.tpe.name, partitionedObject.fieldFunc) match {
      case ("LINEITEMRecord", "L_ORDERKEY") => partitionedObject.arraySize
      case ("LINEITEMRecord", "L_SUPPKEY") => unit((10000 * scalingFactor).toInt)
      case ("LINEITEMRecord", "L_PARTKEY") => unit((250000 * scalingFactor).toInt)
      case ("CUSTOMERRecord", "C_NATIONKEY") | ("SUPPLIERRecord", "S_NATIONKEY") => unit(25)
      case _ => partitionedObject.arraySize / unit(4)
    }

  // TODO use Schema instead of manual cases for TPCH
  def numBucketsFull(partitionedObject: PartitionObject): Rep[Int] = partitionedObject.arr match {
    case Def(ArrayNew(l)) => partitionedObject.tpe.name match {
      case "ORDERSRecord" => l * unit(5)
      case _              => l
    }
    case sym => throw new Exception(s"setting default value for $sym")
  }

  // TODO use Schema instead of manual cases for TPCH
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

  // TODO use Schema instead of manual cases for TPCH
  def par_array_foreach[T: TypeRep](partitionedObject: PartitionObject, key: Rep[Int], f: Rep[T] => Rep[Unit]): Rep[Unit] = {
    if (partitionedObject.is1D) {
      val parArr = partitionedObject.parArr.asInstanceOf[Rep[Array[T]]]
      val bucket = partitionedObject.tpe.name match {
        case "CUSTOMERRecord" | "PARTRecord" | "SUPPLIERRecord" => key - unit(1)
        case _ => key
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
    System.out.println(scala.Console.RED + partitionedObject.arr.tp.typeArguments(0) + " Partitioned on field " + partitionedObject.fieldFunc + scala.Console.RESET)

    class InnerType
    implicit val typeInner = partitionedObject.arr.tp.typeArguments(0).asInstanceOf[TypeRep[InnerType]]
    val originalArray = {
      val origArray =
        if (!seenArrays.contains(partitionedObject.arr) && queryNumber != 18) {
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
    case sym -> (node @ ArrayNew(size)) /*if !seenArrays.contains(sym)*/ =>
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

  rewrite += rule {
    case MultiMapAddBinding(mm, elem, nodev) if shouldBePartitioned(mm) && !getPartitionedObject(mm).hasLeft =>
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
    case ArrayApply(arr, _) if partitionedHashMapObjects.exists(obj => shouldBePartitioned(obj.mapSymbol) && /*!obj.isWindow &&*/ obj.partitionedObject.arr == arr && fillingHole.get(obj.mapSymbol).nonEmpty) =>
      val allObjs = partitionedHashMapObjects.filter(obj => shouldBePartitioned(obj.mapSymbol) && /*!obj.isWindow &&*/ obj.partitionedObject.arr == arr && fillingHole.get(obj.mapSymbol).nonEmpty)
      val sortedObjs = allObjs.toList.sortBy(obj => fillingHole(obj.mapSymbol))
      val hmPartitionedObject = sortedObjs.last
      // System.out.println(s"filling array apply hole with ${fillingElem(hmPartitionedObject.mapSymbol)}: ${hmPartitionedObject.partitionedObject.tpe}, all: ${partitionedHashMapObjects.map(x => x.mapSymbol -> fillingHole.get(x.mapSymbol) -> x.partitionedObject.tpe).mkString("\n")}")
      fillingElem(hmPartitionedObject.mapSymbol)
  }

  rewrite += remove {
    case node @ While(_, _) if partitionedHashMapObjects.exists(obj => shouldBePartitioned(obj.mapSymbol) && /*!obj.isWindow &&*/ obj.partitionedObject.loopSymbol == node) =>
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

  rewrite += removeStatement {
    case sym -> Lambda(_, _, _) if setForeachLambda.exists(_._2 == sym) =>
      ()
  }

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
          def ifThenBody = {
            leftOuterJoinExistsVarSet(mm)
            inlineFunction(f.asInstanceOf[Rep[InnerType => Unit]], e)
          }
          if (queryNumber == 18 && leftArray.fieldFunc == QUERY_18_DUMMY_FIELD) {
            ifThenBody
          } else {
            __ifThenElse[Unit](field[Int](e, leftArray.fieldFunc) __== apply(elem), {
              ifThenBody
            }, {
              unit(())
            })
          }
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

  rewrite += rule {
    case Equal(a, Def(ReadVar(v))) if queryNumber == 18 && leftPartKey.values.exists(_ == v) => unit(true)
  }
}
