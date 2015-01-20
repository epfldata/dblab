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

class HashMapPartitioningTransformer(override val IR: LoweringLegoBase) extends RuleBasedTransformer[LoweringLegoBase](IR) with StructCollector[LoweringLegoBase] {
  import IR._
  val allMaps = scala.collection.mutable.Set[Rep[Any]]()
  val partitionedMaps = scala.collection.mutable.Set[Rep[Any]]()
  val leftPartFunc = scala.collection.mutable.Map[Rep[Any], String]()
  val rightPartFunc = scala.collection.mutable.Map[Rep[Any], String]()
  val leftPartArr = scala.collection.mutable.Map[Rep[Any], Rep[Array[Any]]]()
  val rightPartArr = scala.collection.mutable.Map[Rep[Any], Rep[Array[Any]]]()
  case class HashMapPartitionObject(mapSymbol: Rep[Any], left: Option[PartitionObject], right: Option[PartitionObject])
  case class PartitionObject(arr: Rep[Array[Any]], fieldFunc: String) {
    def buckets = numBuckets(this)
    def count = partitionedObjectsCount(this)
    def parArr = partitionedObjectsArray(this).asInstanceOf[Rep[Array[Array[Any]]]]
  }
  val partitionedHashMapObjects = scala.collection.mutable.Set[HashMapPartitionObject]()
  val partitionedObjectsArray = scala.collection.mutable.Map[PartitionObject, Rep[Array[Any]]]()
  val partitionedObjectsCount = scala.collection.mutable.Map[PartitionObject, Rep[Array[Int]]]()

  def shouldBePartitioned[T: TypeRep](hm: Rep[T]): Boolean = partitionedMaps.exists(x => x == hm) && getPartitionedObject(hm).left.nonEmpty
  def getPartitionedObject[T: TypeRep](hm: Rep[T]): HashMapPartitionObject = partitionedHashMapObjects.find(x => x.mapSymbol == hm).get

  override def optimize[T: TypeRep](node: Block[T]): Block[T] = {
    // phase = FindLoopLength
    // traverseBlock(node)
    // phase = FindColumnArray
    // traverseBlock(node)
    // // System.out.println(s"columnStorePartitionedSymbols: $columnStorePartitionedSymbols, partitionedArrayLengthSymbols: $partitionedArrayLengthSymbols")
    // phase = FindPartitionedLoop
    traverseBlock(node)
    System.out.println(s"leftPartFunc: $leftPartFunc")
    System.out.println(s"rightPartFunc: $rightPartFunc")
    System.out.println(s"leftPartArr: $leftPartArr")
    System.out.println(s"rightPartArr: $rightPartArr")
    partitionedHashMapObjects ++= partitionedMaps.map({ hm =>
      val left = leftPartArr.get(hm).map(v => PartitionObject(v, leftPartFunc(hm)))
      val right = rightPartArr.get(hm).map(v => PartitionObject(v, rightPartFunc(hm)))
      HashMapPartitionObject(hm, left, right)
    })
    System.out.println(s"partitionedHashMapObjects: $partitionedHashMapObjects")
    System.out.println(s"partitionedMaps: $partitionedMaps")

    transformProgram(node)
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
      struct match {
        case Def(ArrayApply(arr, ind)) => leftPartArr += nodeself -> arr
        case _                         =>
      }
      ()
  }

  analysis += rule {
    case node @ MultiMapGet(nodeself, Def(StructImmutableField(struct, field))) if allMaps.contains(nodeself) =>
      partitionedMaps += nodeself
      rightPartFunc += nodeself -> field
      struct match {
        case Def(ArrayApply(arr, ind)) => rightPartArr += nodeself -> arr
        case _                         =>
      }
      ()
  }

  def numBuckets[T](tp: PardisType[T]): Rep[Int] = unit(100)
  def numBuckets(partitionedObject: PartitionObject): Rep[Int] = partitionedObject.arr match {
    case Def(ArrayNew(l)) => l * unit(2)
    case sym              => System.out.println(s"setting default value for $sym"); numBuckets(sym.tp.typeArguments(0))
  }
  def bucketSize(partitionedObject: PartitionObject): Rep[Int] = //unit(100)
    // numBuckets(partitionedObject)
    unit(1 << 7)

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
    case sym -> (node @ MultiMapNew()) if partitionedMaps.exists(x => x == sym) => {
      val hmParObj = getPartitionedObject(sym)(sym.tp)
      hmParObj.left.foreach { l =>
        createPartitionArray(l)
      }
      // hmParObj.right.foreach { r =>
      //   createPartitionArray(r)
      // }
      unit()
    }
  }

  rewrite += remove {
    case MultiMapGet(mm, elem) if shouldBePartitioned(mm) => {
      ()
    }
  }

  rewrite += remove {
    case MultiMapAddBinding(mm, _, nodev) if shouldBePartitioned(mm) =>
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

  rewrite += rule {
    case SetForeach(Def(OptionGet(Def(MultiMapGet(mm, elem)))), f) if shouldBePartitioned(mm) && getPartitionedObject(mm).left.nonEmpty => {
      val hmParObj = getPartitionedObject(mm)
      val leftArray = hmParObj.left.get
      val key = elem.asInstanceOf[Rep[Int]] % leftArray.buckets
      Range(unit(0), leftArray.count(key)).foreach {
        __lambda { i =>
          val e = leftArray.parArr(key)(i)
          __ifThenElse(field[Int](e, leftArray.fieldFunc) __== elem, {
            inlineFunction(f, e)
          }, {
            unit(())
          })
        }
      }
    }
  }
}
