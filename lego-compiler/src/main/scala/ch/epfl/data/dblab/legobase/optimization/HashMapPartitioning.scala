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
import scala.collection.mutable

/**
 * A transformer for partitioning and indexing the MultiMaps. As a result, this transformation
 * converts a MultiMap and the corresponding operations into an Array (either one dimensional or
 * two dimensional).
 *
 * TODO maybe add an example
 *
 * @param IR the polymorphic embedding trait which contains the reified program.
 */
class HashMapPartitioningTransformer(override val IR: LoweringLegoBase,
                                     val schema: Schema)
  extends RuleBasedTransformer[LoweringLegoBase](IR)
  with WhileLoopProcessing {
  import IR.{ __struct_field => _, __block => _, _ }

  /**
   * Keeps the information about a relation which (probably) participates in a join.
   * This information is gathered during the analysis phase.
   */
  case class RelationInfo(partitioningField: String,
                          loop: While,
                          array: Rep[Array[Any]]) {
    def tpe: TypeRep[Any] = array.tp.typeArguments(0).asInstanceOf[TypeRep[Any]]
    def numBuckets: Rep[Int] =
      unit(schema.stats.getDistinctAttrValues(partitioningField))
    def bucketSize: Rep[Int] =
      unit(schema.stats.getConflictsAttr(partitioningField).getOrElse(1 << 10))
    def is1D: Boolean =
      schema.findTableByType(tpe).exists(table =>
        table.primaryKey.exists(pk =>
          pk.attributes.size == 1 && pk.attributes.head.name == partitioningField))
    def table: Table = schema.findTableByType(tpe).get
    def reuseOriginal1DArray: Boolean = table.continuous.nonEmpty
    def arraySize: Rep[Int] = array match {
      case dsl"new Array[Any]($s)" => s
    }
    def loopIndexVariable: Var[Int] = loop.cond match {
      case RangeCondition(v, _) => v
      case _                    => throw new Exception("While loop without appropriate format of condition")
    }
  }

  /**
   * Keeps the information about a MultiMap
   */
  case class MultiMapInfo(multiMapSymbol: Rep[Any],
                          leftRelationInfo: Option[RelationInfo] = None,
                          rightRelationInfo: Option[RelationInfo] = None,
                          isPartitioned: Boolean = false,
                          isPotentiallyWindow: Boolean = false,
                          isPotentiallyAnti: Boolean = false,
                          foreachLambda: Option[Lambda[Any, Unit]] = None,
                          collectionForeachLambda: Option[Rep[Any => Unit]] = None,
                          outerDefault: Option[Block[Unit]] = None) {
    def isOuter: Boolean = getLoweredSymbolOriginalDef(multiMapSymbol) match {
      case Some(loj: LeftOuterJoinOpNew[_, _, _]) => true
      case _                                      => false
    }
    def isAnti: Boolean = isPotentiallyAnti && isPotentiallyWindow
    def shouldBePartitioned: Boolean =
      isPartitioned && (leftRelationInfo.nonEmpty || rightRelationInfo.nonEmpty)
    def partitionedRelationInfo: RelationInfo = (leftRelationInfo, rightRelationInfo) match {
      case _ if isAnti        => rightRelationInfo.get
      case (Some(v), None)    => v
      case (None, Some(v))    => v
      case (Some(l), Some(r)) => l
      case _                  => throw new Exception(s"$this does not have partitioned relation")
    }
    def hasLeft: Boolean =
      leftRelationInfo.nonEmpty
  }

  /**
   * Allows to update and get the associated MultiMapInfo for a MultiMap symbol
   */
  implicit class MultiMapOps[T, S](mm: Rep[MultiMap[T, S]]) {
    private def key = mm.asInstanceOf[Rep[Any]]
    def updateInfo(newInfoFunction: (MultiMapInfo => MultiMapInfo)): Unit =
      multiMapsInfo(key) = newInfoFunction(getInfo)
    def getInfo: MultiMapInfo =
      multiMapsInfo.getOrElseUpdate(key, MultiMapInfo(key))
  }

  /**
   * Data structures for storing the information collected during the analysis phase
   */
  val allMaps = mutable.Set[Rep[Any]]()
  val multiMapsInfo = mutable.Map[Rep[Any], MultiMapInfo]()
  /* Keeps the closest while loop in the scope */
  var currentWhileLoop: While = _

  override def optimize[T: TypeRep](node: Block[T]): Block[T] = {
    val res = super.optimize(node)
    System.out.println(s"${scala.Console.GREEN}[$transformedMapsCount] MultiMaps partitioned!${scala.Console.RESET}")
    res
  }

  /* ---- ANALYSIS PHASE ---- */

  analysis += statement {
    // TODO `new MultiMap` is synthetic and does not exist in shallow
    case sym -> (node @ MultiMapNew()) if node.typeB.isRecord =>
      allMaps += sym
      ()
  }

  analysis += statement {
    case sym -> (node @ dsl"while($cond) $body") =>
      currentWhileLoop = node.asInstanceOf[While]
  }

  analysis += rule {
    case dsl"($mm : MultiMap[Any, Any]).addBinding(__struct_field($struct, ${ Constant(field) }), $nodev)" if allMaps.contains(mm) =>
      mm.updateInfo(_.copy(isPartitioned = true))
      for (array <- getCorrespondingArray(struct)) {
        mm.updateInfo(_.copy(leftRelationInfo =
          Some(RelationInfo(field, currentWhileLoop, array))))
      }
  }

  analysis += rule {
    case dsl"($mm : MultiMap[Any, Any]).get(__struct_field($struct, ${ Constant(field) }))" if allMaps.contains(mm) =>
      mm.updateInfo(_.copy(isPartitioned = true))
      for (array <- getCorrespondingArray(struct)) {
        mm.updateInfo(_.copy(rightRelationInfo =
          Some(RelationInfo(field, currentWhileLoop, array))))
      }
      ()
  }

  analysis += rule {
    case dsl"($mm : MultiMap[Any, Any]).foreach($f)" if allMaps.contains(mm) =>
      mm.updateInfo(_.copy(isPotentiallyWindow = true))
      f match {
        case Def(fun @ Lambda(_, _, _)) =>
          val lambda = fun.asInstanceOf[Lambda[Any, Unit]]
          mm.updateInfo(_.copy(foreachLambda = Some(lambda)))
        case _ => ()
      }
      ()
  }

  analysis += rule {
    case dsl"($mm : MultiMap[Any, Any]).get($elem).get" if allMaps.contains(mm) =>
      // At this phase it's potentially anti hash join multimap, it's not specified for sure
      mm.updateInfo(_.copy(isPotentiallyAnti = true))
      ()
  }

  analysis += rule {
    case dsl"($mm : MultiMap[Any, Any]).get($elem).get.foreach($f)" => {
      val lambda = f.asInstanceOf[Rep[Any => Unit]]
      mm.updateInfo(_.copy(collectionForeachLambda = Some(lambda)))
    }
  }

  analysis += rule {
    case dsl"""(
                if(($mm: MultiMap[Any, Any]).get($elem).nonEmpty) 
                  $thenp 
                else 
                  $elsep
               ): Unit""" if mm.getInfo.isOuter => {
      mm.updateInfo(_.copy(outerDefault = Some(elsep)))
      ()
    }
  }

  def getCorrespondingArray(exp: Rep[Any]): Option[Rep[Array[Any]]] = exp match {
    case dsl"($array: Array[Any]).apply($index)" =>
      Some(array)
    case _ =>
      None
  }

  /**
   * Provides additional operations for a relation which is partitioned
   */
  implicit class PartitionedRelationInfoOps(relationInfo: RelationInfo) {
    import PartitionedRelationInfoOps._
    def count: Rep[Array[Int]] =
      partitionedRelationCount(relationInfo)
    def count_=(value: Rep[Array[Int]]): Unit =
      partitionedRelationCount(relationInfo) = value
    def partitionedArray: Rep[Array[Array[Any]]] =
      partitionedRelationArray(relationInfo).asInstanceOf[Rep[Array[Array[Any]]]]
    def partitionedArray_=[T](value: Rep[Array[T]]) =
      partitionedRelationArray(relationInfo) = value.asInstanceOf[Rep[Array[Any]]]
  }

  /**
   * Stores a code that is produced somewhere and should be reified somewhere else.
   */
  class CodeReifier {
    private var _code: () => Rep[Any] = _
    /**
     * Stores the given code.
     * The given code will not be reified here.
     */
    def :=(code: => Rep[Any]): Unit = {
      _code = () => code
    }
    /**
     * Reifies the stored code.
     */
    def apply[T](): Rep[T] =
      reify[T]()
    /**
     * Reifies the stored code.
     */
    def reify[T](): Rep[T] =
      _code().asInstanceOf[Rep[T]]
  }

  /**
   * Provides additional operations for a MultiMap which is partitioned
   */
  implicit class PartitionedMultiMapInfoOps(multiMap: Rep[Any]) {
    import PartitionedMultiMapInfoOps._
    def antiRetainVar: Var[Boolean] =
      hashJoinAntiRetainVar(multiMap)
    def outerExistsVar: Var[Boolean] =
      leftOuterJoinExistsVar(multiMap)
    def antiRetainVar_=(value: Var[Boolean]): Unit =
      hashJoinAntiRetainVar(multiMap) = value
    def outerExistsVar_=(value: Var[Boolean]): Unit =
      leftOuterJoinExistsVar(multiMap) = value
    /**
     * Specifies if during the rewriting, we are in the phase of filling the hole
     * for a given MultiMap
     */
    def isFillingHole: Boolean =
      fillingHole.contains(multiMap)
    def startFillingHole(): Unit =
      fillingHole += multiMap
    def finishFillingHole(): Unit =
      fillingHole -= multiMap
    /**
     * Reifies the code that is needed for the processing that happening for the
     * right element of a join.
     */
    def rightElemProcessingCode: CodeReifier =
      rightElemProcessingCodeMap.getOrElseUpdate(multiMap, new CodeReifier())
    /**
     * Keeps the code for accessing the left element of a join that should be
     * substituted instead of an array access.
     */
    def leftElemCode: CodeReifier =
      leftElemCodeMap.getOrElseUpdate(multiMap, new CodeReifier())
  }

  /**
   * Data structures for storing the expressions created during the rewriting phase
   */
  object PartitionedRelationInfoOps {
    val partitionedRelationArray = mutable.Map[RelationInfo, Rep[Array[Any]]]()
    val partitionedRelationCount = mutable.Map[RelationInfo, Rep[Array[Int]]]()
  }
  object PartitionedMultiMapInfoOps {
    val hashJoinAntiRetainVar = mutable.Map[Rep[Any], Var[Boolean]]()
    val leftOuterJoinExistsVar = mutable.Map[Rep[Any], Var[Boolean]]()
    val fillingHole = mutable.Set[Rep[Any]]()
    val rightElemProcessingCodeMap = mutable.Map[Rep[Any], CodeReifier]()
    val leftElemCodeMap = mutable.Map[Rep[Any], CodeReifier]()
  }

  var transformedMapsCount = 0

  /* ---- REWRITING PHASE ---- */

  /*
   * If a MultiMap should be partitioned, instead of the construction of that MultiMap object,
   * partitions the corresponding array (if it is needed to be partitioned).
   */
  rewrite += statement {
    case sym -> (node @ MultiMapNew()) if sym.asInstanceOf[Rep[MultiMap[Any, Any]]].getInfo.shouldBePartitioned => {

      createPartitionArray(sym.asInstanceOf[Rep[MultiMap[Any, Any]]].getInfo.partitionedRelationInfo)

      sym
    }
  }

  /* The case for HashJoinAnti */

  rewrite += rule {
    case dsl"($mm: MultiMap[Any, Any]).addBinding($elem, $nodev)" if mm.getInfo.shouldBePartitioned &&
      mm.getInfo.isAnti =>
      class ElemType
      implicit val elemType = nodev.tp.asInstanceOf[TypeRep[ElemType]]
      val value = apply(nodev).asInstanceOf[Rep[ElemType]]
      val mmInfo = mm.getInfo
      val rightArray = mmInfo.partitionedRelationInfo
      val key = apply(elem).asInstanceOf[Rep[Int]]
      val antiLambda = mmInfo.foreachLambda.get
      val foreachFunction = antiLambda.body match {
        case dsl"__block{($set: Set[Any]).foreach($f)}" => f.asInstanceOf[Rep[ElemType => Unit]]
      }
      val resultRetain = __newVarNamed[Boolean](unit(false), "resultRetain")
      mm.antiRetainVar = resultRetain
      class ElemType2
      implicit val elemType2 = rightArray.array.tp.typeArguments(0).asInstanceOf[TypeRep[ElemType2]]
      par_array_foreach[ElemType2](rightArray, key, (e: Rep[ElemType2]) => {
        mm.leftElemCode := e
        mm.rightElemProcessingCode := value
        mm.startFillingHole()
        val res = inlineBlock2(rightArray.loop.body)
        mm.finishFillingHole()
        res
      })
      transformedMapsCount += 1
      dsl"""if(${!readVar(resultRetain)}) {
            ${inlineFunction(foreachFunction, value)}
          } else {
          }"""
  }

  rewrite += remove {
    case dsl"($mm: MultiMap[Any, Any]).foreach($f)" if mm.getInfo.shouldBePartitioned &&
      mm.getInfo.isAnti =>
      ()
  }

  rewrite += rule {
    // TODO a bit ugly, because of the type inference we have to put `: Any`
    case dsl"""(
                if(($mm: MultiMap[Any, Any]).get($elem).nonEmpty) 
                  $thenp 
                else 
                  $elsep
               ): Any""" if mm.getInfo.shouldBePartitioned &&
      mm.getInfo.isAnti &&
      mm.isFillingHole =>
      class ElemType
      val retainPredicate = thenp match {
        case dsl"__block{ ($set: Set[Any]).retain($pred); $res }" => pred.asInstanceOf[Rep[ElemType => Boolean]]
      }
      val typedElem = mm.rightElemProcessingCode[ElemType]()
      implicit val elemType = typedElem.tp.asInstanceOf[TypeRep[ElemType]]
      val resultRetain = mm.antiRetainVar
      dsl"""if(!${inlineFunction(retainPredicate, typedElem)}) {
              ${__assign(resultRetain, unit(true))}
            } else {
            }"""
  }

  /* The case for HashJoin and LeftOuterJoin */

  rewrite += remove {
    case dsl"($mm: MultiMap[Any, Any]).addBinding($elem, $nodev)" if mm.getInfo.shouldBePartitioned &&
      mm.getInfo.hasLeft &&
      !mm.isFillingHole =>
      ()
  }

  rewrite += rule {
    case dsl"($mm: MultiMap[Any, Any]).addBinding($elem, $nodev)" if mm.getInfo.shouldBePartitioned &&
      mm.getInfo.hasLeft &&
      mm.isFillingHole =>
      mm.rightElemProcessingCode()
  }

  rewrite += rule {
    case dsl"($mm: MultiMap[Any, Any]).addBinding($elem, $nodev)" if mm.getInfo.shouldBePartitioned &&
      !mm.getInfo.hasLeft =>
      val rightArray = mm.getInfo.partitionedRelationInfo
      val key = apply(elem).asInstanceOf[Rep[Int]]
      val whileLoop = rightArray.loop
      class InnerType
      implicit val typeInner = rightArray.tpe.asInstanceOf[TypeRep[InnerType]]
      par_array_foreach[InnerType](rightArray, key, (e: Rep[InnerType]) => {
        mm.leftElemCode := e
        mm.rightElemProcessingCode := apply(nodev)
        mm.startFillingHole()
        val res1 = inlineBlock2(whileLoop.body)
        mm.finishFillingHole()
        transformedMapsCount += 1
        res1
      })
  }

  def arrayApplyAssociatesToMultiMap(array: Rep[Array[Any]],
                                     indexVariable: Var[Int],
                                     multiMapInfo: MultiMapInfo): Boolean = {
    multiMapInfo.shouldBePartitioned &&
      multiMapInfo.partitionedRelationInfo.array == array &&
      multiMapInfo.partitionedRelationInfo.loopIndexVariable == indexVariable &&
      multiMapInfo.multiMapSymbol.isFillingHole
  }

  // TODO `as` can improve this rule a lot
  // TODO var handling in quasi quotes can beautify this rule a lot
  /* 
   * Substitutes the array accesses with the filling element specified before.
   */
  rewrite += rule {
    case dsl"($arr: Array[Any]).apply(${ Def(ReadVar(indexVariable)) })" if multiMapsInfo.exists({
      case (_, info) =>
        arrayApplyAssociatesToMultiMap(arr, indexVariable, info)
    }) =>
      val multiMapSymbol = multiMapsInfo.find({
        case (_, info) =>
          arrayApplyAssociatesToMultiMap(arr, indexVariable, info)
      }).head._1
      multiMapSymbol.leftElemCode()
  }

  /*
   * If the loop corresponds to a relation which should be partitioned, 
   * the loop is completely removed. However, the body of the loop is regenerated
   * somewhere else.
   */
  rewrite += remove {
    case node @ dsl"while($cond) $body" if multiMapsInfo.exists({
      case (mm, info) => info.shouldBePartitioned &&
        info.partitionedRelationInfo.loop == node
    }) =>
      ()
  }

  rewrite += remove {
    case dsl"($mm: MultiMap[Any, Any]).get($elem)" if mm.getInfo.shouldBePartitioned => {
      ()
    }
  }

  rewrite += rule {
    case dsl"($mm: MultiMap[Any, Any]).get($elem).nonEmpty" if mm.getInfo.shouldBePartitioned =>
      unit(true)
  }

  rewrite += remove {
    case dsl"($mm: MultiMap[Any, Any]).get($elem).get" if mm.getInfo.shouldBePartitioned =>
      ()
  }

  rewrite += removeStatement {
    case sym -> Lambda(_, _, _) if multiMapsInfo.exists(_._2.collectionForeachLambda.exists(_ == sym)) =>
      ()
  }

  rewrite += rule {
    case dsl"($mm: MultiMap[Any, Any]).get($elem).get.foreach($f)" if mm.getInfo.shouldBePartitioned &&
      mm.getInfo.hasLeft => {
      val leftArray = mm.getInfo.partitionedRelationInfo
      val key = apply(elem).asInstanceOf[Rep[Int]]
      val whileLoop = leftArray.loop
      leftOuterJoinExistsVarDefine(mm)
      class InnerType
      implicit val typeInner = leftArray.tpe.asInstanceOf[TypeRep[InnerType]]
      par_array_foreach[InnerType](leftArray, key, (e: Rep[InnerType]) => {
        mm.leftElemCode := e
        mm.rightElemProcessingCode := {
          def ifThenBody: Rep[Unit] = {
            leftOuterJoinExistsVarSet(mm)
            inlineFunction(f.asInstanceOf[Rep[InnerType => Unit]], e)
          }
          dsl"""if(${field[Int](e, leftArray.partitioningField)} == ${apply(elem)}) {
                    ${ifThenBody}
                  } else {
                  }"""
        }
        // System.out.println(s"STARTED setforeach for the key $key $e.${leftArray.fieldFunc} mm: $mm")
        mm.startFillingHole()
        val res1 = inlineBlock2(whileLoop.body)
        // System.out.println(s"FINISH setforeach for the key $key")
        mm.finishFillingHole()
        transformedMapsCount += 1
        res1
      })
      leftOuterJoinDefaultHandling(mm, key)
    }
  }

  rewrite += rule {
    case dsl"($mm: MultiMap[Any, Any]).get($elem).get.exists($p)" if mm.getInfo.shouldBePartitioned &&
      mm.getInfo.hasLeft => {
      val leftArray = mm.getInfo.partitionedRelationInfo
      val key = apply(elem).asInstanceOf[Rep[Int]]
      val whileLoop = leftArray.loop
      val result = __newVarNamed[Boolean](unit(false), "existsResult")
      class InnerType
      implicit val typeInner = leftArray.tpe.asInstanceOf[TypeRep[InnerType]]
      par_array_foreach[InnerType](leftArray, key, (e: Rep[InnerType]) => {
        mm.leftElemCode := e
        mm.rightElemProcessingCode := {
          dsl"""if(${field[Int](e, leftArray.partitioningField)} == $elem && ${inlineFunction(p, e)}) {
                    ${__assign(result, unit(true))}
                  } else {
                  }"""
        }
        mm.startFillingHole()
        val res1 = inlineBlock2(whileLoop.body)
        mm.finishFillingHole()
        transformedMapsCount += 1
        res1
      })
      readVar(result)
    }
  }

  rewrite += remove {
    case dsl"($mm: MultiMap[Any, Any]).get($elem).get.foreach($f)" if mm.getInfo.shouldBePartitioned &&
      !mm.getInfo.hasLeft &&
      !mm.isFillingHole =>
      ()
  }

  rewrite += rule {
    case dsl"($mm: MultiMap[Any, Any]).get($elem).get.foreach($f)" if mm.getInfo.shouldBePartitioned &&
      !mm.getInfo.hasLeft &&
      mm.isFillingHole =>
      inlineFunction(f, mm.rightElemProcessingCode[Any]())
  }

  /* ---- Helper Methods for Rewriting ---- */

  def array_foreach[T: TypeRep](arr: Rep[Array[T]], f: Rep[T] => Rep[Unit]): Rep[Unit] = {
    Range(unit(0), arr.length).foreach {
      __lambda { i =>
        val e = arr(i)
        f(e)
      }
    }
  }

  def par_array_foreach[T: TypeRep](partitionedRelationInfo: RelationInfo,
                                    key: Rep[Int],
                                    f: Rep[T] => Rep[Unit]): Rep[Unit] = {
    if (partitionedRelationInfo.is1D) {
      val parArr = partitionedRelationInfo.partitionedArray.asInstanceOf[Rep[Array[T]]]
      val bucket = partitionedRelationInfo.table.continuous match {
        case Some(continuous) => key - unit(continuous.offset)
        case None             => key
      }
      val e = parArr(bucket)
      // System.out.println(s"part foreach for val $e=$parArr($bucket) ")
      f(e)
    } else {
      val bucket = key % partitionedRelationInfo.numBuckets
      val count = partitionedRelationInfo.count(bucket)
      val parArrWhole = partitionedRelationInfo.partitionedArray.asInstanceOf[Rep[Array[Array[T]]]]
      val parArr = parArrWhole(bucket)
      Range(unit(0), count).foreach {
        __lambda { i =>
          val e = parArr(i)
          f(e)
        }
      }
    }
  }

  def createPartitionArray(partitionedRelationInfo: RelationInfo): Unit = {
    System.out.println(scala.Console.GREEN + "Table " +
      partitionedRelationInfo.array.tp.typeArguments(0) + " was partitioned on field " +
      partitionedRelationInfo.partitioningField + scala.Console.RESET)

    class InnerType
    implicit val typeInner =
      partitionedRelationInfo.array.tp.typeArguments(0).asInstanceOf[TypeRep[InnerType]]
    val originalArray =
      partitionedRelationInfo.array.asInstanceOf[Rep[Array[InnerType]]]

    val buckets = partitionedRelationInfo.numBuckets
    if (partitionedRelationInfo.is1D) {
      //System.out.println(s"${scala.Console.BLUE}1D Array!!!${scala.Console.RESET}")
      if (partitionedRelationInfo.reuseOriginal1DArray) {
        partitionedRelationInfo.partitionedArray = originalArray
      } else {
        val partitionedArray = __newArray[InnerType](buckets)
        partitionedRelationInfo.partitionedArray = partitionedArray
        array_foreach(originalArray, {
          (e: Rep[InnerType]) =>
            val pkey = field[Int](e, partitionedRelationInfo.partitioningField) % buckets
            partitionedArray(pkey) = e
        })
      }
    } else {
      val partitionedObjectAlreadyExists = {
        PartitionedRelationInfoOps.partitionedRelationArray.find({
          case (po, _) =>
            po.partitioningField == partitionedRelationInfo.partitioningField &&
              po.tpe == partitionedRelationInfo.tpe
        })
      }
      if (partitionedObjectAlreadyExists.nonEmpty) {
        System.out.println(s"${scala.Console.BLUE}2D Array already exists!${scala.Console.RESET}")
        partitionedRelationInfo.partitionedArray = partitionedObjectAlreadyExists.get._1.partitionedArray
        partitionedRelationInfo.count = partitionedObjectAlreadyExists.get._1.count
      } else {
        val partitionedArray = __newArray[Array[InnerType]](buckets)
        val partitionedCount = __newArray[Int](buckets)
        partitionedRelationInfo.partitionedArray = partitionedArray
        partitionedRelationInfo.count = partitionedCount
        Range(unit(0), buckets).foreach {
          __lambda { i =>
            partitionedArray(i) = __newArray[InnerType](partitionedRelationInfo.bucketSize)
          }
        }
        val index = __newVarNamed[Int](unit(0), "partIndex")
        array_foreach(originalArray, {
          (e: Rep[InnerType]) =>
            // TODO needs a better way of computing the index of each object
            val pkey = field[Int](e, partitionedRelationInfo.partitioningField) % buckets
            val currIndex = partitionedCount(pkey)
            val partitionedArrayBucket = partitionedArray(pkey)
            partitionedArrayBucket(currIndex) = e
            partitionedCount(pkey) = currIndex + unit(1)
            __assign(index, readVar(index) + unit(1))
        })
      }
    }
  }

  /* The parts dedicated to left outer join handling */
  def leftOuterJoinDefaultHandling(mm: Rep[MultiMap[Any, Any]], key: Rep[Int]): Rep[Unit] = if (mm.getInfo.isOuter) {
    dsl"""if(!${readVar(mm.outerExistsVar)}) {
              ${inlineBlock[Unit](mm.getInfo.outerDefault.get)}
            } else {
            }"""
  } else dsl"()"

  def leftOuterJoinExistsVarDefine(mm: Rep[MultiMap[Any, Any]]): Unit =
    if (mm.getInfo.isOuter) {
      val exists = __newVarNamed[Boolean](unit(false), "exists")
      mm.outerExistsVar = exists
      ()
    } else ()

  def leftOuterJoinExistsVarSet(mm: Rep[MultiMap[Any, Any]]): Unit =
    if (mm.getInfo.isOuter) {
      val exists = mm.outerExistsVar
      __assign(exists, unit(true))
      ()
    } else ()
}
