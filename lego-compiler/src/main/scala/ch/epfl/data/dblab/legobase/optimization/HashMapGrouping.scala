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
 * As an example the following program:
 * {{{
 *      val multiMap = new MultiMap[Int, LeftRelation]
 *      var leftIndex = 0
 *      while(leftIndex < leftArray.length) {
 *        val leftElement = leftArray(leftIndex)
 *        if(leftPredicate(leftElement)) {
 *          multiMap.addBinding(leftElement.primaryKey, leftElement)
 *        }
 *        leftIndex += 1
 *      }
 *      var rightIndex = 0
 *      while(rightIndex < rightArray.length) {
 *        val rightElement = rightArray(rightIndex)
 *        val setOption = multiMap.get(rightElement.foreignKey)
 *        if(setOption.nonEmpty) {
 *          val tmpBuffer = setOption.get
 *          tmpBuffer foreach { leftElement =>
 *            if (leftElement.primaryKey == rightElement.foreignKey) {
 *              process(leftElement, rightElement)
 *            }
 *          }
 *        }
 *        rightIndex += 1
 *      }
 * }}}
 * is converted to:
 * {{{
 *      var rightIndex = 0
 *      val leftArrayGroupedByPrimaryKey: Array[LeftRelation] = {
 *        // Constructs an array in which the index of each element in that array
 *        // is the primary key of that element (cf. `createPartitionedArray` method)
 *      }
 *      while(rightIndex < rightArray.length) {
 *        val rightElement = rightArray(rightIndex)
 *        val primaryKey = rightElement.foreignKey
 *        val leftElement = leftArrayGroupedByPrimaryKey(primaryKey)
 *        if(leftPredicate(leftElement)) {
 *          if(leftElement.primaryKey == rightElement.foreignKey) {
 *            process(leftElement, rightElement)
 *          }
 *        }
 *        rightIndex += 1
 *      }
 * }}}
 *
 * In the given example, three loops (2 while loops + 1 foreach) are converted
 * into a single loop (1 while loop).
 *
 * @param IR the polymorphic embedding trait which contains the reified program.
 */
class HashMapGrouping(override val IR: LoweringLegoBase,
                      val schema: Schema)
  extends RuleBasedTransformer[LoweringLegoBase](IR)
  with WhileRangeProcessing {
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

  /*
   * Gatters all MultiMap symbols which are holding a record collection as 
   * their value 
   */
  analysis += statement {
    // TODO `new MultiMap` is synthetic and does not exist in shallow
    case sym -> (node @ MultiMapNew()) if node.typeB.isRecord =>
      allMaps += sym
      ()
  }

  /*
   * Keeps the closest while loop in the scope
   */
  analysis += statement {
    case sym -> (node @ dsl"while($cond) $body") =>
      currentWhileLoop = node.asInstanceOf[While]
  }

  /*
   * The following pattern is used for the left relation of HashJoin and LeftOuterJoin
   */
  analysis += rule {
    case dsl"($mm : MultiMap[Any, Any]).addBinding(__struct_field($struct, ${ Constant(field) }), $nodev)" if allMaps.contains(mm) =>
      mm.updateInfo(_.copy(isPartitioned = true))
      for (array <- getCorrespondingArray(struct)) {
        mm.updateInfo(_.copy(leftRelationInfo =
          Some(RelationInfo(field, currentWhileLoop, array))))
      }
  }

  /*
   * The following pattern is used for the right relation of HashJoin and LeftOuterJoin
   */
  analysis += rule {
    case dsl"($mm : MultiMap[Any, Any]).get(__struct_field($struct, ${ Constant(field) }))" if allMaps.contains(mm) =>
      mm.updateInfo(_.copy(isPartitioned = true))
      for (array <- getCorrespondingArray(struct)) {
        mm.updateInfo(_.copy(rightRelationInfo =
          Some(RelationInfo(field, currentWhileLoop, array))))
      }
      ()
  }

  /*
   * The following pattern is used for the right relation of HashJoinAnti and Window
   */
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

  /*
   * The following pattern is used for the right relation of HashJoinAnti
   */
  analysis += rule {
    case dsl"($mm : MultiMap[Any, Any]).get($elem).get" if allMaps.contains(mm) =>
      // At this phase it's potentially anti hash join multimap, it's not specified for sure
      mm.updateInfo(_.copy(isPotentiallyAnti = true))
      ()
  }

  /*
   * The following pattern is used for the right relation of HashJoin and LeftOuterJoin
   */
  analysis += rule {
    case dsl"($mm : MultiMap[Any, Any]).get($elem).get.foreach($f)" => {
      val lambda = f.asInstanceOf[Rep[Any => Unit]]
      mm.updateInfo(_.copy(collectionForeachLambda = Some(lambda)))
    }
  }

  /*
   * The following pattern is used for LeftOuterJoin
   */
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

  /*
   * For an expression representing accessing an element of a particular array,
   * returns that array.
   */
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

      createPartitionedArray(sym.asInstanceOf[Rep[MultiMap[Any, Any]]].getInfo.partitionedRelationInfo)

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
      partitionedArrayForeach[ElemType2](rightArray, key, (e: Rep[ElemType2]) => {
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

  rewrite += rule {
    case dsl"($mm: MultiMap[Any, Any]).get($elem).get.exists($p)" if mm.getInfo.shouldBePartitioned &&
      mm.getInfo.hasLeft => {
      val leftArray = mm.getInfo.partitionedRelationInfo
      val key = apply(elem).asInstanceOf[Rep[Int]]
      val whileLoop = leftArray.loop
      val result = __newVarNamed[Boolean](unit(false), "existsResult")
      class InnerType
      implicit val typeInner = leftArray.tpe.asInstanceOf[TypeRep[InnerType]]
      partitionedArrayForeach[InnerType](leftArray, key, (e: Rep[InnerType]) => {
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

  /* The case for HashJoin and LeftOuterJoin */

  /*
   * Removes the `addBinding` for the left relation, in the case 
   * that the left relation should be partitioned.
   */
  rewrite += remove {
    case dsl"($mm: MultiMap[Any, Any]).addBinding($elem, $nodev)" if mm.getInfo.shouldBePartitioned &&
      mm.getInfo.hasLeft &&
      !mm.isFillingHole =>
      ()
  }

  /*
   * Rewrites the iteration of the set of all matching elements with the right element
   * to an iteration over the partitioned relation (cf. partitionedArrayForeach method).
   * In the body of this iteration, it will put the body of the while loop for the left
   * relation. However, it will substitute the `addBinding` in that while loop, with the
   * code snippet assigned to `rightElemProcessingCode`. Furthermore, in that while loop
   * the array accesses for the left relation should also be substituted by the elements
   * that we are iterating over. These substitutations are performed by other rewrite rules.
   */
  rewrite += rule {
    case dsl"($mm: MultiMap[Any, Any]).get($elem).get.foreach($f)" if mm.getInfo.shouldBePartitioned &&
      mm.getInfo.hasLeft => {
      val leftArray = mm.getInfo.partitionedRelationInfo
      val key = apply(elem).asInstanceOf[Rep[Int]]
      val whileLoop = leftArray.loop
      leftOuterJoinExistsVarDefine(mm)
      class InnerType
      implicit val typeInner = leftArray.tpe.asInstanceOf[TypeRep[InnerType]]
      partitionedArrayForeach[InnerType](leftArray, key, (e: Rep[InnerType]) => {
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

  /*
   * While it reifies the code for the right relation (iterating over the appropriate 
   * bucket in the partitioned array of the left relation), instead of addBinding, puts the
   * code snippet that was constructed in the previous rewrite rule.
   */
  rewrite += rule {
    case dsl"($mm: MultiMap[Any, Any]).addBinding($elem, $nodev)" if mm.getInfo.shouldBePartitioned &&
      mm.getInfo.hasLeft &&
      mm.isFillingHole =>
      mm.rightElemProcessingCode()
  }

  /*
   * The 3 rewrite rules below are dual to the previous 3 rewrite rules.
   * The previous 3 rewrite rules were applicable in the case that the left relation
   * was partitioned, however the following rewrite rules are applicable whenever
   * the right relation should be partitioned.
   */

  rewrite += rule {
    case dsl"($mm: MultiMap[Any, Any]).addBinding($elem, $nodev)" if mm.getInfo.shouldBePartitioned &&
      !mm.getInfo.hasLeft =>
      val rightArray = mm.getInfo.partitionedRelationInfo
      val key = apply(elem).asInstanceOf[Rep[Int]]
      val whileLoop = rightArray.loop
      class InnerType
      implicit val typeInner = rightArray.tpe.asInstanceOf[TypeRep[InnerType]]
      partitionedArrayForeach[InnerType](rightArray, key, (e: Rep[InnerType]) => {
        mm.leftElemCode := e
        mm.rightElemProcessingCode := apply(nodev)
        mm.startFillingHole()
        val res1 = inlineBlock2(whileLoop.body)
        mm.finishFillingHole()
        transformedMapsCount += 1
        res1
      })
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

  /*
   * For a partitioned relation, the following statement should always be true. 
   * While we are iterating over the partitioned array the emptiness is implicitly 
   * encoded.
   */
  rewrite += rule {
    case dsl"($mm: MultiMap[Any, Any]).get($elem).nonEmpty" if mm.getInfo.shouldBePartitioned =>
      unit(true)
  }

  /*
   * As we are completely removing MultiMap abstraction, the following statements
   * should be completely removed.
   */

  rewrite += remove {
    case dsl"($mm: MultiMap[Any, Any]).get($elem)" if mm.getInfo.shouldBePartitioned => {
      ()
    }
  }

  rewrite += remove {
    case dsl"($mm: MultiMap[Any, Any]).get($elem).get" if mm.getInfo.shouldBePartitioned =>
      ()
  }

  rewrite += removeStatement {
    case sym -> Lambda(_, _, _) if multiMapsInfo.exists(_._2.collectionForeachLambda.exists(_ == sym)) =>
      ()
  }

  /**
   * Specifies if an array element access should be substituted with another
   * expression specified in other rewrite rules.
   */
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
   * Substitutes the array accesses with the filling element specified while
   * reifying the code snippet for iterating the partitioned array.
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

  /* ---- Helper Methods for Rewriting ---- */

  /**
   * Iterates over the given `index` of the partitioned array for the
   * given relation, and applies the given function `f`.
   *
   * In the case that a 1D partitioned array is created (the partitioning field
   * is the primary key), function `f` will be applied to only one element with
   * the given primary key.
   * In the case of 2D partitioned array, the function `f` is applied to all
   * elements of the bucket specified by `index`.
   */
  def partitionedArrayForeach[T: TypeRep](partitionedRelationInfo: RelationInfo,
                                          index: Rep[Int],
                                          f: Rep[T] => Rep[Unit]): Rep[Unit] = {
    if (partitionedRelationInfo.is1D) {
      val parArr = partitionedRelationInfo.partitionedArray.asInstanceOf[Rep[Array[T]]]
      val bucket = partitionedRelationInfo.table.continuous match {
        case Some(continuous) => index - unit(continuous.offset)
        case None             => index
      }
      val e = parArr(bucket)
      // System.out.println(s"part foreach for val $e=$parArr($bucket) ")
      f(e)
    } else {
      val bucket = index % partitionedRelationInfo.numBuckets
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

  /**
   * For the given relation, creates a partitioned array.
   *
   * There are different cases when a relation is partitioned:
   * 1) The given relation is partitioned on its primary key. The partitioned array
   *    suffices to be 1D.
   *    1a) If the primary key value of the relation is continous, the original relation
   *        array can be reused
   *    1b) If it is not the case, a new array should be generated that the index of
   *        each element is the same as its primary key
   * 2) The partitioning field is not the primary key. The partitioned array should
   *    be a 2D array in which the first index specifies the bucket number (indexed using
   *    the value of partitioning field). For every bucket a value should be associated
   *    which keeps the size of each bucket. For that another 1D array is generated.
   *    There are cases that an already partitioned array can be reused, without the need
   *    to create a new partitioned array.
   *
   */
  def createPartitionedArray(partitionedRelationInfo: RelationInfo): Unit = {
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
