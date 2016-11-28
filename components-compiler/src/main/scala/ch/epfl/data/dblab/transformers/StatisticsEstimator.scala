package ch.epfl.data
package dblab
package transformers

import schema._
import utils.Logger
import scala.language.implicitConversions
import sc.pardis.ir._
import deep.dsls.QueryEngineExp
import reflect.runtime.universe.{ TypeTag, Type }
import sc.pardis.optimization._
import deep._
import sc.pardis.types._
import sc.pardis.types.PardisTypeImplicits._
import sc.pardis.shallow.utils._
import sc.pardis.ir.StructTags._

/**
 * Analyzes the query to gather additional statistics.
 *
 * @param IR the polymorphic embedding trait which contains the reified program.
 * @param schema the schema over which the query runs. This already contains base
 * relations cardinalities and selectivities between two-way joins
 */
class StatisticsEstimator(override val IR: QueryEngineExp, val schema: Schema) extends RuleBasedTransformer[QueryEngineExp](IR) {
  import IR._
  val logger = Logger[StatisticsEstimator]

  override def optimize[T: TypeRep](node: Block[T]): to.Block[T] = {
    schema.stats.removeQuerySpecificStats()
    traverseBlock(node)
    logger.debug(schema.stats.mkString("\n"))
    node
  }

  var analyzed: Boolean = false
  val aliasesList = new scala.collection.mutable.ListBuffer[String]()

  def getUnaliasedStructFieldName(fieldName: String) = {
    aliasesList.foldLeft(fieldName)((fn, alias) => {
      fn.replaceAll(alias, "")
    })
  }

  def estimateCardinalityFromExpression(expr: Rep[Any]): Option[Int] = expr match {
    case Def(GenericEngineDateToYearObject(_)) => Some(schema.stats.getNumYearsAllDates())
    case Def(ifa: ImmutableFieldAccess[_])     => schema.stats.distinctAttributes(getUnaliasedStructFieldName(ifa.field))
    case _ =>
      logger.warn(s"${scala.Console.RED}Warning${scala.Console.RESET}: Statistics Estimator cannot accurately estimate cardinality for node " + expr.correspondingNode + ". Returning default estimation. This may lead to degraded performance due to unnecessarily large memory pool allocations. ")
      None
  }

  def estimateCardinalityFromFunction(fun: Rep[Any], defaultEstimation: Int): Int = {
    val cardinality = fun match {
      case Def(PardisLambda(_, _, Block(b, Constant(string)))) =>
        Some(1)
      case Def(PardisLambda(_, _, Block(b, res))) if res.correspondingNode.isInstanceOf[FieldDef[_]] =>
        val fieldName = res.correspondingNode.asInstanceOf[FieldDef[_]].field
        schema.stats.distinctAttributes(fieldName)
      case Def(PardisLambda(_, _, Block(b, res))) if res.correspondingNode.isInstanceOf[ConstructorDef[_]] =>
        val structArgs = res.correspondingNode.asInstanceOf[ConstructorDef[_]].argss.flatten
        val cardinalities = structArgs.map(a => estimateCardinalityFromExpression(a.asInstanceOf[Rep[Any]]))
        if (cardinalities.forall(_.nonEmpty)) {
          // Converted to Double because if the numbers are big, the computation gets wrong in the case of using Int
          val structArgsCombinations = cardinalities.flatMap(_.map(_.toDouble)).product
          if (defaultEstimation > structArgsCombinations)
            Some(structArgsCombinations.toInt)
          else
            None
        } else {
          None
        }
      case Def(PardisLambda(_, _, Block(b, res))) =>
        estimateCardinalityFromExpression(res)
    }
    cardinality match {
      case Some(c) => c
      case None    => defaultEstimation
    }
  }

  def analyzeQuery(op: Rep[Any]): Int = {
    var msg: String = null
    var typeInfo: String = null;

    val estimatedSize = op.correspondingNode match {
      case scanOp @ ScanOpNew(_) =>
        typeInfo = scanOp.typeA.toString;
        schema.stats.getCardinality(scanOp.typeA.toString).toInt
      case SelectOpNew(parent, _) =>
        // TODO-GEN: Calculate selectivities for SelectOP rather than blindly assuming 100%
        msg = s"${scala.Console.YELLOW}Selectivity of SelectOp is estimated as 100% for now${scala.Console.RESET}"
        analyzeQuery(parent)
      case MapOpNew(parent, _)  => analyzeQuery(parent)
      case SortOpNew(parent, _) => analyzeQuery(parent)
      case ViewOpNew(parent)    => analyzeQuery(parent)

      case PrintOpNew(parent, _, Constant(limit)) =>
        val parentES = analyzeQuery(parent)
        val es = if (limit != -1) limit else parentES
        schema.stats += "QS_OUTPUT_SIZE_ESTIMATION" -> es
        es

      case ao @ AggOpNew(parent, _, grp, _) =>
        val parentES = analyzeQuery(parent)
        val numDistinctVals = {
          val key = grp.tp.typeArguments(1).name
          schema.stats.cardinalities(key) match {
            case Some(card) => card
            case None       => estimateCardinalityFromFunction(grp, parentES)
          }
        }

        // Handle additional estimations per function type
        grp match {
          case Def(PardisLambda(_, _, Block(b, Def(_: ConstructorDef[_])))) =>
            // In this case, the key is a record itself. In this case, estimate how many such keys will be created
            schema.stats.querySpecificCardinalities += (ao.typeB.toString, parentES)
          case _ =>
        }

        // Estimate that the following number of aggregate records and aggregate arrays will be created
        schema.stats.querySpecificCardinalities += ("AGG_" + ao.typeB, numDistinctVals)
        schema.stats.querySpecificCardinalities += ("ARRAY_DOUBLE", numDistinctVals)
        numDistinctVals.toInt
      case wo @ WindowOpNew(parent, grp, _) =>
        val parentES = analyzeQuery(parent)
        val numDistinctVals = estimateCardinalityFromFunction(grp, parentES)
        // TODO-GEN Isn't there a better way to get the return type of window op that the next line?
        schema.stats increase ("QS_MEM_WINDOW_" + wo.typeB + "_" + wo.typeC) -> numDistinctVals
        numDistinctVals

      case NestedLoopsJoinOpNew(leftParent, rightParent, Constant(leftAlias), Constant(rightAlias), _) =>
        val leftParentES = analyzeQuery(leftParent)
        val rightParentES = analyzeQuery(rightParent)
        aliasesList += leftAlias
        aliasesList += rightAlias
        msg = s"${scala.Console.YELLOW} Assumes 1-N join for now: ${scala.Console.RESET}Max(leftParentSize,rightParentSize) = Max(" + leftParentES + "," + rightParentES + ") "
        Math.max(leftParentES, rightParentES)
      case HashJoinOpNew1(leftParent, rightParent, Constant(leftAlias), Constant(rightAlias), _, _, _) =>
        val leftParentES = analyzeQuery(leftParent)
        val rightParentES = analyzeQuery(rightParent)
        aliasesList += leftAlias
        aliasesList += rightAlias
        msg = s"${scala.Console.YELLOW} Assumes 1-N join for now: ${scala.Console.RESET}Max(leftParentSize,rightParentSize) = Max(" + leftParentES + "," + rightParentES + ") "
        Math.max(leftParentES, rightParentES) // TODO-GEN: Assumes 1-N: Make this explicit somehow
      case LeftHashSemiJoinOpNew(leftParent, rightParent, _, _, _) =>
        val leftParentES = analyzeQuery(leftParent)
        val rightParentES = analyzeQuery(rightParent)
        msg = s"${scala.Console.YELLOW}Assumes 100% selectivity for now (i.e. that all tuples of leftParent are returned).${scala.Console.RESET}"
        leftParentES
      case HashJoinAntiNew(leftParent, rightParent, _, _, _) =>
        val leftParentES = analyzeQuery(leftParent)
        val rightParentES = analyzeQuery(rightParent)
        msg = s"${scala.Console.YELLOW}Assumes 100% selectivity for now (i.e. that all tuples of leftParent are returned).${scala.Console.RESET}"
        leftParentES
      case MergeJoinOpNew(leftParent, rightParent, _) =>
        val leftParentES = analyzeQuery(leftParent)
        val rightParentES = analyzeQuery(rightParent)
        msg = s"${scala.Console.YELLOW}Assumes 100% selectivity for now (i.e. that all tuples of leftParent are returned).${scala.Console.RESET}"
        leftParentES
      //case other @ _ => System.out.println(other.asInstanceOf[Rep[Any]].correspondingNode)
    }
    val sb = new StringBuilder
    sb ++= ("QUERY ANALYZER: " + op.correspondingNode.nodeName)
    if (typeInfo != null) sb ++= ("(" + typeInfo + ")")
    sb ++= (" Estimated Size is = " + estimatedSize)
    if (msg != null) sb ++= (" (" + msg + ")")
    sb ++= "."
    logger.debug(sb.toString)

    estimatedSize
  }

  analysis += statement {
    case sym -> PrintOpNew(parent, _, _) if !analyzed =>
      analyzeQuery(sym);
      analyzed = true;
  }
}