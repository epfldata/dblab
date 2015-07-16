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
import sc.pardis.shallow.utils._
import sc.pardis.ir.StructTags._

/**
 * Analyzes the query to gather additional statistics.
 *
 * @param IR the polymorphic embedding trait which contains the reified program.
 * @param schema the schema over which the query runs. This already contains base
 * relations cardinalities and selectivities between two-way joins
 */
class StatisticsEstimator(override val IR: LegoBaseExp, val schema: Schema) extends RuleBasedTransformer[LegoBaseExp](IR) {
  import IR._

  override def optimize[T: TypeRep](node: Block[T]): to.Block[T] = {
    schema.stats.removeQuerySpecificStats()
    traverseBlock(node)
    System.out.println(schema.stats.mkString("\n"))
    node
  }

  var analyzed: Boolean = false
  val aliasesList = new scala.collection.mutable.ListBuffer[String]()

  def getUnaliasedStructFieldName(fieldName: String) = {
    aliasesList.foldLeft(fieldName)((fn, alias) => {
      fn.replaceAll(alias, "")
    })
  }

  def estimateCardinalityFromExpression(expr: Rep[Any], defaultEstimation: Int): Int = expr match {
    case Def(GenericEngineDateToYearObject(_)) => schema.stats.getNumYearsAllDates()
    case _ =>
      System.out.println(s"${scala.Console.RED}Warning${scala.Console.RESET}: Statistics Estimator cannot accurately estimate cardinality for node " + expr.correspondingNode + ". Returning default estimation. This may lead to degraded performance due to unnecessarily large memory pool allocations. ")
      defaultEstimation
  }

  def estimateCardinalityFromFunction(fun: Rep[Any], defaultEstimation: Int): Int = {
    fun match {
      case Def(PardisLambda(_, _, Block(b, Constant(string)))) =>
        1
      case Def(PardisLambda(_, _, Block(b, Def(PardisStructImmutableField(f, name))))) =>
        schema.stats.getDistinctAttrValues(name).toInt
      // TODO-GEN: This is here, because the PardisStructImmutableField does not capture all field accesses. Is this a BUG or intended?
      case Def(PardisLambda(_, _, Block(b, res))) if res.correspondingNode.isInstanceOf[FieldDef[_]] =>
        val fieldName = res.correspondingNode.asInstanceOf[FieldDef[_]].field
        schema.stats.getDistinctAttrValues(fieldName).toInt
      case Def(PardisLambda(_, _, Block(b, res))) if res.correspondingNode.isInstanceOf[ConstructorDef[_]] =>
        val structArgs = res.correspondingNode.asInstanceOf[ConstructorDef[_]].argss.flatten
        val structArgsCombinations = structArgs.foldLeft(1.0)((cnt, attr) => attr match {
          // basically estimate all possible combinations
          case Def(ifa: ImmutableFieldAccess[_]) => cnt * schema.stats.getDistinctAttrValues(getUnaliasedStructFieldName(ifa.field))
          case _                                 => cnt * estimateCardinalityFromExpression(attr.asInstanceOf[Expression[_]], defaultEstimation) // account for an expression
        }).toInt

        // If parent sends less tuples that the estimated possible combinations, then choose the estimation of parent
        Math.min(defaultEstimation, structArgsCombinations).toInt
      case Def(PardisLambda(_, _, Block(b, res))) =>
        estimateCardinalityFromExpression(res, defaultEstimation)
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
        val numDistinctVals = estimateCardinalityFromFunction(grp, parentES)

        // Handle additional estimations per function type
        grp match {
          case Def(PardisLambda(_, _, Block(b, Constant(string))))                                       =>
          case Def(PardisLambda(_, _, Block(b, Def(PardisStructImmutableField(f, name)))))               =>
          case Def(PardisLambda(_, _, Block(b, res))) if res.correspondingNode.isInstanceOf[FieldDef[_]] =>
          case Def(PardisLambda(_, _, Block(b, res))) if res.correspondingNode.isInstanceOf[ConstructorDef[_]] =>
            // In this case, the key is a record itself. In this case, estimate how many such keys will be created
            schema.stats increase ("QS_MEM_" + ao.typeB) -> parentES // Because we will create that many records independent of the output size
          case Def(PardisLambda(_, _, Block(b, res))) =>
        }

        // Estimate that the following number of aggregate records and aggregate arrays will be created
        schema.stats increase ("QS_MEM_AGG_" + ao.typeB) -> numDistinctVals
        schema.stats increase "QS_MEM_ARRAY_DOUBLE" -> numDistinctVals
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
      //case other @ _ => System.out.println(other.asInstanceOf[Rep[Any]].correspondingNode)
    }

    System.out.print("QUERY ANALYZER: " + op.correspondingNode.nodeName)
    if (typeInfo != null) System.out.print("(" + typeInfo + ")")
    System.out.print(" Estimated Size is = " + estimatedSize)
    if (msg != null) System.out.print(" (" + msg + ")")
    System.out.println(".")

    estimatedSize;
  }

  analysis += rule {
    case po @ PrintOpNew(parent, _, _) if !analyzed =>
      analyzeQuery(po);
      analyzed = true;
  }
}