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
 * Analyzes the query to gather additional statisti
 *
 * @param IR the polymorphic embedding trait which contains the reified program.
 * @param schema the schema over which the query runs. This already contains base
 * relations cardinalities and selectivities between two-way joins
 */
class StatisticsEstimator(override val IR: LoweringLegoBase, val schema: Schema) extends RuleBasedTransformer[LoweringLegoBase](IR) {
  import IR._

  override def optimize[T: TypeRep](node: Block[T]): to.Block[T] = {
    // QS stands for Query specific
    schema.stats += "QS_MEM_ARRAY_DOUBLE" -> 0
    traverseBlock(node)
    System.out.println(schema.stats.mkString("\n"))
    node
  }

  var analyzed: Boolean = false

  def analyzeQuery(op: Rep[Any]): Int = {
    var estimatedSize: Int = -1
    op.correspondingNode match {
      case PrintOpNew(parent, _, _) =>
        estimatedSize = analyzeQuery(parent)
        System.out.println("QUERY ANALYZER: PRINTOP Estimated Size = " + estimatedSize)
      case SelectOpNew(parent, _) =>
        // TODO-GEN: Calculate selectivities for SelectOP rather than blindly assuming 100%
        estimatedSize = analyzeQuery(parent)
        System.out.println("QUERY ANALYZER: SELECTOP Estimated Size = " + estimatedSize)
      case MapOpNew(parent, _) =>
        estimatedSize = analyzeQuery(parent)
        System.out.println("QUERY ANALYZER: MAPOP Estimated Size = " + estimatedSize)
      case scanOp @ ScanOpNew(_) =>
        estimatedSize = schema.stats.getCardinality(scanOp.typeA.toString).toInt
        System.out.println("QUERY ANALYZER: SCANOP(" + scanOp.typeA + ") ESTIMATED_SIZE = " + estimatedSize)
      case sortOp @ SortOpNew(parent, _) =>
        estimatedSize = analyzeQuery(parent)
        System.out.println("QUERY ANALYZER: SORTOP Estimated Size = " + estimatedSize)
      case ao @ AggOpNew(parent, _, grp, _) =>
        val parentES = analyzeQuery(parent)

        estimatedSize = grp match {
          case Def(PardisLambda(_, _, Block(b, Def(PardisStructImmutableField(f, name))))) =>
            val numDistinctVals = schema.stats.getDistinctAttrValues(name)
            schema.stats += "QS_MEM_ARRAY_DOUBLE" -> (schema.stats("QS_MEM_ARRAY_DOUBLE") + numDistinctVals)
            //TODO-GEN Refactor the following line so that it aggregates instead of just inserting (as above)
            schema.stats += ("QS_MEM_AGG_" + ao.typeB) -> numDistinctVals
            numDistinctVals.toInt
          case Def(PardisLambda(_, _, Block(b, Constant(string)))) =>
            schema.stats += ("QS_MEM_AGG_" + ao.typeB) -> 1
            schema.stats += "QS_MEM_ARRAY_DOUBLE" -> (schema.stats("QS_MEM_ARRAY_DOUBLE") + 1)
            parentES
          case Def(PardisLambda(_, _, Block(b, res))) if res.correspondingNode.isInstanceOf[ConstructorDef[_]] =>
            val structArgs = res.correspondingNode.asInstanceOf[ConstructorDef[_]].argss.flatten
            val structArgsCombinations = structArgs.foldLeft(1.0)((cnt, attr) => attr match {
              case Def(ifa: ImmutableFieldAccess[_]) => cnt * schema.stats.getDistinctAttrValues(ifa.field) // all possible combinations
            }).toInt
            // If parent sends less tuples that the estimated possible combinations, then choose the estimation of parent
            val numDistinctVals = Math.min(parentES, structArgsCombinations)

            // Estimate that the following number of keys and aggregate records will be created
            //TODO-GEN Refactor the following line so that it aggregates instead of just inserting (as above)
            schema.stats += ("QS_MEM_" + ao.typeB) -> parentES
            schema.stats += ("QS_MEM_AGG_" + ao.typeB) -> numDistinctVals
            schema.stats += "QS_MEM_ARRAY_DOUBLE" -> (schema.stats("QS_MEM_ARRAY_DOUBLE") + numDistinctVals)
            numDistinctVals.toInt
          case Def(PardisLambda(_, _, Block(b, res))) =>
            System.out.println(s"${scala.Console.RED}Warning${scala.Console.RESET}: Statistics for Aggregate Operator (grp = " + grp.correspondingNode + ") not accurate. This may lead to degraded performance due to unnecessarily large memory pool allocations. ")
            schema.stats += ("QS_MEM_AGG_" + ao.typeB) -> parentES // TODO-GEN: Make message better
            parentES
        }
        System.out.println("QUERY ANALYZER: AGGOP Estimated Size = " + estimatedSize)

      case LeftHashSemiJoinOpNew(leftParent, rightParent, _, _, _) =>
        val leftParentES = analyzeQuery(leftParent)
        val rightParentES = analyzeQuery(rightParent)
        estimatedSize = leftParentES
        System.out.print("QUERY ANALYZER: LEFTHASHSEMIJOINOP Estimated Size = " + estimatedSize)
        System.out.println(" (Left/Right parent sizes are: " + leftParentES + "," + rightParentES + ")")
      case NestedLoopsJoinOpNew(leftParent, rightParent, _, _, _) =>
        val leftParentES = analyzeQuery(leftParent)
        val rightParentES = analyzeQuery(rightParent)
        estimatedSize = Math.max(leftParentES, rightParentES)
        System.out.print("QUERY ANALYZER: NESTEDLOOPSJOINOP Estimated Size = " + estimatedSize)
        System.out.println(" (Left/Right parent sizes are: " + leftParentES + "," + rightParentES + ")")
      case HashJoinOpNew1(leftParent, rightParent, _, _, _, _, _) =>
        val leftParentES = analyzeQuery(leftParent)
        val rightParentES = analyzeQuery(rightParent)
        estimatedSize = Math.max(leftParentES, rightParentES)
        System.out.print("QUERY ANALYZER: HASHJOINOP1 Estimated Size = " + estimatedSize)
        System.out.println(" (Left/Right parent sizes are: " + leftParentES + "," + rightParentES + ")")
      //case HashJoinOpNew2(parent1, parent2, _, _, _) =>
      //System.out.println("QUERY ANALYZER: HASHJOINOP2 Estimated Size = " + analyzeQuery(parent2))
      case HashJoinAntiNew(leftParent, rightParent, _, _, _) =>
        val leftParentES = analyzeQuery(leftParent)
        val rightParentES = analyzeQuery(rightParent)
        estimatedSize = leftParentES //Assume that no tuple is being matched
        System.out.print("QUERY ANALYZER: HASHJOINANTI Estimated Size = " + estimatedSize)
        System.out.println(" (Left/Right parent sizes are: " + leftParentES + "," + rightParentES + ")")
      //case other @ _ => System.out.println(other.asInstanceOf[Rep[Any]].correspondingNode)
    }
    estimatedSize;
  }

  analysis += rule {
    case po @ PrintOpNew(parent, _, _) if !analyzed =>
      analyzeQuery(po);
      analyzed = true;
  }
}