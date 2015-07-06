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

/**
 * In the cases that printing the result output is very time consuming, this transformer
 * puts the printing part outside of the query processing timing.
 */
class LargeOutputPrintHoister(override val IR: LoweringLegoBase, val schema: Schema) extends RuleBasedTransformer[LoweringLegoBase](IR) {
  import IR._

  val OUTPUT_THREASHOLD = 8192

  rewrite += rule {
    case GenericEngineRunQueryObject(b) if schema.stats.getOutputSizeEstimation() > OUTPUT_THREASHOLD =>
      System.out.println(s"${scala.Console.GREEN}Info${scala.Console.RESET}: Query has been estimated with large output size of " + schema.stats.getOutputSizeEstimation() + " tuples (or a more precise estimation was not possible to do). Hoisting out printing of query results from total execution time.")
      // TODO: Isn't this always the case? If yes, then shouldn't it be removed?
      if (b.tp != UnitType) {
        throw new Exception(s"The runQuery block must have a Unit type but its type is ${b.tp}")
      }
      /* find the index of the last while loop which contains the printf operation */
      val endStatementIndex = b.stmts.lastIndexWhere(stm => stm.rhs.isInstanceOf[While])
      val runQueryBlock = Block(b.stmts.take(endStatementIndex), unit(()))
      GenericEngine.runQuery(inlineBlock(runQueryBlock))
      b.stmts.drop(endStatementIndex).foreach(transformStm)
      unit(())
  }
}