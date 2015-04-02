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

class LargeOutputPrintHoister(override val IR: LoweringLegoBase) extends RuleBasedTransformer[LoweringLegoBase](IR) {
  import IR._

  rewrite += rule {
    case GenericEngineRunQueryObject(b) =>
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