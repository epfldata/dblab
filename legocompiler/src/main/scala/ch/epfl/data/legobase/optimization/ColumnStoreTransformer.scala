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

class ColumnStoreTransformer(override val IR: LoweringLegoBase) extends Optimizer[LoweringLegoBase](IR) with StructCollector[LoweringLegoBase] {
  import IR._
  import CNodes._
  import CTypes._

  def optimize[T: TypeRep](node: Block[T]): to.Block[T] = {
    traverseBlock(node)
    transformProgram(node)
  }

  override def traverseDef(node: Def[_]): Unit = node match {
    case _ => super.traverseDef(node)
  }

  override def transformDef[T: PardisType](node: Def[T]): to.Def[T] = (node match {
    case _ => super.transformDef(node)
  }).asInstanceOf[to.Def[T]]
}