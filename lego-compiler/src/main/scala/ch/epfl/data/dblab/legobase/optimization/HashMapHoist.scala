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
import sc.pardis.utils.Graph

/**
 * Factory for creating instances of [[HashMapHoist]]
 */
object HashMapHoist extends TransformerHandler {
  def apply[Lang <: Base, T: PardisType](context: Lang)(block: context.Block[T]): context.Block[T] = {
    new HashMapHoist(context.asInstanceOf[LoweringLegoBase]).optimize(block)
  }
}

// TODO consider rewriting it using the new transformation framework.
/**
 * Hoists `new HashMap`s and `new MultiMap`s from the query processing part
 * into the loading time.
 *
 * Example:
 * {{{
 *    // Loading Time
 *    loadTables()
 *    // Query Processing Time
 *    val hm = new HashMap
 *    val mm = new MultiMap
 *    processQuery()
 * }}}
 * is converted to:
 * {{{
 *    // Loading Time
 *    loadTables()
 *    val hm = new HashMap
 *    val mm = new MultiMap
 *    // Query Processing Time
 *    processQuery()
 * }}}
 *
 * @param IR the polymorphic embedding trait which contains the reified program.
 */
class HashMapHoist(override val IR: LoweringLegoBase) extends Optimizer[LoweringLegoBase](IR) {
  import IR._
  import CNodes._
  import CTypes._

  /**
   * First, in the analysis phase, it collects the statements constructing a HashMap
   * or MultiMap. Furthermore, it looks for the dependent statements, since hoisting
   * statements without hoisting the dependent statements makes the program incorrect.
   * Second, all hoisted statements are scheduled in the right order.
   * Finally, the scheduled statements are moved to the loading part and are removed
   * from the query processing time.
   */
  def optimize[T: TypeRep](node: Block[T]): to.Block[T] = {
    do {
      newStatementHoisted = false
      traverseBlock(node)
    } while (newStatementHoisted)
    scheduleHoistedStatements()
    transformProgram(node)
  }

  var startCollecting = false
  var newStatementHoisted = false
  /**
   * Specifies the nesting level of the statements that we are traversing over.
   */
  var depthLevel = 0
  val hoistedStatements = collection.mutable.ArrayBuffer[Stm[Any]]()
  // TODO check if we can remove this one?
  val currentHoistedStatements = collection.mutable.ArrayBuffer[Stm[Any]]()
  /**
   * Contains the list of symbols that we should find their dependency in the next
   * analysis iteration.
   */
  val workList = collection.mutable.Set[Sym[Any]]()

  /**
   * Schedules the statments that should be hoisted.
   */
  def scheduleHoistedStatements() {
    val result = Graph.schedule(hoistedStatements.toList, (stm1: Stm[Any], stm2: Stm[Any]) =>
      getDependencies(stm2.rhs).contains(stm1.sym))
    hoistedStatements.clear()
    hoistedStatements ++= result
  }

  /**
   * Returns the symbols that the given definition is dependent on them.
   */
  def getDependencies(node: Def[_]): List[Sym[Any]] =
    node.funArgs.filter(_.isInstanceOf[Sym[Any]]).map(_.asInstanceOf[Sym[Any]])

  override def traverseDef(node: Def[_]): Unit = node match {
    case GenericEngineRunQueryObject(b) => {
      startCollecting = true
      depthLevel = 0
      currentHoistedStatements.clear()
      traverseBlock(b)
      hoistedStatements.prependAll(currentHoistedStatements)
      startCollecting = false
    }
    case _ => super.traverseDef(node)
  }

  override def traverseBlock(block: Block[_]): Unit = {
    depthLevel += 1
    super.traverseBlock(block)
    depthLevel -= 1
  }

  /**
   * Gathers the statements that should be hoisted.
   */
  override def traverseStm(stm: Stm[_]): Unit = stm match {
    case Stm(sym, rhs) => {
      def hoistStatement() {
        currentHoistedStatements += stm.asInstanceOf[Stm[Any]]
        workList ++= getDependencies(rhs)
        newStatementHoisted = true
      }
      rhs match {
        case HashMapNew() if startCollecting && !hoistedStatements.contains(stm) => {
          hoistStatement()
        }
        case MultiMapNew() if startCollecting && !hoistedStatements.contains(stm) => {
          hoistStatement()
        }
        case ArrayNew(_) if startCollecting && depthLevel == 1 && !hoistedStatements.contains(stm) => {
          hoistStatement()
        }
        case _ if startCollecting && workList.contains(sym) && !hoistedStatements.contains(stm) => {
          hoistStatement()
          workList -= sym
        }
        case _ => super.traverseStm(stm)
      }
    }
  }

  /**
   * Removes the hoisted statements from the query processing time.
   */
  override def transformStmToMultiple(stm: Stm[_]): List[to.Stm[_]] =
    if (hoistedStatements.contains(stm.asInstanceOf[Stm[Any]]))
      Nil
    else
      super.transformStmToMultiple(stm)

  /**
   * Reifies the hoisted statements in the loading time.
   */
  override def transformDef[T: PardisType](node: Def[T]): to.Def[T] = (node match {
    case GenericEngineRunQueryObject(b) =>
      for (stm <- hoistedStatements) {
        reflectStm(stm)
      }
      val newBlock = transformBlock(b)
      GenericEngineRunQueryObject(newBlock)(newBlock.tp)
    case _ => super.transformDef(node)
  }).asInstanceOf[to.Def[T]]
}
