package ch.epfl.data
package dblab.legobase
package optimization
package monad

import scala.language.implicitConversions
import sc.pardis.ir._
import reflect.runtime.universe.{ TypeTag, Type }
import sc.pardis.optimization._
import deep._
import sc.pardis.types._
import sc.pardis.types.PardisTypeImplicits._
import sc.pardis.shallow.utils.DefaultValue
import compiler._
import sc.pardis.utils.Graph

class QueryMonadHoisting(override val IR: LegoBaseExp) extends Optimizer[LegoBaseExp](IR) {
  import IR._

  /**
   * First, in the analysis phase, it collects the statements constructing a groupBy.
   * Furthermore, it looks for the dependent statements, since hoisting
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
  def getDependencies(node: Def[_]): List[Sym[Any]] = {
    val allArgs = node.funArgs ++ {
      node match {
        case x: PardisLambdaDef => x.body.stmts.flatMap(x => x.sym :: x.rhs.funArgs)
        case b: Block[_]        => b.stmts.flatMap(x => x.sym :: x.rhs.funArgs)
        case _                  => Nil
      }
    }
    allArgs.filter(_.isInstanceOf[Sym[Any]]).map(_.asInstanceOf[Sym[Any]])
  }

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
        case QueryGroupBy(_, _) | QueryFilteredGroupBy(_, _, _) if startCollecting && !hoistedStatements.contains(stm) => {
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
