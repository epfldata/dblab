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

object HashMapHoist extends TransformerHandler {
  def apply[Lang <: Base, T: PardisType](context: Lang)(block: context.Block[T]): context.Block[T] = {
    new HashMapHoist(context.asInstanceOf[LoweringLegoBase]).optimize(block)
  }
}

/**
 *  Transforms `new HashMap`s inside the part which runs the query into buffers which are allocated
 *  at the loading time.
 */
class HashMapHoist(override val IR: LoweringLegoBase) extends Optimizer[LoweringLegoBase](IR) {
  import IR._
  import CNodes._
  import CTypes._

  /* If you want to disable this optimization, set this flag to `false` */
  val enabled = true

  def optimize[T: TypeRep](node: Block[T]): to.Block[T] = {
    do {
      foundFlag = false
      traverseBlock(node)
    } while (foundFlag)
    // System.out.println(s"all hoisted stms before: $hoistedStatements")
    scheduleHoistedStatements()
    // System.out.println(s"all hoisted stms after: $hoistedStatements")
    transformProgram(node)
  }

  var startCollecting = false
  var foundFlag = false
  // specifies the depth level statement with respect to the source anchor
  var depthLevel = 0
  val hoistedStatements = collection.mutable.ArrayBuffer[Stm[Any]]()
  val currentHoistedStatements = collection.mutable.ArrayBuffer[Stm[Any]]()
  val workList = collection.mutable.Set[Sym[Any]]()

  def scheduleHoistedStatements() {
    // TODO it's not generic, for the statements with nodes without children it does not work
    // if (!hoistedStatements.forall(stm => stm.rhs.funArgs.isEmpty)) {
    //   val dependenceGraphEdges = for (stm1 <- hoistedStatements; stm2 <- hoistedStatements if (stm1 != stm2 && getDependencies(stm1.rhs).contains(stm2.sym))) yield (stm2 -> stm1)
    //   hoistedStatements.clear()
    //   hoistedStatements ++= pardis.utils.Graph.tsort(dependenceGraphEdges)
    // }
    val result = pardis.utils.Graph.schedule(hoistedStatements.toList, (stm1: Stm[Any], stm2: Stm[Any]) => getDependencies(stm1.rhs).contains(stm2.sym))
    hoistedStatements.clear()
    hoistedStatements ++= result
  }

  def getDependencies(node: Def[_]): List[Sym[Any]] = node.funArgs.filter(_.isInstanceOf[Sym[Any]]).map(_.asInstanceOf[Sym[Any]])

  override def traverseDef(node: Def[_]): Unit = node match {
    case GenericEngineRunQueryObject(b) => {
      startCollecting = enabled
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

  override def traverseStm(stm: Stm[_]): Unit = stm match {
    case Stm(sym, rhs) => {
      def hoistStatement() {
        currentHoistedStatements += stm.asInstanceOf[Stm[Any]]
        workList ++= getDependencies(rhs)
        foundFlag = true
      }
      rhs match {
        case HashMapNew3(hashFunc, size) if startCollecting && !hoistedStatements.contains(stm) => {
          hoistStatement()
        }
        case HashMapNew4(hashFunc, size) if startCollecting && !hoistedStatements.contains(stm) => {
          hoistStatement()
        }
        case HashMapNew() if startCollecting && !hoistedStatements.contains(stm) => {
          // System.out.println("hm new found!")
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

  override def transformStmToMultiple(stm: Stm[_]): List[to.Stm[_]] = if (hoistedStatements.contains(stm.asInstanceOf[Stm[Any]])) Nil else super.transformStmToMultiple(stm)

  override def transformDef[T: PardisType](node: Def[T]): to.Def[T] = (node match {
    // Profiling and utils functions mapping
    case GenericEngineRunQueryObject(b) =>
      //Console.err.printf(unit("New place for hash maps\n"))
      for (stm <- hoistedStatements) {
        // System.out.println(s"reflecting $stm!")
        reflectStm(stm)
      }
      startCollecting = enabled
      val newBlock = transformBlock(b)
      startCollecting = false
      GenericEngineRunQueryObject(newBlock)
    case _ => super.transformDef(node)
  }).asInstanceOf[to.Def[T]]
}
