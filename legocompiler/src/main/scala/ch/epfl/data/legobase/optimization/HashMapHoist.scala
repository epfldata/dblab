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
    transformProgram(node)
  }

  var startCollecting = false
  var foundFlag = false
  val hoistedStatements = collection.mutable.ArrayBuffer[Stm[Any]]()
  val currentHoistedStatements = collection.mutable.ArrayBuffer[Stm[Any]]()
  val workList = collection.mutable.Set[Sym[Any]]()

  override def traverseDef(node: Def[_]): Unit = node match {
    case GenericEngineRunQueryObject(b) => {
      startCollecting = enabled
      currentHoistedStatements.clear()
      traverseBlock(b)
      hoistedStatements.prependAll(currentHoistedStatements)
      startCollecting = false
    }
    case _ => super.traverseDef(node)
  }

  override def traverseStm(stm: Stm[_]): Unit = stm match {
    case Stm(sym, rhs) => {
      def hoistStatement() {
        currentHoistedStatements += stm.asInstanceOf[Stm[Any]]
        workList ++= rhs.funArgs.filter(_.isInstanceOf[Sym[Any]]).map(_.asInstanceOf[Sym[Any]])
        foundFlag = true
      }
      rhs match {
        case HashMapNew3(hashFunc, size) if startCollecting && !hoistedStatements.contains(stm) => {
          hoistStatement()
        }
        case HashMapNew4(hashFunc, size) if startCollecting && !hoistedStatements.contains(stm) => {
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
      debugMsg(stderr, unit("New place for hash maps\n"))
      for (stm <- hoistedStatements) {
        reflectStm(stm)
      }
      startCollecting = enabled
      val newBlock = transformBlock(b)
      startCollecting = false
      GenericEngineRunQueryObject(newBlock)
    case _ => super.transformDef(node)
  }).asInstanceOf[to.Def[T]]
}
