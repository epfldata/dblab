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
    scheduleHoistedStatements()
    System.out.println(s">>>final:${hoistedStatements.mkString("\n")}")
    transformProgram(node)
  }

  var startCollecting = false
  var foundFlag = false
  val hoistedStatements = collection.mutable.ArrayBuffer[Stm[Any]]()
  val currentHoistedStatements = collection.mutable.ArrayBuffer[Stm[Any]]()
  val workList = collection.mutable.Set[Sym[Any]]()

  /* Adopted from https://gist.github.com/ThiporKong/4399695 */
  private def tsort[A](edges: Traversable[(A, A)]): Iterable[A] = {
    import scala.collection.immutable.Set
    @scala.annotation.tailrec
    def tsort(toPreds: Map[A, Set[A]], done: Iterable[A]): Iterable[A] = {
      val (noPreds, hasPreds) = toPreds.partition { _._2.isEmpty }
      if (noPreds.isEmpty) {
        if (hasPreds.isEmpty) done else sys.error(hasPreds.toString)
      } else {
        val found = noPreds.map { _._1 }
        tsort(hasPreds.mapValues { _ -- found }, done ++ found)
      }
    }

    val toPred = edges.foldLeft(Map[A, Set[A]]()) { (acc, e) =>
      acc + (e._1 -> acc.getOrElse(e._1, Set())) + (e._2 -> (acc.getOrElse(e._2, Set()) + e._1))
    }
    tsort(toPred, Seq())
  }

  def scheduleHoistedStatements() {
    val dependenceGraphEdges = for (stm1 <- hoistedStatements; stm2 <- hoistedStatements if (stm1 != stm2 && getDependencies(stm1.rhs).contains(stm2.sym))) yield (stm2 -> stm1)
    hoistedStatements.clear()
    hoistedStatements ++= tsort(dependenceGraphEdges)
  }

  def getDependencies(node: Def[_]): List[Sym[Any]] = node.funArgs.filter(_.isInstanceOf[Sym[Any]]).map(_.asInstanceOf[Sym[Any]])

  override def traverseDef(node: Def[_]): Unit = node match {
    case GenericEngineRunQueryObject(b) => {
      startCollecting = enabled
      currentHoistedStatements.clear()
      traverseBlock(b)
      hoistedStatements.prependAll(currentHoistedStatements)
      System.out.println(s">>>added:${currentHoistedStatements.mkString("\n")}")
      startCollecting = false
    }
    case _ => super.traverseDef(node)
  }

  override def traverseStm(stm: Stm[_]): Unit = stm match {
    case Stm(sym, rhs) => {
      def hoistStatement() {
        currentHoistedStatements += stm.asInstanceOf[Stm[Any]]
        // workList ++= rhs.funArgs.filter(_.isInstanceOf[Sym[Any]]).map(_.asInstanceOf[Sym[Any]])
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
