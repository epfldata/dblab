package ch.epfl.data
package legobase
package optimization

import scala.language.implicitConversions
import pardis.ir._
import reflect.runtime.universe.{ TypeTag, Type }
import pardis.optimization._
import deep._
import ch.epfl.data.pardis.ir.pardisTypeImplicits._

/**
 *  Transforms `malloc`s inside the part which runs the query into buffers which are allocated
 *  at the loading time.
 */
class MemoryManagementTransfomer(override val IR: LoweringLegoBase) extends Optimizer[LoweringLegoBase](IR) {
  import IR._
  import CNodes._
  import CTypes._

  /* If you want to disable this optimization, set this flag to `false` */
  val enabled = true

  def optimize[T: TypeRep](node: Block[T]): to.Block[T] = {
    traverseBlock(node)
    transformProgram(node)
  }

  var startCollecting = false
  val mallocNodes = collection.mutable.ArrayBuffer[Malloc[Any]]()
  case class BufferInfo(pool: Sym[Any], index: Var[Int])
  case class MallocInstance(tp: PardisType[Any], node: Malloc[Any])
  val mallocBuffers = collection.mutable.Map[MallocInstance, BufferInfo]()

  override def traverseDef(node: Def[_]): Unit = node match {
    case GenericEngineRunQueryObject(b) => {
      startCollecting = enabled
      traverseBlock(b)
      startCollecting = false
    }
    case Malloc(numElems) if startCollecting => {
      mallocNodes += node.asInstanceOf[Malloc[Any]]
    }
    case _ => super.traverseDef(node)
  }

  val POOL_SIZE = 12000000

  def cForLoop(start: Int, end: Int, f: Rep[Int] => Rep[Unit]) {
    val index = __newVar[Int](unit(start))
    __whileDo(readVar(index) < unit(end), {
      f(readVar(index))
      __assign(index, readVar(index) + unit(1))
    })
  }

  def mallocToInstance(node: Malloc[Any]): MallocInstance = MallocInstance(node.typeT, node)

  def createBuffers() {
    System.out.println("Creating buffers for mallocNodes: " + mallocNodes.mkString("\n"))
    System.out.println("")
    val mallocInstances = mallocNodes.map(m => mallocToInstance(m)).distinct.filter(t => !t.tp.name.contains("CArray") /* && !t.tp.name.contains("Pointer")*/ )
    for (mallocInstance <- mallocInstances) {
      val mallocTp = mallocInstance.tp
      System.out.println("Type is: " + mallocTp)
      val mallocNode = mallocInstance.node
      //	 val elemTp = mallocTp.typeArguments(0)
      val index = __newVar[Int](unit(0))
      val elemType = mallocTp
      val poolType = typePointer(elemType)
      System.out.println("Result types: " + elemType + " / " + poolType)
      val pool = malloc(unit(POOL_SIZE))(poolType)
      cForLoop(0, POOL_SIZE, (i: Rep[Int]) => {
        val allocatedSpace = malloc(unit(1))(elemType)
        // arrayUpdate(pool.asInstanceOf[Expression[Array[Any]]], i, allocatedSpace)(typePointer(mallocTp).asInstanceOf[PardisType[Any]])
        // val currentPool = infix_asInstanceOf(pool.asInstanceOf[Rep[Int]] + i)(pool.tp)
        // pointer_assign(currentPool.asInstanceOf[Expression[Pointer[Any]]], allocatedSpace)
        pointer_assign(pool.asInstanceOf[Expression[Pointer[Any]]], i, allocatedSpace)
        unit(())
      })
      mallocBuffers += mallocInstance -> BufferInfo(pool.asInstanceOf[Sym[Any]], index)
      // printf(unit("A buffer for tp " + mallocTp + " size " + mallocNode.numElems + "\n"))
    }
  }

  override def transformExp[T: TypeRep, S: TypeRep](exp: Rep[T]): Rep[S] = exp match {
    case t: typeOf[_] => typeOf()(apply(t.tp)).asInstanceOf[Rep[S]]
    case _            => super.transformExp[T, S](exp)
  }

  override def transformDef[T: PardisType](node: Def[T]): to.Def[T] = (node match {
    // Profiling and utils functions mapping
    case GenericEngineRunQueryObject(b) =>
      createBuffers()
      val diff = readVar(__newVar[TimeVal](PardisCast[Int, TimeVal](unit(0))))
      val start = readVar(__newVar[TimeVal](PardisCast[Int, TimeVal](unit(0))))
      val end = readVar(__newVar[TimeVal](PardisCast[Int, TimeVal](unit(0))))
      gettimeofday(&(start))
      startCollecting = enabled
      toAtom(transformBlock(b))
      startCollecting = false
      gettimeofday(&(end))
      val tm = timeval_subtract(&(diff), &(end), &(start))
      Printf(unit("Generated code run in %ld milliseconds."), tm)
    case m @ Malloc(numElems) if startCollecting && !m.tp.name.contains("CArray") /* && !m.tp.name.contains("Pointer") */ => {
      val mallocInstance = mallocToInstance(m)
      val bufferInfo = mallocBuffers(mallocInstance)
      val p = pointer_content(bufferInfo.pool.asInstanceOf[Rep[Pointer[Any]]], readVar(bufferInfo.index))(m.tp.asInstanceOf[PardisType[Any]])
      __assign(bufferInfo.index, readVar(bufferInfo.index) + unit(1))
      // printf(unit("should be substituted by " + bufferInfo.pool + ", " + bufferInfo.index))
      ReadVal(p)
    }
    case _ => super.transformDef(node)
  }).asInstanceOf[to.Def[T]]
}
