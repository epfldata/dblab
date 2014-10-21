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
import pardis.shallow.utils._

/**
 *  Transforms `malloc`s inside the part which runs the query into buffers which are allocated
 *  at the loading time.
 */
class MemoryManagementTransfomer(override val IR: LoweringLegoBase) extends Optimizer[LoweringLegoBase](IR) {
  import IR._
  //import CNodes._
  //import CTypes._

  /* If you want to disable this optimization, set this flag to `false` */
  val enabled = true

  def optimize[T: TypeRep](node: Block[T]): to.Block[T] = {
    phase = FindMallocs
    traverseBlock(node)
    phase = FindSize
    traverseBlock(node)
    transformProgram(node)
  }

  sealed trait Phase
  case object FindMallocs extends Phase
  case object FindSize extends Phase

  var phase: Phase = _

  var startCollecting = false
  //val mallocNodes = collection.mutable.ArrayBuffer[PardisStruct[Any]]()
  val mallocNodes = collection.mutable.ArrayBuffer[Def[Any]]()
  case class BufferInfo(pool: Sym[Any], index: Var[Int])
  case class MallocInstance(tp: PardisType[Any], node: Def[Any])
  val mallocBuffers = collection.mutable.Map[MallocInstance, BufferInfo]()

  override def traverseDef(node: Def[_]): Unit = node match {
    case GenericEngineRunQueryObject(b) => {
      startCollecting = enabled
      traverseBlock(b)
      startCollecting = false
    }
    case m @ PardisStruct(tag, elems, methods) if startCollecting && phase == FindMallocs => {
      mallocNodes += node.asInstanceOf[Def[Any]]
    }
    case m @ ArrayNew(size) if startCollecting && phase == FindMallocs => {
      mallocNodes += node.asInstanceOf[Def[Any]]
    }
    case _ => super.traverseDef(node)
  }

  def cForLoop(start: Int, end: Rep[Int], f: Rep[Int] => Rep[Unit]) {
    val index = __newVar[Int](unit(start))
    __whileDo(readVar(index) < end, {
      f(readVar(index))
      __assign(index, readVar(index) + unit(1))
    })
  }

  def mallocToInstance(node: Def[Any]): MallocInstance = MallocInstance(node.tp, node)

  def createBuffers() {
    //System.out.println("Creating buffers for mallocNodes: " + mallocNodes.mkString("\n"))
    val mallocInstances = mallocNodes.map(m => mallocToInstance(m)).distinct //.filter(t => !t.tp.name.contains("CArray") /* && !t.tp.name.contains("Pointer")*/ )
    for (mallocInstance <- mallocInstances) {
      val mallocTp = mallocInstance.tp
      //	 val elemTp = mallocTp.typeArguments(0)
      val index = __newVar[Int](unit(0))
      val elemType = mallocTp
      val poolType = elemType //if (mallocTp.isPrimitive) elemType else typePointer(elemType)
      val POOL_SIZE = 12000000 * (poolType.toString.split("_").length + 1)
      //val POOL_SIZE = 100000
      /* this one is a hack */
      /*def regenerateSize(s: Rep[Int]): Rep[Int] = s match {
        case c @ Constant(_)           =>  s
        case Def(Int$div4(x, y))   => regenerateSize(x) / regenerateSize(y)
        case Def(Int$times4(x, y)) => regenerateSize(x) * regenerateSize(y)
        case d @ Def(_)            => s
      }
      val POOL_SIZE = regenerateSize(mallocNode.numElems) * 200*/
      val pool = arrayNew(POOL_SIZE)(poolType) //malloc(POOL_SIZE)(poolType)
      System.out.println("pool.tp = " + pool.tp)
      if (!mallocTp.isPrimitive) {
        cForLoop(0, POOL_SIZE, (i: Rep[Int]) => {
          if (poolType.isRecord) {
            val mallocNode = mallocInstance.node.asInstanceOf[PardisStruct[Any]]
            val newElems = mallocNode.elems.map(e => {
              val in = {
                if ((e.init.tp.isRecord) || (e.init.tp.isArray)) {
                  val other = mallocBuffers.find(mb => mb._1.tp.name == e.init.tp.name).get._2.pool
                  arrayApply(other.asInstanceOf[Expression[Array[Any]]], i)(e.init.tp)
                } else infix_asInstanceOf(unit(DefaultValue(e.init.tp.name))(e.init.tp))(e.init.tp)
              }
              PardisStructArg(e.name, e.mutable, in)
            })
            val allocatedSpace = toAtom(PardisStruct(mallocNode.tag, newElems, mallocNode.methods)(elemType))(elemType)
            arrayUpdate(pool, i, allocatedSpace)
          } else if (poolType.isArray) {
            val mallocNode = mallocInstance.node.asInstanceOf[ArrayNew[Any]]
            val newType = poolType
            System.out.println("newtype = " + newType)
            val allocatedSpace = toAtom(ArrayNew(mallocNode._length)(newType))(typeArray(poolType))
            arrayUpdate(pool, i, allocatedSpace)
          }
          //malloc(unit(1))(elemType)
          //pointer_assign(pool.asInstanceOf[Expression[Pointer[Any]]], i, allocatedSpace)
          unit(())
        })
      }
      mallocBuffers += mallocInstance -> BufferInfo(pool.asInstanceOf[Sym[Any]], index)
      //printf(unit("Buffer for type %s of size %d initialized!\n"), unit(mallocTp.toString), POOL_SIZE)
    }
  }

  /*override def transformExp[T: TypeRep, S: TypeRep](exp: Rep[T]): Rep[S] = exp match {
    case t: typeOf[_] => typeOf()(apply(t.tp)).asInstanceOf[Rep[S]]
    case _            => super.transformExp[T, S](exp)
  }*/

  override def transformDef[T: PardisType](node: Def[T]): to.Def[T] = (node match {
    // Profiling and utils functions mapping
    case GenericEngineRunQueryObject(b) =>
      createBuffers()
      //Console.err.printf(unit("Initializing LegoBase buffers (this may take a long time...)\n"))
      //createBuffers()
      //Console.err.printf(unit("DONE. Now running query\n"))
      //val diff = readVar(__newVar[TimeVal](PardisCast[Int, TimeVal](unit(0))))
      //val start = readVar(__newVar[TimeVal](PardisCast[Int, TimeVal](unit(0))))
      //val end = readVar(__newVar[TimeVal](PardisCast[Int, TimeVal](unit(0))))
      //gettimeofday(&(start))
      startCollecting = enabled
      val tb = transformBlock(b)
      startCollecting = false
      //gettimeofday(&(end))
      //val tm = timeval_subtract(&(diff), &(end), &(start))
      //Printf(unit("Generated code run in %ld milliseconds.\n"), tm)
      GenericEngineRunQueryObject(tb)
    //case m @ Malloc(numElems) if startCollecting /* && !m.tp.name.contains("CArray")  && !m.tp.name.contains("Pointer") */ => {
    case ps @ ArrayNew(size) if startCollecting => {
      val mallocInstance = mallocToInstance(ps.asInstanceOf[Def[Any]])
      val bufferInfo = mallocBuffers(mallocInstance)
      val p = arrayApply(bufferInfo.pool.asInstanceOf[Rep[Array[Any]]], readVar(bufferInfo.index)(IntType))(ps.tp.asInstanceOf[PardisType[Any]])
      __assign(bufferInfo.index, readVar(bufferInfo.index)(IntType) + (1))
      // printf(unit("should be substituted by " + bufferInfo.pool + ", " + bufferInfo.index))
      ReadVal(p)(ps.tp.asInstanceOf[PardisType[Any]])
    }

    case ps @ PardisStruct(tag, elems, methods) if startCollecting => {
      val mallocInstance = mallocToInstance(ps.asInstanceOf[Def[Any]])
      val bufferInfo = mallocBuffers(mallocInstance)
      /*val p = {
        if (m.tp.typeArguments(0).isPrimitive) {
          &(bufferInfo.pool, readVar(bufferInfo.index))(m.tp.typeArguments(0).asInstanceOf[PardisType[Any]])
        } else pointer_content(
          bufferInfo.pool.asInstanceOf[Rep[Pointer[Any]]], readVar(bufferInfo.index))(m.tp.asInstanceOf[PardisType[Any]])
      }*/
      val p = arrayApply(bufferInfo.pool.asInstanceOf[Rep[Array[Any]]], readVar(bufferInfo.index)(IntType))(ps.tp.asInstanceOf[PardisType[Any]])
      __assign(bufferInfo.index, readVar(bufferInfo.index)(IntType) + (1))
      // printf(unit("should be substituted by " + bufferInfo.pool + ", " + bufferInfo.index))
      ReadVal(p)(ps.tp.asInstanceOf[PardisType[Any]])
    }
    case _ => super.transformDef(node)
  }).asInstanceOf[to.Def[T]]
}
