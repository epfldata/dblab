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

object MemoryManagementTransfomer extends TransformerHandler {
  def apply[Lang <: Base, T: PardisType](context: Lang)(block: context.Block[T]): context.Block[T] = {
    new MemoryManagementTransfomer(context.asInstanceOf[LoweringLegoBase]).optimize(block)
  }
}

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
      /*System.out.println("---------------------")
      System.out.println("INSERTING STRUCT: " + node.tp + " TO LIST ")
      System.out.println(mallocNodes.map(mn => mn.tp).mkString("\n"))
      System.out.println("CONTAINED? " + mallocNodes.find(mn => mn.tp == node.tp).isEmpty)
      System.out.println("---------------------\n")
      if (mallocNodes.find(mn => mn.tp == node.tp).isEmpty)*/
      mallocNodes += node.asInstanceOf[Def[Any]]
    }
    case m @ ArrayNew(size) if startCollecting && phase == FindMallocs => {
      /*System.out.println("---------------------")
      System.out.println("INSERTING ARRAy: " + node.tp + " TO LIST ")
      System.out.println(mallocNodes.map(mn => mn.tp).mkString("\n"))
      System.out.println("CONTAINED? " + mallocNodes.find(mn => mn.tp == node.tp).isEmpty)
      System.out.println("---------------------\n")
      if (mallocNodes.find(mn => mn.tp == node.tp).isEmpty)*/
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
    val mallocInstances = mallocNodes.map(m => mallocToInstance(m)) //.sortBy(ll => ll.tp.name.length) //.distinct //.filter(t => !t.tp.name.contains("CArray") /* && !t.tp.name.contains("Pointer")*/ )
    System.out.println(mallocInstances.map(mn => mn.tp).mkString("\n"))
    System.out.println("\n")
    for (mallocInstance <- mallocInstances) {
      val mallocTp = mallocInstance.tp
      //	 val elemTp = mallocTp.typeArguments(0)
      val index = __newVar[Int](unit(0))
      val elemType = mallocTp
      val poolType = elemType //if (mallocTp.isPrimitive) elemType else typePointer(elemType)
      val POOL_SIZE = 1800000 //* (poolType.toString.split("_").length + 1)
      //val POOL_SIZE = 100000
      /* this one is a hack */
      def regenerateSize(s: Rep[Int]): Rep[Int] = s match {
        case c @ Constant(_)       => s
        case Def(Int$div3(x, y))   => regenerateSize(x) / regenerateSize(y)
        case Def(Int$times3(x, y)) => regenerateSize(x) * regenerateSize(y)
        case d @ Def(_)            => s
      }
      //val POOL_SIZE = regenerateSize(mallocNode.numElems) * 200*/
      val pool = arrayNew(POOL_SIZE)(poolType) //malloc(POOL_SIZE)(poolType)
      if (!mallocTp.isPrimitive) {
        cForLoop(0, POOL_SIZE, (i: Rep[Int]) => {
          if (poolType.isRecord) {
            val mallocNode = mallocInstance.node.asInstanceOf[PardisStruct[Any]]
            val newElems = mallocNode.elems.map(e => {
              val in = {
                if ((e.init.tp.isRecord) || (e.init.tp.isArray)) {
                  System.out.println("----->" + e.init.tp.name)
                  val other = mallocBuffers.find(mb => mb._1.tp.name == e.init.tp.name).get._2.pool
                  arrayApply(other.asInstanceOf[Expression[Array[Any]]], i)(e.init.tp)
                } else infix_asInstanceOf(unit(DefaultValue(e.init.tp.name))(e.init.tp))(e.init.tp)
              }
              PardisStructArg(e.name, true, in)
            })
            System.out.println("Methods for tag " + mallocNode.tag + " ARE " + mallocNode.methods.map(m => m.name))
            val newMethods = mallocNode.methods.map(m => m.copy(body =
              transformDef(m.body.asInstanceOf[Def[Any]]).asInstanceOf[PardisLambdaDef]))
            val allocatedSpace = toAtom(PardisStruct(mallocNode.tag, newElems, newMethods)(elemType))(elemType)
            arrayUpdate(pool, i, allocatedSpace)
          } else if (poolType.isArray) {
            mallocInstance.node match {
              case an @ ArrayNew(size) => {
                val newType = poolType.typeArguments(0)
                val newSize = regenerateSize(size)
                //printf(unit("%d\n"), newSize)
                val allocatedSpace = toAtom(ArrayNew(newSize)(newType.asInstanceOf[PardisType[Any]]))(poolType.asInstanceOf[PardisType[Array[Any]]])
                arrayUpdate(pool, i, allocatedSpace)
              }
            }
          }
          //malloc(unit(1))(elemType)
          //pointer_assign(pool.asInstanceOf[Expression[Pointer[Any]]], i, allocatedSpace)
          unit(())
        })
      }
      mallocBuffers += mallocInstance -> BufferInfo(pool.asInstanceOf[Sym[Any]], index)
      printf(unit("Buffer for type %s of size %d initialized!\n"), unit(mallocTp.toString), POOL_SIZE)
    }
  }

  override def transformDef[T: PardisType](node: Def[T]): to.Def[T] = (node match {
    case GenericEngineRunQueryObject(b) =>
      createBuffers()
      startCollecting = enabled
      val tb = transformBlock(b)
      startCollecting = false
      GenericEngineRunQueryObject(tb)

    case ps @ ArrayNew(size) if startCollecting => {
      System.out.println("REPLACING ARRAY ALLOCATION IN MEMORYTRANSFORMER")
      val mallocInstance = mallocToInstance(ps.asInstanceOf[Def[Any]])
      val bufferInfo = mallocBuffers(mallocInstance)
      val s = toAtom(ReadVal(size))
      val p = arrayApply(bufferInfo.pool.asInstanceOf[Rep[Array[Any]]], readVar(bufferInfo.index)(IntType))(ps.tp.asInstanceOf[PardisType[Any]])
      __assign(bufferInfo.index, readVar(bufferInfo.index)(IntType) + (1))
      ReadVal(p)(ps.tp.asInstanceOf[PardisType[Any]])
    }

    case ps @ PardisStruct(tag, elems, methods) if startCollecting => {
      System.out.println("REPLACING STRUCT ALLOCATION IN MEMORYTRANSFORMER")
      val mallocInstance = mallocToInstance(ps.asInstanceOf[Def[Any]])
      val bufferInfo = mallocBuffers(mallocInstance)
      val p = arrayApply(bufferInfo.pool.asInstanceOf[Rep[Array[Any]]], readVar(bufferInfo.index)(IntType))(ps.tp.asInstanceOf[PardisType[Any]])
      val s = mallocInstance.node.asInstanceOf[PardisStruct[Any]]
      s.elems.map(e => e.name).zip(elems.map(e => e.init)).foreach({
        case (fname, initel) => {
          toAtom(PardisStructFieldSetter(p, fname, initel))
        }
      })
      __assign(bufferInfo.index, readVar(bufferInfo.index)(IntType) + (1))
      ReadVal(p)(ps.tp.asInstanceOf[PardisType[Any]])
    }
    case _ => super.transformDef(node)
  }).asInstanceOf[to.Def[T]]
}
