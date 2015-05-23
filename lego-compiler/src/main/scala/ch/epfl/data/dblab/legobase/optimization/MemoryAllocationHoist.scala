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
import sc.pardis.shallow.utils._

// TODO there should be no need to queryNumber and scalingFactor thanks to Schema

/**
 * Transforms `malloc`s inside the part which runs the query into buffers which are allocated
 * at the loading time.
 *
 * @param IR the polymorphic embedding trait which contains the reified program.
 * @param queryNumber specifies the TPCH query number (TODO should be removed)
 * @param scalingFactor specifies the scaling factor used for TPCH queries (TODO should be removed)
 */
class MemoryAllocationHoist(override val IR: LoweringLegoBase, val queryNumber: Int, val scalingFactor: Double) extends RuleBasedTransformer[LoweringLegoBase](IR) with StructCollector[LoweringLegoBase] {
  import IR._
  //import CNodes._
  //import CTypes._

  /* If you want to disable this optimization, set this flag to `false` */
  val enabled = true

  override def optimize[T: TypeRep](node: Block[T]): to.Block[T] = {
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
  var distinctInstances = List[MallocInfo]()
  case class BufferInfo(pool: Sym[Any], index: Var[Int])
  case class MallocInstance(tp: PardisType[Any], node: Def[Any])
  sealed trait MallocInfo {
    val tp: PardisType[Any]
    def isDependent(o: MallocInfo): Boolean
  }
  case class ArrayMallocInfo(tp: PardisType[Any], size: List[Rep[Int]]) extends MallocInfo {
    def isDependent(o: MallocInfo): Boolean = o match {
      case StructMallocInfo(t, _, _) if tp.typeArguments(0) == t => true
      case _ => false
    }
  }
  case class StructMallocInfo(tp: PardisType[Any], node: PardisStruct[Any], count: Int) extends MallocInfo {
    def isDependent(o: MallocInfo): Boolean = node.elems.map(_.init.tp).exists(_ == o.tp)
  }

  val mallocBuffers = collection.mutable.Map[PardisType[Any], BufferInfo]()

  analysis += rule {
    case GenericEngineRunQueryObject(b) => {
      startCollecting = enabled
      traverseBlock(b)
      startCollecting = false
    }
  }
  analysis += rule {
    case m @ PardisStruct(tag, elems, methods) if startCollecting && phase == FindMallocs => {
      /*System.out.println("---------------------")
      System.out.println("INSERTING STRUCT: " + node.tp + " TO LIST ")
      System.out.println(mallocNodes.map(mn => mn.tp).mkString("\n"))
      System.out.println("CONTAINED? " + mallocNodes.find(mn => mn.tp == node.tp).isEmpty)
      System.out.println("---------------------\n")
      if (mallocNodes.find(mn => mn.tp == node.tp).isEmpty)*/
      mallocNodes += m.asInstanceOf[Def[Any]]
      ()
    }
  }

  analysis += rule {
    case m @ ArrayNew(size) if startCollecting && phase == FindMallocs => {
      /*System.out.println("---------------------")
      System.out.println("INSERTING ARRAy: " + node.tp + " TO LIST ")
      System.out.println(mallocNodes.map(mn => mn.tp).mkString("\n"))
      System.out.println("CONTAINED? " + mallocNodes.find(mn => mn.tp == node.tp).isEmpty)
      System.out.println("---------------------\n")
      if (mallocNodes.find(mn => mn.tp == node.tp).isEmpty)*/
      mallocNodes += m.asInstanceOf[Def[Any]]
      ()
    }
  }

  def cForLoop(start: Int, end: Rep[Int], f: Rep[Int] => Rep[Unit]) {
    val index = __newVar[Int](unit(start))
    __whileDo(readVar(index) < end, {
      f(readVar(index))
      __assign(index, readVar(index) + unit(1))
    })
  }

  def mallocToInstance(node: Def[Any]): MallocInstance = MallocInstance(node.tp, node)

  // An appropriate way of scheduling the code, removes the need for this part of the code
  def regenerateSize(s: Rep[Int]): Rep[Int] = s match {
    case c @ Constant(_)       => s
    case Def(Int$div1(x, y))   => regenerateSize(x) / regenerateSize(y)
    case Def(Int$times1(x, y)) => regenerateSize(x) * regenerateSize(y)
    case d @ Def(_)            => s
  }

  def getPoolSize(mallocInfo: MallocInfo): Int = {
    // val POOL_SIZE = 1800000 //* (poolType.toString.split("_").length + 1)
    //val POOL_SIZE = 100000
    //val POOL_SIZE = regenerateSize(mallocNode.numElems) * 200*/
    val estimatedSize = queryNumber match {
      case 1 | 13 | 18 => 50 * 1000 * 1000
      case 9           => 30 * 1000 * 1000
      case _           => 1800 * 1000
    }
    (estimatedSize / 8 * scalingFactor).toInt
  }

  def createBuffers() {
    //System.out.println("Creating buffers for mallocNodes: " + mallocNodes.mkString("\n"))
    val mallocInstances = mallocNodes.map(m => mallocToInstance(m)) //.sortBy(ll => ll.tp.name.length) //.distinct //.filter(t => !t.tp.name.contains("CArray") /* && !t.tp.name.contains("Pointer")*/ )
    val mallocInstancesTps = mallocInstances.map(mn => mn.tp)
    // System.out.println(mallocInstancesTps.mkString("\n"))
    // System.out.println("\n")
    // System.out.println(s"size: ${mallocInstances.size}, distinct size: ${mallocInstances.distinct.size}")
    distinctInstances = mallocInstances.foldLeft[List[MallocInfo]](Nil)((prev, curr) => {
      prev.find(p => p.tp == curr.tp) match {
        case Some(elem) => {
          elem match {
            case StructMallocInfo(t, sn, cnt) => StructMallocInfo(t, sn, cnt + 1) :: (prev diff List(elem))
            case ArrayMallocInfo(t, l)        => ArrayMallocInfo(t, curr.node.asInstanceOf[ArrayNew[Any]]._length :: l) :: (prev diff List(elem))
          }
        }
        case None =>
          if (curr.tp.isRecord)
            StructMallocInfo(curr.tp, curr.node.asInstanceOf[Struct[Any]], 1) :: prev
          else // if curr.tp.isArray
            ArrayMallocInfo(curr.tp, List(curr.node.asInstanceOf[ArrayNew[Any]]._length)) :: prev
      }
    })
    // schedule the malloc nodes
    // val depEdges = for (inst1 <- distinctInstances; inst2 <- distinctInstances if (inst1 != inst2 && inst1.isDependent(inst2))) yield (inst2 -> inst1)
    // distinctInstances = pardis.utils.Graph.tsort(depEdges).toList
    distinctInstances = sc.pardis.utils.Graph.schedule(distinctInstances, (x: MallocInfo, y: MallocInfo) => y.isDependent(x))
    // System.out.println(s"distinctInstances: ${distinctInstances.mkString("\n\t")}")
    // now iterate over them
    for (mallocInstance <- distinctInstances) {
      val mallocTp = mallocInstance.tp
      //	 val elemTp = mallocTp.typeArguments(0)
      val index = __newVarNamed[Int](unit(0), "memoryPoolIndex")
      val elemType = mallocTp
      val poolType = elemType //if (mallocTp.isPrimitive) elemType else typePointer(elemType)
      val poolSize = getPoolSize(mallocInstance)
      val pool = arrayNew(poolSize)(poolType) //malloc(poolSize)(poolType)
      if (!mallocTp.isPrimitive) {
        cForLoop(0, poolSize, (i: Rep[Int]) => {
          if (poolType.isRecord) {
            val mallocNode = mallocInstance.asInstanceOf[StructMallocInfo].node
            val newElems = mallocNode.elems.map(e => {
              val in = {
                if (mallocBuffers.exists(mb => mb._1.name == e.init.tp.name) &&
                  ((e.init.tp.isRecord && e.init.tp != mallocInstance.asInstanceOf[StructMallocInfo].tp) || (e.init.tp.isArray))) {
                  // System.out.println("----->" + e.init.tp.name)
                  val other = mallocBuffers.find(mb => mb._1.name == e.init.tp.name).get._2.pool
                  arrayApply(other.asInstanceOf[Expression[Array[Any]]], i)(e.init.tp)
                } else infix_asInstanceOf(unit(DefaultValue(e.init.tp.name))(e.init.tp))(e.init.tp)
              }
              PardisStructArg(e.name, true, in)
            })
            // System.out.println("Methods for tag " + mallocNode.tag + " ARE " + mallocNode.methods.map(m => m.name))
            val newMethods = mallocNode.methods.map(m => m.copy(body =
              transformDef(m.body.asInstanceOf[Def[Any]]).asInstanceOf[PardisLambdaDef]))
            val allocatedSpace = toAtom(PardisStruct(mallocNode.tag, newElems, newMethods)(elemType))(elemType)
            arrayUpdate(pool, i, allocatedSpace)
          } else if (poolType.isArray) {
            // mallocInstance.node match {
            //   case an @ ArrayNew(size) => {
            // TODO compute the sum of all additions
            val size = mallocInstance.asInstanceOf[ArrayMallocInfo].size.head
            val newType = poolType.typeArguments(0)
            val newSize = regenerateSize(size)
            //printf(unit("%d\n"), newSize)
            val allocatedSpace = toAtom(ArrayNew(newSize)(newType.asInstanceOf[PardisType[Any]]))(poolType.asInstanceOf[PardisType[Array[Any]]])
            arrayUpdate(pool, i, allocatedSpace)
            //   }
            // }
          }
          //malloc(unit(1))(elemType)
          //pointer_assign(pool.asInstanceOf[Expression[Pointer[Any]]], i, allocatedSpace)
          unit(())
        })
      }
      mallocBuffers += mallocInstance.tp -> BufferInfo(pool.asInstanceOf[Sym[Any]], index)
      // printf(unit("Buffer for type %s of size %d initialized!\n"), unit(mallocTp.toString), poolSize)
    }
    // System.out.println(s"mallocBuffers: ${mallocBuffers.mkString("\n\t")}")
  }

  rewrite += rule {
    case GenericEngineRunQueryObject(b) =>
      createBuffers()
      startCollecting = enabled
      val tb = transformBlock(b)(b.tp)
      startCollecting = false
      GenericEngineRunQueryObject(tb)(tb.tp)
  }

  rewrite += rule {
    case ps @ ArrayNew(size) if startCollecting => {
      System.out.println(s"REPLACING ARRAY ALLOCATION IN MEMORYTRANSFORMER ${ps.tp}")
      // val mallocInstance = mallocToInstance(ps.asInstanceOf[Def[Any]])
      // val bufferInfo = mallocBuffers(mallocInstance)
      val bufferInfo = mallocBuffers(ps.tp.asInstanceOf[PardisType[Any]])
      // val s = toAtom(ReadVal(size))
      val s = size
      val p = arrayApply(bufferInfo.pool.asInstanceOf[Rep[Array[Any]]], readVar(bufferInfo.index)(IntType))(ps.tp.asInstanceOf[PardisType[Any]])
      __assign(bufferInfo.index, readVar(bufferInfo.index)(IntType) + (1))
      p
    }
  }

  rewrite += rule {
    case ps @ PardisStruct(tag, elems, methods) if startCollecting => {
      System.out.println(s"REPLACING STRUCT ALLOCATION IN MEMORYTRANSFORMER ${ps.tp}")
      // val mallocInstance = mallocToInstance(ps.asInstanceOf[Def[Any]])
      // val bufferInfo = mallocBuffers(mallocInstance)
      // System.out.println(s"finding tp: ${ps.tp} with class ${ps.tp.asInstanceOf[RecordType[_]].tag} ${mallocBuffers.find(_._1 == ps.tp)}")
      val mallocInstance = distinctInstances.find(_.tp == ps.tp).get.asInstanceOf[StructMallocInfo]
      val bufferInfo = mallocBuffers(ps.tp.asInstanceOf[PardisType[Any]])
      val p = arrayApply(bufferInfo.pool.asInstanceOf[Rep[Array[Any]]], readVar(bufferInfo.index)(IntType))(ps.tp.asInstanceOf[PardisType[Any]])
      val s = mallocInstance.node.asInstanceOf[PardisStruct[Any]]
      s.elems.map(e => e.name).zip(elems.map(e => e.init)).foreach({
        case (fname, initel) => {
          // toAtom(PardisStructFieldSetter(p, fname, apply(initel)))
          fieldSetter(p, fname, apply(initel))
        }
      })
      __assign(bufferInfo.index, readVar(bufferInfo.index)(IntType) + (1))
      p
    }
  }
}
