package ch.epfl.data
package legobase
package optimization

import scala.language.implicitConversions
import pardis.ir._
import reflect.runtime.universe.{ TypeTag, Type }
import pardis.optimization._
import deep._
import ch.epfl.data.pardis.ir.pardisTypeImplicits._

class HashMapToArrayTransformer(override val IR: LoweringLegoBase) extends Optimizer[LoweringLegoBase](IR) {
  import IR._
  import CNodes._
  import CTypes._

  var enabled = true

  sealed trait Kind
  case object AggOpCase extends Kind
  case object WindowOpCase extends Kind
  case object OtherCase extends Kind

  case class HashMapStruct[K, V](sym: Sym[Any], size: Var[Int], keySet: Var[Set[K]], arr: Rep[Array[NextContainer[ArrayBuffer[V]]]])(implicit val keyType: TypeRep[K], valueType: TypeRep[V])

  val hashMapKinds = collection.mutable.Map[Sym[Any], Kind]()
  val hashMapExtractor = collection.mutable.Map[Sym[Any], Rep[Any => Any]]()
  val hashMapStructs = collection.mutable.Map[Sym[Any], HashMapStruct[Any, Any]]()
  val arrayBufferHashMaps = collection.mutable.Map[Sym[Any], Sym[Any]]()

  def optimize[T: TypeRep](node: Block[T]): to.Block[T] = {
    traverseBlock(node)
    System.out.println(hashMapKinds.mkString("\n"))
    System.out.println("==========")
    System.out.println(arrayBufferHashMaps.mkString("\n"))
    transformProgram(node)
  }

  override def traverseDef(node: Def[_]): Unit = node match {
    case ArrayBufferAppend(ret @ Def(HashMapGetOrElseUpdate(hm, _, _)), _) => {
      // this will override the value written by the next one
      hashMapKinds(hm.asInstanceOf[Sym[Any]]) = OtherCase
      arrayBufferHashMaps(ret.asInstanceOf[Sym[Any]]) = hm.asInstanceOf[Sym[Any]]
    }
    case HashMapGetOrElseUpdate(hm, _, _) => {
      hashMapKinds(hm.asInstanceOf[Sym[Any]]) = AggOpCase
    }
    case OptionGet(ret @ Def(HashMapRemove(hm, _))) => {
      if (getKind(hm) != AggOpCase)
        hashMapKinds(hm.asInstanceOf[Sym[Any]]) = WindowOpCase
      arrayBufferHashMaps(ret.asInstanceOf[Sym[Any]]) = hm.asInstanceOf[Sym[Any]]
    }
    case HashMapClear(_) => enabled = false
    case _               => super.traverseDef(node)
  }

  override def traverseStm(stm: Stm[_]): Unit = stm match {
    case Stm(sym, HashMapNew3(extractor)) => {
      hashMapExtractor(sym.asInstanceOf[Sym[Any]]) = extractor.asInstanceOf[Rep[Any => Any]]
    }
    case _ => {
      super.traverseStm(stm)
    }
  }

  val ARRAY_SIZE = 1000000

  override def transformStm(stm: Stm[_]): to.Stm[_] = stm match {
    case Stm(sym, hmn @ HashMapNew3(_)) if isNotAggOp(sym) => {
      val size = __newVar[Int](unit(0))
      val keySet = __newVar(Set()(hmn.typeA, overloaded2))(SetType(hmn.typeA))
      val arr = __newArray(unit(ARRAY_SIZE))(NextContainerType(ArrayBufferType(hmn.typeB)))
      val hmSym = sym.asInstanceOf[Sym[Any]]
      hashMapStructs(hmSym) = HashMapStruct(hmSym, size, keySet, arr)(hmn.typeA, hmn.typeB)
      // does not reify this statment in the following way
      stm
    }
    case _ => super.transformStm(stm)
  }

  def getKind[T](hm: Rep[T]): Kind = hashMapKinds(hm.asInstanceOf[Sym[Any]])
  def getStruct[K, V](hm: Rep[_]): HashMapStruct[K, V] = hashMapStructs(hm.asInstanceOf[Sym[Any]]).asInstanceOf[HashMapStruct[K, V]]

  trait Key
  trait Value

  def key2Hash(key: Rep[Key]) = {
    infix_hashCode(key)(key.tp) % unit(ARRAY_SIZE)
  }

  def proceedHashMap[T, K](hmInput: Rep[T], k: Rep[K]): Var[Any] = {
    implicit val manKey = hmInput.tp.typeArguments(0).asInstanceOf[TypeRep[Key]]
    implicit val manValue = hmInput.tp.typeArguments(1).typeArguments(0).asInstanceOf[TypeRep[Value]]
    val hmSym = hmInput.asInstanceOf[Sym[Any]]
    val hm = hmInput.asInstanceOf[Rep[HashMap[Key, ArrayBuffer[Value]]]]
    val key = k.asInstanceOf[Rep[Key]]
    val hmStruct = hashMapStructs(hmSym).asInstanceOf[HashMapStruct[Key, Value]]
    val arr = hmStruct.arr
    val ext = hashMapExtractor(hmSym).asInstanceOf[Rep[Value => Key]]
    val hashKey = key2Hash(key)
    val elem = __newVar(arr(hashKey))
    __whileDo(infix_!=(readVar(elem), unit(null)) && {
      val currentBuffer = readVar(elem).current
      !currentBuffer.isEmpty && infix_!=(__app(ext).apply(currentBuffer.apply(unit(0))), key)
    }, {
      __assign(elem, readVar(elem).next)
    })
    elem.asInstanceOf[Var[Any]]
  }

  def checkForHashSym[T](hm: Rep[T], p: Sym[Any] => Boolean): Boolean = enabled && p(hm.asInstanceOf[Sym[Any]])
  def checkForArrayBuffer[T](rt: Rep[T], p: Sym[Any] => Boolean): Boolean = enabled && arrayBufferHashMaps.get(rt.asInstanceOf[Sym[Any]]).map(x => p(x)).getOrElse(false)

  def isWindowOp[T](hm: Rep[T]) = checkForHashSym(hm, x => getKind(x) == WindowOpCase)
  def isWindowOpArrayBuffer[T](rt: Rep[T]) = checkForArrayBuffer(rt, x => getKind(x) == WindowOpCase)
  // def isNotAggOp[T](hm: Rep[T]) = checkForHashSym(hm, x => getKind(x) != AggOpCase)
  // def isNotAggOpArrayBuffer[T](rt: Rep[T]) = checkForArrayBuffer(rt, x => getKind(x) != AggOpCase)
  def isNotAggOp[T](hm: Rep[T]) = checkForHashSym(hm, x => getKind(x) == WindowOpCase)
  def isNotAggOpArrayBuffer[T](rt: Rep[T]) = checkForArrayBuffer(rt, x => getKind(x) == WindowOpCase)

  def currentElement(e: Var[Any])(hm: Rep[_]): Def[Any] = {
    implicit val manValue = hm.tp.typeArguments(1).typeArguments(0).asInstanceOf[TypeRep[Value]]
    val elem = e.asInstanceOf[Var[NextContainer[ArrayBuffer[Value]]]]
    ReadVal(readVar(elem).current)
    // ReadVar(elem)(elem.tp).asInstanceOf[Def[Any]]
  }

  override def transformDef[T: PardisType](node: Def[T]): to.Def[T] = (node match {
    case HashMapGetOrElseUpdate(hm, k, v) if isNotAggOp(hm) => {
      implicit val manKey = hm.tp.typeArguments(0).asInstanceOf[TypeRep[Key]]
      implicit val manValue = hm.tp.typeArguments(1).typeArguments(0).asInstanceOf[TypeRep[Value]]
      val hmStruct = hashMapStructs(hm.asInstanceOf[Sym[Any]]).asInstanceOf[HashMapStruct[Key, Value]]
      val elem = proceedHashMap(hm, k).asInstanceOf[Var[NextContainer[ArrayBuffer[Value]]]]
      val key = k.asInstanceOf[Rep[Key]]
      val hashKey = key2Hash(key)
      val currElem = readVar(elem)
      val arr = hmStruct.arr
      val keySet = hmStruct.keySet
      val size = hmStruct.size
      __ifThenElse(infix_==(currElem, unit(null)), {
        val newElem = __newNextContainer[ArrayBuffer[Value]](ArrayBuffer[Value](), arr(hashKey))
        arr(hashKey) = newElem
        __assign(elem, newElem)
        __assign(size, readVar(size) + unit(1))
        // the following is terribly slow
        // __assign(keySet, readVar(keySet) + key)
        readVar(keySet) += key
      }, unit(()))
      currentElement(elem)(hm)
      // ReadVal(readVar(elem).current)
    }
    case HashMapSize(hm) if isNotAggOp(hm) => {
      ReadVar(getStruct(hm).size)
    }
    case HashMapContains(hm, key) if isNotAggOp(hm) => {
      val elem = proceedHashMap(hm, key)
      NotEqual(elem, unit(null))
    }
    case HashMapApply(hm, key) if isNotAggOp(hm) => {
      val elem = proceedHashMap(hm, key)
      currentElement(elem)(hm)
      // ReadVal(readVar(elem).current)
    }
    case HashMapKeySet(hm) if isNotAggOp(hm) => {
      val hmStruct = getStruct[Key, Value](hm)
      ReadVar(hmStruct.keySet)(SetType(hmStruct.keyType))
    }
    case HashMapRemove(hm, k) if isWindowOp(hm) => {
      implicit val manKey = hm.tp.typeArguments(0).asInstanceOf[TypeRep[Key]]
      implicit val manValue = hm.tp.typeArguments(1).typeArguments(0).asInstanceOf[TypeRep[Value]]
      val hmStruct = hashMapStructs(hm.asInstanceOf[Sym[Any]]).asInstanceOf[HashMapStruct[Key, Value]]
      val key = k.asInstanceOf[Rep[Key]]
      val keySet = hmStruct.keySet
      val size = hmStruct.size
      val elem = proceedHashMap(hm, key)
      readVar(keySet).remove(key)
      __assign(size, readVar(size) - 1)
      currentElement(elem)(hm)
      // ReadVal(readVar(elem).current)
    }
    case HashMapRemove(hm, k) if isNotAggOp(hm) => { // hence, it's for AntiHashJoin
      implicit val manKey = hm.tp.typeArguments(0).asInstanceOf[TypeRep[Key]]
      implicit val manValue = hm.tp.typeArguments(1).typeArguments(0).asInstanceOf[TypeRep[Value]]
      val hmStruct = hashMapStructs(hm.asInstanceOf[Sym[Any]]).asInstanceOf[HashMapStruct[Key, Value]]
      val key = k.asInstanceOf[Rep[Key]]
      val keySet = hmStruct.keySet
      val size = hmStruct.size
      readVar(keySet).remove(key)
      ReadVal(__assign(size, readVar(size) - 1))
      // ReadVal(readVar(elem).current)
    }
    // case ArrayBufferAppend(sym, v) if isNotAggOpArrayBuffer(sym) => {
    //   implicit val manValue = v.tp.asInstanceOf[TypeRep[Value]]
    //   val value = v.asInstanceOf[Rep[Value]]
    //   val ret = sym.asInstanceOf[Rep[NextContainer[ArrayBuffer[Value]]]]
    //   val retCurrent = ret.current
    //   ArrayBufferAppend(retCurrent, value)
    // }
    case OptionGet(ret) if isNotAggOpArrayBuffer(ret) => {
      ret.correspondingNode
    }
    case _ => super.transformDef(node)
  }).asInstanceOf[to.Def[T]]
}
