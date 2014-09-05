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

  var enabled = false

  sealed trait Kind
  case object AggOpCase extends Kind
  case object WindowOpCase extends Kind
  case object OtherCase extends Kind

  case class HashMapStruct[K, V](sym: Sym[Any], size: Var[Int], keySet: Var[Set[K]], arr: Rep[Array[NextContainer[ArrayBuffer[V]]]], lastElem: Var[NextContainer[ArrayBuffer[V]]])(implicit val keyType: TypeRep[K], valueType: TypeRep[V])

  case class HashMapStructAggOp[K](sym: Sym[Any], arr: Rep[Array[NextKeyContainer[K, Array[Double]]]], lastPointer: Var[Int])(implicit val keyType: TypeRep[K]) {
    val valueType = typeRep[Array[Double]]
  }

  val hashMapKinds = collection.mutable.Map[Sym[Any], Kind]()
  val hashMapExtractor = collection.mutable.Map[Sym[Any], Rep[Any => Any]]()
  val hashMapStructs = collection.mutable.Map[Sym[Any], HashMapStruct[Any, Any]]()
  val hashMapAggOpStructs = collection.mutable.Map[Sym[Any], HashMapStructAggOp[Any]]()
  val arrayBufferHashMaps = collection.mutable.Map[Sym[Any], Sym[Any]]()
  /* is being used for AggOp */
  val arrayHashMaps = collection.mutable.Map[Sym[Any], Sym[Any]]()
  val keySetHashMaps = collection.mutable.Map[Var[Any], Sym[Any]]()

  def optimize[T: TypeRep](node: Block[T]): to.Block[T] = {
    traverseBlock(node)
    System.out.println(hashMapKinds.mkString("\n"))
    System.out.println("==========")
    System.out.println(arrayBufferHashMaps.mkString("\n"))
    System.out.println("====<>====")
    System.out.println(keySetHashMaps.mkString("\n"))
    System.out.println("===ARR====")
    System.out.println(arrayHashMaps.mkString("\n"))

    transformProgram(node)
  }

  /* Here we identify which kind of hash map it is */

  override def traverseDef(node: Def[_]): Unit = node match {
    /*case ArrayBufferAppend(ret @ Def(HashMapGetOrElseUpdate(hm, _, _)), _) => {
      // this will override the value written by the pattern
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
    case ReadVar(v @ Var(Def(NewVar(Def(SetNew(Def(SetToSeq(Def(HashMapKeySet(hm)))))))))) => {
      keySetHashMaps(v.asInstanceOf[Var[Any]]) = hm.asInstanceOf[Sym[Any]]
    }
    case HashMapClear(_) => enabled = false
    case ArrayUpdate(arr @ Def(HashMapGetOrElseUpdate(hm, _, _)), _, _) => {
      arrayHashMaps += arr.asInstanceOf[Sym[Any]] -> hm.asInstanceOf[Sym[Any]]
    }*/
    case _ => super.traverseDef(node)
  }

  override def traverseStm(stm: Stm[_]): Unit = stm match {
    case Stm(sym, HashMapNew3(extractor, _)) => {
      hashMapExtractor(sym.asInstanceOf[Sym[Any]]) = extractor.asInstanceOf[Rep[Any => Any]]
    }
    case _ => {
      super.traverseStm(stm)
    }
  }

  override def transformStmToMultiple(stm: Stm[_]): List[to.Stm[_]] = stm match {
    case Stm(sym, ArrayBufferNew2()) => Nil
    case Stm(sym, SetHead(s))        => Nil
    case Stm(sym, SetRemove(s, k))   => Nil
    case _                           => super.transformStmToMultiple(stm)
  }

  override def transformStm(stm: Stm[_]): to.Stm[_] = stm match {
    // case Stm(sym, hmn @ HashMapNew3(_)) if !isAggOp(sym) => {
    //implicit val manValue = sym.tp.typeArguments(1).typeArguments(0).asInstanceOf[TypeRep[Value]]
    //val size = __newVar[Int](unit(0))
    //val keySet = __newVar(Set()(hmn.typeA, overloaded2))(SetType(hmn.typeA))
    //val arrBufTp = NextContainerType(ArrayBufferType(hmn.typeB))
    // val arr = __newArray(unit(ARRAY_SIZE))(hmn.typeB)
    // val lastElemNull = infix_asInstanceOf(unit(null))(arrBufTp)
    //val lastElemNull = __newNextContainer[ArrayBuffer[Value]](ArrayBuffer[Value](), unit(null))
    // System.out.println("last elem: " + lastElemNull)
    // val lastElem = __newVar(lastElemNull)(arrBufTp)
    //val lastElem = __newVar(lastElemNull)
    // val lastElem = null
    // val lastElem = __newVar(unit(null))
    //val hmSym = sym.asInstanceOf[Sym[Any]]
    //hashMapStructs(hmSym) = HashMapStruct(hmSym, size, keySet, arr, lastElem.asInstanceOf[Var[NextContainer[ArrayBuffer[Any]]]])(hmn.typeA, hmn.typeB)
    // does not reify this statment in the following way
    //stm
    //  arr
    //}
    /*case Stm(sym, hmn @ HashMapNew4()) if isAggOp(sym) => {
      implicit val manKey = sym.tp.typeArguments(0).asInstanceOf[TypeRep[Key]]
      type Value = Array[Double]
      implicit val manValue = sym.tp.typeArguments(1).typeArguments(0).asInstanceOf[TypeRep[Value]]
      val lastPointer = __newVar[Int](unit(0))
      val arr = __newArray[NextKeyContainer[Key, Value]](unit(ARRAY_SIZE))(NextKeyContainerType(manKey, manValue))
      val hmSym = sym.asInstanceOf[Sym[Any]]
      hashMapAggOpStructs(hmSym) = HashMapStructAggOp[Key](hmSym, arr, lastPointer).asInstanceOf[HashMapStructAggOp[Any]]
      // does not reify this statment in the following way
      stm
    }*/
    case Stm(sym, Int$greater$eq4(e1, Def(ab @ ArrayBufferSize(a)))) => a.correspondingNode match {
      case rv @ PardisReadVar(f @ PardisVar(ab)) =>
        implicit val manValue = a.tp.asInstanceOf[TypeRep[Value]]
        val newSym = fresh[Boolean]
        val z = ReadVal(infix_==(PardisStructFieldGetter(ab, "next")(manValue), unit(null)))(BooleanType)
        val s = Stm(newSym, z)
        subst += sym -> newSym
        reflectStm(s)
        s
      case _ => throw new Exception("Internal BUG in HashMapToArrayTransformer. ArrayBuffer should be a Var");
    }
    case _ => super.transformStm(stm)
  }

  def getKind[T](hm: Rep[T]): Kind = hashMapKinds(hm.asInstanceOf[Sym[Any]])
  def getStruct[K, V](hm: Rep[_]): HashMapStruct[K, V] = hashMapStructs(hm.asInstanceOf[Sym[Any]]).asInstanceOf[HashMapStruct[K, V]]

  trait Key
  trait Value

  def key2Hash(key: Rep[Key], size: Rep[Int]) = {
    infix_hashCode(key)(key.tp) % size
  }

  // def proceedHashMap[T, K](hmInput: Rep[T], k: Rep[K]): Var[Any] = {
  //   implicit val manKey = hmInput.tp.typeArguments(0).asInstanceOf[TypeRep[Key]]
  //   implicit val manValue = hmInput.tp.typeArguments(1).typeArguments(0).asInstanceOf[TypeRep[Value]]
  //   val hmSym = hmInput.asInstanceOf[Sym[Any]]
  //   val hm = hmInput.asInstanceOf[Rep[HashMap[Key, ArrayBuffer[Value]]]]
  //   val key = k.asInstanceOf[Rep[Key]]
  //   val hmStruct = hashMapStructs(hmSym).asInstanceOf[HashMapStruct[Key, Value]]
  //   val arr = hmStruct.arr
  //   val ext = hashMapExtractor(hmSym).asInstanceOf[Rep[Value => Key]]
  //   val hashKey = key2Hash(key)
  //   val elem = __newVar(arr(hashKey))
  //   __whileDo(infix_!=(readVar(elem), unit(null)) && {
  //     val currentBuffer = readVar(elem).current
  //     currentBuffer.isEmpty || infix_!=(__app(ext).apply(currentBuffer.apply(unit(0))), key)
  //   }, {
  //     __assign(elem, readVar(elem).next)
  //   })
  //   elem.asInstanceOf[Var[Any]]
  // }

  // def proceedHashMapAggOp[T, K](hmInput: Rep[T], k: Rep[K]): Var[Any] = {
  //   implicit val manKey = hmInput.tp.typeArguments(0).asInstanceOf[TypeRep[Key]]
  //   implicit val manValue = hmInput.tp.typeArguments(1).typeArguments(0).asInstanceOf[TypeRep[Value]]
  //   val hmSym = hmInput.asInstanceOf[Sym[Any]]
  //   val hm = hmInput.asInstanceOf[Rep[HashMap[Key, ArrayBuffer[Value]]]]
  //   val key = k.asInstanceOf[Rep[Key]]
  //   val hmStruct = hashMapStructs(hmSym).asInstanceOf[HashMapStruct[Key, Value]]
  //   val arr = hmStruct.arr
  //   val ext = hashMapExtractor(hmSym).asInstanceOf[Rep[Value => Key]]
  //   val hashKey = key2Hash(key)
  //   val elem = __newVar(arr(hashKey))
  //   __whileDo(infix_!=(readVar(elem), unit(null)) && {
  //     val currentBuffer = readVar(elem).current
  //     currentBuffer.isEmpty || infix_!=(__app(ext).apply(currentBuffer.apply(unit(0))), key)
  //   }, {
  //     __assign(elem, readVar(elem).next)
  //   })
  //   elem.asInstanceOf[Var[Any]]
  // }

  def checkForHashSym[T](hm: Rep[T], p: Sym[Any] => Boolean): Boolean = enabled && p(hm.asInstanceOf[Sym[Any]])
  def checkForArrayBuffer[T](rt: Rep[T], p: Sym[Any] => Boolean): Boolean = enabled && arrayBufferHashMaps.get(rt.asInstanceOf[Sym[Any]]).map(x => p(x)).getOrElse(false)
  def checkForSet[T](v: Var[T], p: Sym[Any] => Boolean): Boolean = enabled && keySetHashMaps.get(v.asInstanceOf[Var[Any]]).map(x => p(x)).getOrElse(false)
  def checkForArray[T](arr: Rep[T], p: Sym[Any] => Boolean): Boolean = enabled && arrayHashMaps.get(arr.asInstanceOf[Sym[Any]]).map(x => p(x)).getOrElse(false)

  def isWindowOp[T](hm: Rep[T]) = checkForHashSym(hm, x => getKind(x) == WindowOpCase)
  def isWindowOpArrayBuffer[T](rt: Rep[T]) = checkForArrayBuffer(rt, x => getKind(x) == WindowOpCase)
  def isAggOp[T](hm: Rep[T]) = checkForHashSym(hm, x => getKind(x) != AggOpCase)
  def isAggOpArrayBuffer[T](rt: Rep[T]) = checkForArrayBuffer(rt, x => getKind(x) != AggOpCase)
  // def !isAggOp[T](hm: Rep[T]) = checkForHashSym(hm, x => getKind(x) == WindowOpCase)
  // def !isAggOpArrayBuffer[T](rt: Rep[T]) = checkForArrayBuffer(rt, x => getKind(x) == WindowOpCase)

  def currentElement(e: Var[Any])(hm: Rep[_]): Def[Any] = {
    implicit val manValue = hm.tp.typeArguments(1).typeArguments(0).asInstanceOf[TypeRep[Value]]
    val elem = e.asInstanceOf[Var[NextContainer[ArrayBuffer[Value]]]]
    ReadVal(readVar(elem).current)
    // ReadVar(elem)(elem.tp).asInstanceOf[Def[Any]]
  }

  override def transformType[T: PardisType]: PardisType[Any] = ({
    val tp = typeRep[T]
    if (tp.name.startsWith("HashMap")) {
      if (tp.typeArguments(1).typeArguments.size != 0) typeArray(tp.typeArguments(1).typeArguments(0))
      else typeArray(tp.typeArguments(1))
    } else if (tp.name.contains("ArrayBuffer")) typePardisVariable(tp.typeArguments(0))
    else super.transformType[T]
  }).asInstanceOf[PardisType[Any]]

  val counterMap = new scala.collection.mutable.HashMap[Expression[_], Var[Int]]()

  override def transformDef[T: PardisType](node: Def[T]): to.Def[T] = (node match {
    case ps @ PardisStruct(tag, elems, methods) =>
      val init = infix_asInstanceOf(Constant(null))(ps.tp)
      PardisStruct(tag, elems ++ List(PardisStructArg("next", true, init)), methods)

    case hmn @ HashMapNew3(_, size) =>
      val arr = arrayNew(size)(hmn.typeB)
      val index = __newVar[Int](0)
      __whileDo(readVar(index) < size, {
        val init = malloc(unit(1))(hmn.typeB)
        arrayUpdate(arr, readVar(index), init)
        val e = arrayApply(arr, readVar(index))(hmn.typeB)
        toAtom(PardisStructFieldSetter(e, "next", Constant(null)))
        __assign(index, readVar(index) + unit(1))
        unit()
      })
      counterMap += arr -> __newVar[Int](unit(0))
      ReadVal(arr)(arr.tp)

    case hmgeu @ HashMapGetOrElseUpdate(hm, k, v) /*if !isAggOp(hm)*/ => {
      implicit val manValue = transformType(hm.tp) /*.typeArguments(1).typeArguments(0)*/ .asInstanceOf[TypeRep[Value]]
      val key = k.asInstanceOf[Rep[Key]]
      val size = arrayLength(transformExp(hm)(hm.tp, typeArray(hmgeu.typeB)))
      val hashKey = key2Hash(key, size)
      val lhm = transformExp(hm)(hm.tp, typeArray(manValue))
      val ptrValue = typePointer(manValue)
      val bucket = __newVar(ArrayApply(lhm, hashKey)(manValue))(manValue)
      ReadVar(bucket)(manValue)
    }
    case hmcnt @ HashMapContains(hm, k) /*if !isAggOp(hm)*/ => {
      implicit val manValue = hm.tp.typeArguments(1).typeArguments(0).asInstanceOf[TypeRep[Value]]
      val key = k.asInstanceOf[Rep[Key]]
      val size = arrayLength(transformExp(hm)(hm.tp, typeArray(hmcnt.typeB)))
      val hashKey = key2Hash(key, size)
      val lhm = transformExp(hm)(hm.tp, typeArray(manValue))
      val bucket = ArrayApply(lhm, hashKey)(manValue)
      val elem = PardisStructFieldGetter(bucket, "next")(manValue)
      ReadVal(infix_!=(elem, unit(null)))
    }
    case hmapp @ HashMapApply(hm, k) /*if !isAggOp(hm)*/ => {
      implicit val manValue = hm.tp.typeArguments(1).typeArguments(0).asInstanceOf[TypeRep[Value]]
      val key = k.asInstanceOf[Rep[Key]]
      val size = arrayLength(transformExp(hm)(hm.tp, typeArray(hmapp.typeB)))
      val hashKey = key2Hash(key, size)
      val lhm = transformExp(hm)(hm.tp, typeArray(manValue))
      val bucket = __newVar(ArrayApply(lhm, hashKey)(manValue))(manValue)
      ReadVar(bucket)
    }
    case hmr @ HashMapRemove(hm, k) => {
      implicit val manValue = hm.tp.typeArguments(1).typeArguments(0).asInstanceOf[TypeRep[Value]]
      val key = k.asInstanceOf[Rep[Key]]
      val size = arrayLength(transformExp(hm)(hm.tp, typeArray(hmr.typeB)))
      val hashKey = key2Hash(key, size)
      val lhm = transformExp(hm)(hm.tp, typeArray(manValue))
      val bucket = __newVar(ArrayApply(lhm, hashKey)(manValue))(manValue)
      val headNext = toAtom(PardisStructFieldGetter(bucket, "next")(manValue))
      //val headNextNext = toAtom(PardisStructFieldGetter)
      //toAtom(PardisStructFieldSetter(bucket, "next", headNext))
      ReadVal(headNext)
    }
    case hmz @ HashMapSize(hm) => {
      System.out.println(counterMap.mkString("\n"))
      hm match {
        case Def(ReadVal(x)) => ReadVar(counterMap(x))
      }
    }

    //case abf @ ArrayBufferNew2() => PardisCast(Constant(null))(typeRep[Null], abf.tp.typeArguments(0))
    //ReadVal(x.asInstanceOf[Expression[Any]])(typePointer(abf.tp.typeArguments(0)).asInstanceOf[PardisType[Any]])
    case aba @ ArrayBufferAppend(a, e) =>
      implicit val manValue = a.tp.typeArguments(0).asInstanceOf[TypeRep[Value]]
      val la = transformExp(a)(a.tp, manValue).asInstanceOf[Expression[Pointer[Any]]]

      val headNext = toAtom(PardisStructFieldGetter(la, "next")(manValue))
      toAtom(PardisStructFieldSetter(e, "next", headNext))
      toAtom(PardisStructFieldSetter(la, "next", e))
      ReadVal(Constant(true))
    case ArrayBufferApply(a, idx) =>
      a.correspondingNode match {
        case rv @ PardisReadVar(f @ PardisVar(ab)) =>
          implicit val manValue = ab.tp.typeArguments(0).asInstanceOf[TypeRep[Value]]
          val aVarNext = toAtom(PardisStructFieldGetter(ab, "next")(manValue))
          __assign(f.asInstanceOf[PardisVar[Any]], aVarNext)
          ReadVal(ab)
        case _ => throw new Exception("Internal BUG in HashMapToArrayTransformer. ArrayBuffer should be a Var");
      }

    //ReadVal(bucket)
    //       implicit val manKey = hm.tp.typeArguments(0).asInstanceOf[TypeRep[Key]]
    //   val hmStruct = hashMapStructs(hm.asInstanceOf[Sym[Any]]).asInstanceOf[HashMapStruct[Key, Value]]
    //   val elem = proceedHashMap(hm, k).asInstanceOf[Var[NextContainer[ArrayBuffer[Value]]]]
    // val currElem = readVar(elem)
    // val arr = hmStruct.arr
    // val keySet = hmStruct.keySet
    // val size = hmStruct.size
    //  __ifThenElse(infix_==(currElem, unit(null)), {
    //    val newElem = __newNextContainer[ArrayBuffer[Value]](ArrayBuffer[Value](), arr(hashKey))
    //   arr(hashKey) = newElem
    //  __assign(elem, newElem)
    // __assign(size, readVar(size) + unit(1))
    // the following is terribly slow
    // __assign(keySet, readVar(keySet) + key)
    // / readVar(keySet) += key
    //}, unit(()))

    //currentElement(elem)(hm)
    // ReadVal(readVar(elem).current)
    //  }
    // case HashMapSize(hm) if !isAggOp(hm) => {
    //   ReadVar(getStruct(hm).size)
    // }
    // case HashMapContains(hm, key) if !isAggOp(hm) => {
    //   val elem = proceedHashMap(hm, key)
    //   /* here is an optimization */
    //   // implicit val manKey = hm.tp.typeArguments(0).asInstanceOf[TypeRep[Key]]
    //   // implicit val manValue = hm.tp.typeArguments(1).typeArguments(0).asInstanceOf[TypeRep[Value]]
    //   // val hmStruct = hashMapStructs(hm.asInstanceOf[Sym[Any]]).asInstanceOf[HashMapStruct[Key, Value]]
    //   // __assign(hmStruct.lastElem, readVar(elem))
    //   NotEqual(readVar(elem), unit(null))
    // }
    // case HashMapApply(hm, key) if !isAggOp(hm) => {
    //   val elem = proceedHashMap(hm, key)
    //   currentElement(elem)(hm)
    //   /* here is an optimization */
    //   // implicit val manKey = hm.tp.typeArguments(0).asInstanceOf[TypeRep[Key]]
    //   // implicit val manValue = hm.tp.typeArguments(1).typeArguments(0).asInstanceOf[TypeRep[Value]]
    //   // val hmStruct = hashMapStructs(hm.asInstanceOf[Sym[Any]]).asInstanceOf[HashMapStruct[Key, Value]]
    //   // ReadVal(readVar(hmStruct.lastElem).current)
    // }
    // case HashMapKeySet(hm) if !isAggOp(hm) => {
    //   val hmStruct = getStruct[Key, Value](hm)
    //   ReadVar(hmStruct.keySet)(SetType(hmStruct.keyType))
    // }
    // case HashMapRemove(hm, k) if isWindowOp(hm) => { // hence, it's for WindowOp
    //   implicit val manKey = hm.tp.typeArguments(0).asInstanceOf[TypeRep[Key]]
    //   implicit val manValue = hm.tp.typeArguments(1).typeArguments(0).asInstanceOf[TypeRep[Value]]
    //   val hmStruct = hashMapStructs(hm.asInstanceOf[Sym[Any]]).asInstanceOf[HashMapStruct[Key, Value]]
    //   val key = k.asInstanceOf[Rep[Key]]
    //   val keySet = hmStruct.keySet
    //   val size = hmStruct.size
    //   val elem = proceedHashMap(hm, key)
    //   readVar(keySet).remove(key)
    //   __assign(size, readVar(size) - 1)
    //   currentElement(elem)(hm)
    //   // ReadVal(readVar(elem).current)
    // }
    // case HashMapRemove(hm, k) if !isAggOp(hm) => { // hence, it's for HashAntiJoin
    //   implicit val manKey = hm.tp.typeArguments(0).asInstanceOf[TypeRep[Key]]
    //   implicit val manValue = hm.tp.typeArguments(1).typeArguments(0).asInstanceOf[TypeRep[Value]]
    //   val hmStruct = hashMapStructs(hm.asInstanceOf[Sym[Any]]).asInstanceOf[HashMapStruct[Key, Value]]
    //   val key = k.asInstanceOf[Rep[Key]]
    //   val keySet = hmStruct.keySet
    //   val size = hmStruct.size
    //   readVar(keySet).remove(key)
    //   ReadVal(__assign(size, readVar(size) - 1))
    //   // ReadVal(readVar(elem).current)
    // }
    // case SetHead(Def(ReadVar(v))) if checkForSet(v, x => getKind(x) != AggOpCase) => {
    //   val hm = keySetHashMaps(v)
    //   implicit val manKey = hm.tp.typeArguments(0).asInstanceOf[TypeRep[Key]]
    //   implicit val manValue = hm.tp.typeArguments(1).typeArguments(0).asInstanceOf[TypeRep[Value]]
    //   val hmStruct = hashMapStructs(hm.asInstanceOf[Sym[Any]]).asInstanceOf[HashMapStruct[Key, Value]]
    //   val keySet = readVar(hmStruct.keySet)
    //   ReadVal(keySet.head)
    // }
    // case SetRemove(Def(ReadVar(v)), k) if checkForSet(v, x => getKind(x) != AggOpCase) => {
    //   val hm = keySetHashMaps(v)
    //   implicit val manKey = hm.tp.typeArguments(0).asInstanceOf[TypeRep[Key]]
    //   implicit val manValue = hm.tp.typeArguments(1).typeArguments(0).asInstanceOf[TypeRep[Value]]
    //   val hmStruct = hashMapStructs(hm.asInstanceOf[Sym[Any]]).asInstanceOf[HashMapStruct[Key, Value]]
    //   val keySet = readVar(hmStruct.keySet)
    //   ReadVal(keySet.remove(k.asInstanceOf[Rep[Key]]))
    // }
    // case OptionGet(ret) if !isAggOpArrayBuffer(ret) => {
    //   ret.correspondingNode
    // }
    /*
    case HashMapGetOrElseUpdate(hm, k, v) if checkForHashSym(hm, x => getKind(x) == AggOpCase) => {
      implicit val manKey = hm.tp.typeArguments(0).asInstanceOf[TypeRep[Key]]
      implicit val manValue = hm.tp.typeArguments(1).typeArguments(0).asInstanceOf[TypeRep[Array[Double]]]
      val hmStruct = hashMapAggOpStructs(hm.asInstanceOf[Sym[Any]]).asInstanceOf[HashMapAggOpStruct[Key]]
      val elem = proceedHashMapAggOp(hm, k).asInstanceOf[Var[NextKeyContainer[Key, Array[Double]]]]
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
    case ArrayUpdate(arr, idx, v) if checkForArray(arr, x => getKind(x) == AggOpCase) => {
      val hm = arrayHashMaps(arr)
      implicit val manKey = hm.tp.typeArguments(0).asInstanceOf[TypeRep[Key]]
      implicit val manValue = hm.tp.typeArguments(1).typeArguments(0).asInstanceOf[TypeRep[Array[Double]]]
      val index = idx.asInstanceOf[Rep[Key]]
      val value = v.asInstanceOf[Rep[Double]]
      val struct = hashMapAggOpStructs(hm).asInstanceOf[HashMapAggOpStruct[Key]]
      struct.arr(index) = value
    }
    */
    case _ => super.transformDef(node)
  }).asInstanceOf[to.Def[T]]
}
