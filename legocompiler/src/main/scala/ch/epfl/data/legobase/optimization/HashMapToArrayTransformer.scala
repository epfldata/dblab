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

  sealed trait Kind
  case object HashJoinOpCase extends Kind
  case object AggOpCase extends Kind

  val hashMapKinds = collection.mutable.Map[Sym[Any], Kind]()

  def optimize[T: TypeRep](node: Block[T]): to.Block[T] = {
    traverseBlock(node)
    transformProgram(node)
  }

  /* Here we identify which kind of hash map it is */
  override def traverseDef(node: Def[_]): Unit = node match {
    case hmgeu @ HashMapGetOrElseUpdate(hm, _, _) => {
      hmgeu.typeB match {
        case c if c.name.contains("ArrayBuffer") => hashMapKinds(hm.asInstanceOf[Sym[Any]]) = HashJoinOpCase
        case c if c.name.contains("AGGRecord")   => hashMapKinds(hm.asInstanceOf[Sym[Any]]) = AggOpCase
        case _                                   => throw new Exception("Incorrect type " + hmgeu.typeB + " encountered in operators HashMap")
      }
    }
    case _ => super.traverseDef(node)
  }

  override def transformStmToMultiple(stm: Stm[_]): List[to.Stm[_]] = stm match {
    case Stm(sym, ArrayBufferNew2())            => Nil
    case Stm(sym, SetHead(s))                   => Nil
    case Stm(sym, SetRemove(s, k))              => Nil
    case Stm(sym, HashMapKeySet(m))             => Nil
    case Stm(sym, SetToSeq(s))                  => Nil
    case Stm(sym, SetNew(s))                    => Nil
    case Stm(sym, PardisNewVar(Def(SetNew(s)))) => Nil
    case _                                      => super.transformStmToMultiple(stm)
  }

  override def transformStm(stm: Stm[_]): to.Stm[_] = stm match {
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

  trait Key
  trait Value

  def key2Hash(key: Rep[Key], size: Rep[Int]) = {
    infix_hashCode(key)(key.tp) % size
  }

  def checkForHashSym[T](hm: Rep[T], p: Sym[Any] => Boolean): Boolean = p(hm.asInstanceOf[Sym[Any]])

  def isAggOp[T](hm: Rep[T]) = checkForHashSym(hm, x => getKind(x) == AggOpCase)

  override def transformType[T: PardisType]: PardisType[Any] = ({
    val tp = typeRep[T]
    if (tp.name.startsWith("HashMap")) {
      if (tp.typeArguments(1).typeArguments.size != 0) typeArray(tp.typeArguments(1).typeArguments(0))
      else typeArray(tp.typeArguments(1))
    } else if (tp.name.contains("ArrayBuffer")) typePardisVariable(tp.typeArguments(0))
    else super.transformType[T]
  }).asInstanceOf[PardisType[Any]]

  val counterMap = new scala.collection.mutable.HashMap[Expression[_], (Var[Int], Var[Int])]()

  def getSizeCounterMap[A, B](hm: Expression[HashMap[A, B]]) = hm match {
    case Def(ReadVal(x)) => counterMap(x)._1
    case _               => throw new Exception("Counter not found for lowered HashMap " + hm)
  }
  def getRemoveIndexCounterMap[A, B](hm: Expression[HashMap[A, B]]) = hm match {
    case Def(ReadVal(x)) => counterMap(x)._2
    case _               => throw new Exception("Counter not found for lowered HashMap " + hm)
  }

  override def transformDef[T: PardisType](node: Def[T]): to.Def[T] = (node match {
    case ps @ PardisStruct(tag, elems, methods) =>
      val init = infix_asInstanceOf(Constant(null))(ps.tp)
      PardisStruct(tag, elems ++ List(PardisStructArg("next", true, init)), methods)

    case hmn @ HashMapNew3(_, size) =>
      printf(unit("Initializing map for type %s of size %d\n"), unit(hmn.typeB.toString), size)
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
      counterMap += arr -> (__newVar[Int](unit(0)), __newVar[Int](unit(0)))
      printf(unit("DONE!\n"))
      ReadVal(arr)(arr.tp)

    case hmgeu @ HashMapGetOrElseUpdate(hm, k, v) if !isAggOp(hm) => {
      implicit val manValue = transformType(hm.tp).asInstanceOf[TypeRep[Value]]
      val key = k.asInstanceOf[Rep[Key]]
      val size = arrayLength(transformExp(hm)(hm.tp, typeArray(hmgeu.typeB)))
      val hashKey = key2Hash(key, size)
      val lhm = transformExp(hm)(hm.tp, typeArray(manValue))
      val ptrValue = typePointer(manValue)
      val bucket = __newVar(ArrayApply(lhm, hashKey)(manValue))(manValue)
      ReadVar(bucket)(manValue)
    }
    case hmgeu @ HashMapGetOrElseUpdate(hm, k, v) if isAggOp(hm) => {
      implicit val manValue = transformType(hm.tp).typeArguments(0).asInstanceOf[TypeRep[Value]]
      val key = k.asInstanceOf[Rep[Key]]
      val size = arrayLength(transformExp(hm)(hm.tp, typeArray(hmgeu.typeB)))
      val hashKey = key2Hash(key, size)
      val lhm = transformExp(hm)(hm.tp, typeArray(manValue))
      val counter = getSizeCounterMap(hm)
      val ptrValue = typePointer(manValue)
      val bucket = __newVar(ArrayApply(lhm, hashKey)(manValue))(manValue)
      ReadVal(__ifThenElse(infix_==(PardisStructFieldGetter(ReadVar(bucket)(manValue), "next")(manValue), Constant(null)), {
        val next = toAtom(transformBlockTyped(v)(v.tp, manValue))(manValue)
        __assign(counter, readVar(counter) + unit(1))
        toAtom(PardisStructFieldSetter(ReadVar(bucket)(manValue), "next", next))
        ReadVal(next)(manValue)
      }, PardisStructFieldGetter(ReadVar(bucket)(manValue), "next")(manValue))(manValue))(manValue)
    }
    case hmcnt @ HashMapContains(hm, k) => {
      implicit val manValue = hm.tp.typeArguments(1).typeArguments(0).asInstanceOf[TypeRep[Value]]
      val key = k.asInstanceOf[Rep[Key]]
      val size = arrayLength(transformExp(hm)(hm.tp, typeArray(hmcnt.typeB)))
      val hashKey = key2Hash(key, size)
      val lhm = transformExp(hm)(hm.tp, typeArray(manValue))
      val bucket = ArrayApply(lhm, hashKey)(manValue)
      val elem = PardisStructFieldGetter(bucket, "next")(manValue)
      ReadVal(infix_!=(elem, unit(null)))
    }
    case hmapp @ HashMapApply(hm, k) => {
      implicit val manValue = hm.tp.typeArguments(1).typeArguments(0).asInstanceOf[TypeRep[Value]]
      val key = k.asInstanceOf[Rep[Key]]
      val size = arrayLength(transformExp(hm)(hm.tp, typeArray(hmapp.typeB)))
      val hashKey = key2Hash(key, size)
      val lhm = transformExp(hm)(hm.tp, typeArray(manValue))
      val bucket = __newVar(ArrayApply(lhm, hashKey)(manValue))(manValue)
      ReadVar(bucket)
    }
    case hmr @ HashMapRemove(hm, k) if isAggOp(hm) => {
      implicit val manValue = hm.tp.typeArguments(1).typeArguments(0).asInstanceOf[TypeRep[Value]]
      val size = arrayLength(transformExp(hm)(hm.tp, typeArray(hmr.typeB)))
      val lhm = transformExp(hm)(hm.tp, typeArray(manValue))
      val numElems = getSizeCounterMap(hm)
      val removeIndex = getRemoveIndexCounterMap(hm)
      val bucket = __newVar(ArrayApply(lhm, ReadVar(removeIndex))(manValue))(manValue)
      __whileDo(infix_==(PardisStructFieldGetter(bucket, "next")(manValue), Constant(null)), {
        __assign(removeIndex, readVar(removeIndex) + unit(1))
        __assign(bucket, ArrayApply(lhm, ReadVar(removeIndex))(manValue))(manValue)
      })
      __assign(numElems, readVar(numElems) - unit(1))
      val headNext = toAtom(PardisStructFieldGetter(bucket, "next")(manValue))
      val headNextNext = toAtom(PardisStructFieldGetter(headNext, "next")(manValue))
      toAtom(PardisStructFieldSetter(bucket, "next", headNextNext))
      ReadVal(headNext)
    }
    case hmz @ HashMapSize(hm) => {
      implicit val manValue = hm.tp.typeArguments(1).typeArguments(0).asInstanceOf[TypeRep[Value]]
      val size = arrayLength(transformExp(hm)(hm.tp, typeArray(hmz.typeB)))
      val lhm = transformExp(hm)(hm.tp, typeArray(manValue))
      val counter = getSizeCounterMap(hm)
      ReadVar(counter)
    }

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
    case _ => super.transformDef(node)
  }).asInstanceOf[to.Def[T]]
}
