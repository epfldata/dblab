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

class HashMapToArrayTransformer(override val IR: LoweringLegoBase) extends Optimizer[LoweringLegoBase](IR) with StructCollector[LoweringLegoBase] {
  import IR._
  import CNodes._
  import CTypes._

  sealed trait Kind
  case object HashJoinOpCase extends Kind
  case object AggOpCase extends Kind

  val hashMapKinds = collection.mutable.Map[Sym[Any], Kind]()
  val hashMapRemoveMap = collection.mutable.Map[Sym[Any], Sym[Any]]()

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

  override def traverseStm(stm: Stm[_]): Unit = stm match {
    case Stm(sym, HashMapApply(hm, e)) => hashMapRemoveMap += sym -> hm.asInstanceOf[Sym[Any]]
    case _                             => super.traverseStm(stm)
  }

  override def transformStmToMultiple(stm: Stm[_]): List[to.Stm[_]] = stm match {
    case Stm(sym, SetHead(s))                   => Nil
    case Stm(sym, SetRemove(s, k))              => Nil
    case Stm(sym, HashMapKeySet(m))             => Nil
    case Stm(sym, SetToSeq(s))                  => Nil
    case Stm(sym, SetNew(s))                    => Nil
    case Stm(sym, PardisNewVar(Def(SetNew(s)))) => Nil
    case Stm(sym, PardisAssign(PardisVar(z), y)) =>
      if (z.tp.name.contains("Set") && y.tp.name.contains("Set")) Nil
      else super.transformStmToMultiple(stm)
    case Stm(sym, PardisReadVar(PardisVar(z))) =>
      if (z.tp.name.contains("Set")) Nil
      else super.transformStmToMultiple(stm)
    case _ => super.transformStmToMultiple(stm)
  }

  def getKind[T](hm: Rep[T]): Kind = hashMapKinds(hm.asInstanceOf[Sym[Any]])

  trait Key
  trait Value

  // Duplication of code with Scala2C -- refactoring needed
  def getStructHashMethod[A: PardisType] = {
    val structDef = getStructDef[A].get
    val hashMethod = transformDef(structDef.methods.find(_.name == "hash").get.body.asInstanceOf[PardisLambda[Any, Int]])
    toAtom(hashMethod)(hashMethod.tp)
  }
  def getStructEqualsMethod[A: PardisType] = {
    val structDef = getStructDef[A].get
    val equalsMethod = transformDef(structDef.methods.find(_.name == "equals").get.body.asInstanceOf[PardisLambda2[Any, Any, Boolean]])
    toAtom(equalsMethod)(equalsMethod.tp)
  }

  def key2Hash(key: Rep[Key], size: Rep[Int]) = {
    val k = getHashMethod(key.tp)(key.tp)
    __app(k).apply(key) % size
  }

  def int_hash = (doLambda((s: Rep[Int]) => s)).asInstanceOf[Rep[Any => Int]]
  def int_eq = (doLambda2((s1: Rep[Int], s2: Rep[Int]) => infix_==(s1, s2))).asInstanceOf[Rep[(Any, Any) => Boolean]]
  def double_hash = (doLambda((s: Rep[Double]) => infix_asInstanceOf(s)(IntType))).asInstanceOf[Rep[Any => Int]]
  def double_eq = (doLambda2((s1: Rep[Double], s2: Rep[Double]) => infix_==(s1, s2))).asInstanceOf[Rep[(Any, Any) => Boolean]]
  def optimalString_hash = (doLambda((t: Rep[OptimalString]) => { infix_hashCode(t) })).asInstanceOf[Rep[Any => Int]]
  def optimalString_eq = (doLambda2((s1: Rep[OptimalString], s2: Rep[OptimalString]) => OptimalString$eq$eq$eq(s1, s2))).asInstanceOf[Rep[(Any, Any) => Boolean]]

  def getEqualsMethod[A: PardisType](k: PardisType[A]) = k match {
    case c if k.name == "Int"           => int_eq
    case c if k.name == "Double"        => double_eq
    case c if k.isRecord                => getStructEqualsMethod(k)
    case c if k.name == "String"        => optimalString_eq
    case c if k.name == "OptimalString" => optimalString_eq
  }

  def getHashMethod[A: PardisType](k: PardisType[A]): Rep[Any => Int] = k match {
    case c if k.name == "Int"           => int_hash
    case c if k.name == "Double"        => double_hash
    case c if k.isRecord                => getStructHashMethod(k)
    case c if k.name == "String"        => optimalString_hash
    case c if k.name == "OptimalString" => optimalString_hash
  }

  def checkForHashSym[T](hm: Rep[T], p: Sym[Any] => Boolean): Boolean = p(hm.asInstanceOf[Sym[Any]])

  def isAggOp[T](hm: Rep[T]) = checkForHashSym(hm, x => getKind(x) == AggOpCase)

  override def transformType[T: PardisType]: PardisType[Any] = ({
    val tp = typeRep[T]
    if (tp.name.startsWith("HashMap")) {
      if (tp.typeArguments(1).typeArguments.size != 0) typeArray(tp.typeArguments(1).typeArguments(0))
      else typeArray(tp.typeArguments(1))
    } else if (tp.name.contains("ArrayBuffer")) { System.out.println(tp); typePardisVariable(tp.typeArguments(0)) }
    else super.transformType[T]
  }).asInstanceOf[PardisType[Any]]

  val counterMap = new scala.collection.mutable.HashMap[Expression[_], (Var[Int], Var[Int])]()

  def getSizeCounterMap[A, B](hm: Expression[HashMap[A, B]]) = hm match {
    case Def(ReadVal(x)) => counterMap(x)._1
    case dflt @ _        => null
  }
  def getRemoveIndexCounterMap[A, B](hm: Expression[HashMap[A, B]]) = hm match {
    case Def(ReadVal(x)) => counterMap(x)._2
    case _               => throw new Exception("RemoveCounter not found for lowered HashMap " + hm)
  }

  object HashMapNews {
    def unapply[T](node: Def[T]): Option[(Rep[Int], PardisType[Any])] = node match {
      case hmn @ HashMapNew3(_, size) => Some(size -> hmn.typeB)
      case hmn @ HashMapNew4(_, size) => Some(size -> /*ArrayBufferType*/ (hmn.typeB).asInstanceOf[PardisType[Any]])
      case _                          => None
    }
  }

  override def transformDef[T: PardisType](node: Def[T]): to.Def[T] = (node match {
    case ps @ PardisStruct(tag, elems, methods) =>
      val init = infix_asInstanceOf(Constant(null))(ps.tp)
      // TODO: Prev does not need to be everywhere, only for queries using
      // AntiJoin
      PardisStruct(tag, elems ++ List(PardisStructArg("next", true, init)) ++ List(PardisStructArg("prev", true, init)), methods)(ps.tp)

    case hmn @ HashMapNews(size, typeB) =>
      printf(unit("Initializing map for type %s of size %d\n"), unit(typeB.toString), size)
      val arr = arrayNew(size)(typeB)
      val index = __newVar[Int](0)
      __whileDo(readVar(index) < size, {
        System.out.println(typeB)
        val init = toAtom(Malloc(unit(1))(typeB))(typeB.asInstanceOf[PardisType[Pointer[Any]]])
        arrayUpdate(arr, readVar(index), init)
        val e = arrayApply(arr, readVar(index))(typeB)
        toAtom(PardisStructFieldSetter(e, "next", Constant(null)))
        __assign(index, readVar(index) + unit(1))
        unit()
      })
      counterMap(arr) = (__newVar[Int](unit(0)), __newVar[Int](unit(0)))
      System.out.println(counterMap.mkString("\n"))
      printf(unit("DONE!\n"))
      ReadVal(arr)(arr.tp)

    case hmgeu @ HashMapGetOrElseUpdate(hm, k, v) if !isAggOp(hm) => {
      implicit val manValue = transformType(hm.tp) //.asInstanceOf[TypeRep[Value]]
      val key = k.asInstanceOf[Rep[Key]]
      val size = arrayLength(transformExp(hm)(hm.tp, typeArray(hmgeu.typeB)))
      val hashKey = key2Hash(key, size)
      val lhm = transformExp(hm)(hm.tp, manValue)
      val et = manValue.typeArguments(0).asInstanceOf[PardisType[Any]]
      val bucket = __newVar(arrayApply(lhm.asInstanceOf[Expression[Array[Any]]], hashKey)(et))(et)
      val counter = getSizeCounterMap(hm)
      val bucketNext = toAtom(PardisStructFieldGetter(readVar(bucket)(et), "next")(et))(et)
      if (counter != null) {
        __ifThenElse(infix_==(bucketNext, Constant(null)), {
          __assign(counter, readVar(counter) + unit(1))
        }, unit())
      }
      ReadVar(bucket)(manValue.typeArguments(0).asInstanceOf[PardisType[Any]])
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
      val e = __newVar(toAtom(PardisStructFieldGetter(ReadVar(bucket)(manValue), "next")(manValue))(manValue))(manValue)
      val ek = __newVar(toAtom(PardisStructFieldGetter(e, "key")(key.tp))(key.tp).asInstanceOf[Expression[Value]])(key.tp.asInstanceOf[TypeRep[Value]])
      val eq: Rep[(Any, Any) => Boolean] = getEqualsMethod(k.tp)
      __whileDo(boolean$amp$amp(infix_!=(e, Constant(null)), !__app(eq).apply(readVar(ek)(key.tp.asInstanceOf[TypeRep[Value]]), k)), {
        __assign(e, toAtom(PardisStructFieldGetter(readVar(e), "next")(manValue))(manValue))
        __assign(ek, toAtom(PardisStructFieldGetter(readVar(e), "key")(key.tp))(key.tp).asInstanceOf[Expression[Value]])
      })

      ReadVal(__ifThenElse(infix_==(readVar(e), Constant(null)), {
        val next = toAtom(transformBlockTyped(v)(v.tp, manValue))(manValue)
        __assign(counter, readVar(counter) + unit(1))
        val headNext = toAtom(PardisStructFieldGetter(ReadVar(bucket)(manValue), "next")(manValue))
        toAtom(PardisStructFieldSetter(next, "next", headNext))
        toAtom(PardisStructFieldSetter(ReadVar(bucket)(manValue), "next", next))
        ReadVal(next)(manValue)
      }, ReadVar(e))(manValue))(manValue)
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
      // implicit val manValue = hm.tp.typeArguments(1).typeArguments(0).asInstanceOf[TypeRep[Value]]
      implicit val manValue = hm.tp.typeArguments(1).asInstanceOf[TypeRep[Value]]
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
      val headNext = __newVar(toAtom(PardisStructFieldGetter(bucket, "next")(manValue)))(manValue)
      val headNextNext = toAtom(PardisStructFieldGetter(headNext, "next")(manValue))
      toAtom(PardisStructFieldSetter(bucket, "next", headNextNext))
      ReadVar(headNext)
    }
    case hmr @ HashMapRemove(hm, k) if !isAggOp(hm) => {
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
      __assign(removeIndex, readVar(removeIndex) + unit(1))
      ReadVar(bucket)
    }

    case hmz @ HashMapSize(hm) => {
      implicit val manValue = if (isAggOp(hm))
        hm.tp.typeArguments(1).asInstanceOf[TypeRep[Value]]
      else
        hm.tp.typeArguments(1).typeArguments(0).asInstanceOf[TypeRep[Value]]
      val size = arrayLength(transformExp(hm)(hm.tp, typeArray(hmz.typeB)))
      val lhm = transformExp(hm)(hm.tp, typeArray(manValue))
      val counter = getSizeCounterMap(hm)
      ReadVar(counter)
    }

    case aba @ ArrayBufferAppend(a @ Def(rv @ PardisReadVar(f @ PardisVar(ab))), e) =>
      implicit val manValue = a.tp.typeArguments(0).asInstanceOf[TypeRep[Value]]
      val head = transformExp(a)(a.tp, manValue).asInstanceOf[Expression[Pointer[Any]]]

      val headNext = toAtom(PardisStructFieldGetter(head, "next")(manValue))
      // Connect e
      toAtom(PardisStructFieldSetter(e, "next", headNext))
      toAtom(PardisStructFieldSetter(e, "prev", head))
      // Update head
      toAtom(PardisStructFieldSetter(head, "next", e))
      // Update headNext
      __ifThenElse(infix_!=(headNext, Constant(null)), {
        toAtom(PardisStructFieldSetter(headNext, "prev", e))
      }, unit())
      ReadVal(Constant(true))

    case ArrayBufferApply(a @ Def(rv @ PardisReadVar(f @ PardisVar(ab))), idx) =>
      implicit val manValue = ab.tp.typeArguments(0).asInstanceOf[TypeRep[Value]]
      val aVarNext = toAtom(PardisStructFieldGetter(ab, "next")(manValue))(manValue)
      __assign(f.asInstanceOf[PardisVar[Any]], aVarNext)
      ReadVar(f)
    case ArrayBufferRemove(a @ Def(rv @ PardisReadVar(f @ PardisVar(ab))), y) =>
      implicit val manValue = ab.tp.typeArguments(0).asInstanceOf[TypeRep[Value]]
      //printf(unit("Removing from list\n"))
      val aVarNext = toAtom(PardisStructFieldGetter(ab, "next")(manValue))
      val aVarPrev = toAtom(PardisStructFieldGetter(ab, "prev")(manValue))
      toAtom(PardisStructFieldSetter(aVarPrev, "next", aVarNext))
      __ifThenElse(infix_!=(aVarNext, Constant(null)), {
        PardisStructFieldSetter(aVarNext, "prev", aVarPrev)
      }, unit())
      __ifThenElse(infix_==(aVarNext, Constant(null)), {
        val numElems = getSizeCounterMap(hashMapRemoveMap(a.asInstanceOf[Sym[Any]]).asInstanceOf[Rep[HashMap[Any, Any]]])
        __assign(numElems, readVar(numElems) - unit(1))
      }, unit())
      ReadVal(Constant(true))
    case ArrayBufferFoldLeft(a, cnt, Def(Lambda2(fun, input1, input2, o))) => a.correspondingNode match {
      case rv @ PardisReadVar(f @ PardisVar(ab)) =>
        var agg = __newVar(cnt)
        implicit val manValue = ab.tp.typeArguments(0).asInstanceOf[TypeRep[Value]]
        val init = readVar(f.asInstanceOf[Var[Value]])(manValue)
        __whileDo(infix_!=(f, Constant(null)), {
          val z = fun(agg, readVar(f.asInstanceOf[Var[Value]])(manValue))
          __assign(agg, z)
          // Advance pointer
          val aVarNext = toAtom(PardisStructFieldGetter(ab, "next")(manValue))(manValue)
          __assign(f.asInstanceOf[PardisVar[Any]], aVarNext)
        })
        __assign(f.asInstanceOf[PardisVar[Any]], init)
        ReadVar(agg)
      case _ => throw new Exception("Internal BUG in HashMapToArrayTransformer. ArrayBuffer should be a Var");
    }
    case ArrayBufferMinBy(a, f @ Def(Lambda(fun, input, o))) => a.correspondingNode match {
      case rv @ PardisReadVar(f @ PardisVar(ab)) =>
        implicit val manValue = ab.tp.typeArguments(0).asInstanceOf[TypeRep[Value]]
        // Skip bucket
        __assign(f.asInstanceOf[PardisVar[Any]], toAtom(PardisStructFieldGetter(ab, "next")(manValue))(manValue))
        // Set first element
        var min = __newVar(readVar(f.asInstanceOf[Var[Value]])(manValue))
        var minBy = __newVar(fun(min).asInstanceOf[Expression[Int]])
        // Advance pointer
        __assign(f.asInstanceOf[PardisVar[Any]], toAtom(PardisStructFieldGetter(ab, "next")(manValue))(manValue))
        // Process elements as far as we haven't reached NULL
        __whileDo(infix_!=(f, Constant(null)), {
          val eminBy = fun(readVar(f.asInstanceOf[Var[Value]])(manValue)).asInstanceOf[Expression[Int]]
          __ifThenElse(eminBy < readVar(minBy), {
            __assign(min, f.asInstanceOf[Var[Value]])
            __assign(minBy, eminBy)
          }, unit())
          // Advance pointer
          val aVarNext = toAtom(PardisStructFieldGetter(ab, "next")(manValue))(manValue)
          __assign(f.asInstanceOf[PardisVar[Any]], aVarNext)
        })
        ReadVar(min)
      case _ => throw new Exception("Internal BUG in HashMapToArrayTransformer. ArrayBuffer should be a Var");
    }
    case ArrayBufferSize(a @ Def(rv @ PardisReadVar(f @ PardisVar(ab)))) =>
      implicit val manValue = ab.tp.typeArguments(0).asInstanceOf[TypeRep[Value]]
      val init = readVar(f.asInstanceOf[Var[Value]])(manValue)
      val count = __newVar[Int](unit(0))
      __assign(f.asInstanceOf[Var[Value]], toAtom(PardisStructFieldGetter(readVar(f.asInstanceOf[Var[Value]])(manValue), "next")(manValue))(manValue))
      __whileDo(infix_!=(f, Constant(null)), {
        __assign(count, readVar(count) + unit(1))
        __assign(f.asInstanceOf[Var[Value]], toAtom(PardisStructFieldGetter(readVar(f.asInstanceOf[Var[Value]])(manValue), "next")(manValue))(manValue))
      })
      __assign(f.asInstanceOf[PardisVar[Any]], init)
      ReadVar(count)

    case OptionGet(x) => x.correspondingNode

    case _            => super.transformDef(node)
  }).asInstanceOf[to.Def[T]]
}
