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
    case Stm(sym, PardisAssign(PardisVar(z), y)) =>
      if (z.tp.name.contains("Set") && y.tp.name.contains("Set")) Nil
      else super.transformStmToMultiple(stm)
    case _ => super.transformStmToMultiple(stm)
  }

  override def transformStm(stm: Stm[_]): to.Stm[_] = {
    def __transformStm[A: PardisType](a: Def[A], sym: Sym[Any]) = a match {
      case rv @ PardisReadVar(f @ PardisVar(ab)) =>
        implicit val manValue = a.tp.asInstanceOf[TypeRep[Value]]
        val newSym = fresh[Boolean]
        val z = ReadVal(infix_==(PardisStructFieldGetter(ab, "next")(manValue), unit(null)))(BooleanType)
        val s = Stm(newSym, z)
        subst += sym -> newSym
        reflectStm(s)
        s
    }
    stm match {
      case Stm(sym, Int$greater$eq4(e1, Def(ab @ ArrayBufferSize(a)))) => __transformStm(a.correspondingNode, sym)
      case Stm(sym, Equal(Def(ab @ ArrayBufferSize(a)), e1))           => __transformStm(a.correspondingNode, sym)
      case Stm(sym, Int$less4(e1, Def(ab @ ArrayBufferSize(a)))) => a.correspondingNode match {
        case rv @ PardisReadVar(f @ PardisVar(ab)) =>
          implicit val manValue = a.tp.asInstanceOf[TypeRep[Value]]
          val newSym = fresh[Boolean]
          val z = ReadVal(infix_!=(PardisStructFieldGetter(ab, "next")(manValue), unit(null)))(BooleanType)
          val s = Stm(newSym, z)
          subst += sym -> newSym
          reflectStm(s)
          s
      }
      case _ => super.transformStm(stm)
    }
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

  def string_hash = (doLambda((s: Rep[String]) => { unit(5) })).asInstanceOf[Rep[Any => Int]]
  def string_eq = (doLambda2((s1: Rep[String], s2: Rep[String]) => infix_==(strcmp(s1, s2), 0))).asInstanceOf[Rep[(Any, Any) => Boolean]]
  def int_hash = (doLambda((s: Rep[Int]) => s)).asInstanceOf[Rep[Any => Int]]
  def int_eq = (doLambda2((s1: Rep[Int], s2: Rep[Int]) => infix_==(s1, s2))).asInstanceOf[Rep[(Any, Any) => Boolean]]
  def optimalString_hash = (doLambda((t: Rep[OptimalString]) => {
    val len = toAtom(OptimalStringLength(t.asInstanceOf[Expression[OptimalString]]))(IntType)
    val idx = __newVar[Int](0)
    val h = __newVar[Int](0)
    __whileDo(readVar(idx) < len, {
      __assign(h, readVar(h) + OptimalStringApply(t.asInstanceOf[Expression[OptimalString]], 0) + OptimalStringApply(t.asInstanceOf[Expression[OptimalString]], readVar(idx)))
      __assign(idx, readVar(idx) + unit(1));
    })
    ReadVar(h)
  })).asInstanceOf[Rep[Any => Int]]
  def optimalString_eq = (doLambda2((s1: Rep[OptimalString], s2: Rep[OptimalString]) => OptimalString$eq$eq$eq(s1, s2))).asInstanceOf[Rep[(Any, Any) => Boolean]]

  def getEqualsMethod[A: PardisType](k: PardisType[A]) = k match {
    case c if k.name == "Int"           => int_eq
    case c if k.isRecord                => getStructEqualsMethod(k)
    case c if k.name == "String"        => string_eq
    case c if k.name == "OptimalString" => optimalString_eq
  }

  def getHashMethod[A: PardisType](k: PardisType[A]): Rep[Any => Int] = k match {
    case c if k.name == "Int"           => int_hash
    case c if k.isRecord                => getStructHashMethod(k)
    case c if k.name == "String"        => string_hash
    case c if k.name == "OptimalString" => optimalString_hash
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
    case dflt @ _        => null
  }
  def getRemoveIndexCounterMap[A, B](hm: Expression[HashMap[A, B]]) = hm match {
    case Def(ReadVal(x)) => counterMap(x)._2
    case _               => throw new Exception("RemoveCounter not found for lowered HashMap " + hm)
  }

  override def transformDef[T: PardisType](node: Def[T]): to.Def[T] = (node match {
    case ps @ PardisStruct(tag, elems, methods) =>
      val init = infix_asInstanceOf(Constant(null))(ps.tp)
      // TODO: Prev does not need to be everywhere, only for queries using
      // AntiJoin
      PardisStruct(tag, elems ++ List(PardisStructArg("next", true, init)) ++ List(PardisStructArg("prev", true, init)), methods)

    case hmn @ HashMapNew3(_, size) =>
      printf(unit("Initializing map for type %s of size %d\n"), unit(hmn.typeB.toString), size)
      val arr = arrayNew(size)(hmn.typeB)
      val index = __newVar[Int](0)
      __whileDo(readVar(index) < size, {
        System.out.println(hmn.typeB)
        val init = toAtom(Malloc(unit(1))(hmn.typeB))(hmn.typeB.asInstanceOf[PardisType[Pointer[Any]]])
        arrayUpdate(arr, readVar(index), init)
        val e = arrayApply(arr, readVar(index))(hmn.typeB)
        toAtom(PardisStructFieldSetter(e, "next", Constant(null)))
        __assign(index, readVar(index) + unit(1))
        unit()
      })
      counterMap += arr -> (__newVar[Int](unit(0)), __newVar[Int](unit(0)))
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
      val ek = __newVar(toAtom(PardisStructFieldGetter(e, "key")(key.tp))(key.tp).asInstanceOf[Expression[Value]])
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
      printf(unit("CHECKPOINT HASHMAP REMOVE START WHILE\n"))
      __whileDo(infix_==(PardisStructFieldGetter(bucket, "next")(manValue), Constant(null)), {
        __assign(removeIndex, readVar(removeIndex) + unit(1))
        __assign(bucket, ArrayApply(lhm, ReadVar(removeIndex))(manValue))(manValue)
      })
      __assign(numElems, readVar(numElems) - unit(1))
      printf(unit("CHECKPOINT HASHMAP REMOVE END WHILE -- SIZE NOW IS %d\n"),
        readVar(numElems)(IntType))
      __assign(removeIndex, readVar(removeIndex) + unit(1))
      ReadVar(bucket)
    }

    case hmz @ HashMapSize(hm) => {
      implicit val manValue = hm.tp.typeArguments(1).typeArguments(0).asInstanceOf[TypeRep[Value]]
      val size = arrayLength(transformExp(hm)(hm.tp, typeArray(hmz.typeB)))
      val lhm = transformExp(hm)(hm.tp, typeArray(manValue))
      val counter = getSizeCounterMap(hm)
      //printf(unit("size of map of " + manValue + " is now %d\n"), readVar(counter)(IntType))
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

    case ArrayBufferRemove(a, y) => a.correspondingNode match {
      case rv @ PardisReadVar(f @ PardisVar(ab)) =>
        implicit val manValue = ab.tp.typeArguments(0).asInstanceOf[TypeRep[Value]]
        val aVarNext = toAtom(PardisStructFieldGetter(ab, "next")(manValue))
        val aVarPrev = toAtom(PardisStructFieldGetter(ab, "prev")(manValue))
        PardisStructFieldSetter(aVarPrev, "next", aVarNext)
      case _ => throw new Exception("Internal BUG in HashMapToArrayTransformer. ArrayBuffer should be a Var");
    }
    //case ArrayBufferApply(Def(OptionGet(x)), idx) => throw new Exception()
    case OptionGet(x) => x.correspondingNode

    case _            => super.transformDef(node)
  }).asInstanceOf[to.Def[T]]
}
