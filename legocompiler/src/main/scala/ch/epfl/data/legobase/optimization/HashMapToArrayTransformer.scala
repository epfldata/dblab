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
import pardis.shallow.utils.DefaultValue

object HashMapToArrayTransformer {
  def apply(generateCCode: Boolean) = new TransformerHandler {
    def apply[Lang <: Base, T: PardisType](context: Lang)(block: context.Block[T]): context.Block[T] = {
      new HashMapToArrayTransformer(context.asInstanceOf[LoweringLegoBase], generateCCode).optimize(block)
    }
  }
}

class HashMapToArrayTransformer(override val IR: LoweringLegoBase, val generateCCode: Boolean) extends Optimizer[LoweringLegoBase](IR) with StructCollector[LoweringLegoBase] {
  import IR._
  //import CNodes._
  //import CTypes._

  // Abstract over different keys and values
  trait Key
  trait Value

  sealed trait Kind
  case object ConstantKeyMap extends Kind
  case object HashJoinOpCase extends Kind
  case object AggOpCase extends Kind

  def optimize[T: TypeRep](node: Block[T]): to.Block[T] = {
    traverseBlock(node)
    transformProgram(node)
  }

  /* Traversals -- accumulate information about the maps used */
  val hashMapKinds = collection.mutable.Map[Sym[Any], Kind]()
  def getKind[T](hm: Rep[T]): Kind = hashMapKinds(hm.asInstanceOf[Sym[Any]])
  def isAggOp[T](hm: Rep[T]) = checkForHashSym(hm, x => getKind(x) == AggOpCase)
  def mapHasConstantKey[T](hm: Rep[T]): Boolean = hashMapKinds(hm.asInstanceOf[Sym[Any]]) == ConstantKeyMap

  val hashMapRemoveMap = collection.mutable.Map[Sym[Any], Sym[Any]]()
  val constantKeyMap = collection.mutable.Map[Sym[Any], Block[Any]]()
  val counterMap = new scala.collection.mutable.HashMap[Expression[_], (Var[Int], Var[Int])]()

  override def traverseDef(node: Def[_]): Unit = node match {
    case hmgeu @ HashMapGetOrElseUpdate(hm, key, value) => {
      super.traverseDef(node)
      key match {
        case Constant(c) =>
          hashMapKinds(hm.asInstanceOf[Sym[Any]]) = ConstantKeyMap
          constantKeyMap(hm.asInstanceOf[Sym[Any]]) = value.asInstanceOf[PardisBlock[Any]]
        case _ => hmgeu.typeB match {
          case c if c.name.contains("ArrayBuffer") => hashMapKinds(hm.asInstanceOf[Sym[Any]]) = HashJoinOpCase
          case c if c.name.contains("AGGRecord")   => hashMapKinds(hm.asInstanceOf[Sym[Any]]) = AggOpCase
          case _                                   => throw new Exception("Incorrect type " + hmgeu.typeB + " encountered in operators HashMap")
        }
      }
    }
    case _ => super.traverseDef(node)
  }

  override def traverseStm(stm: Stm[_]): Unit = stm match {
    case Stm(sym, HashMapApply(hm, e)) => hashMapRemoveMap += sym -> hm.asInstanceOf[Sym[Any]]
    case _                             => super.traverseStm(stm)
  }

  /* Transformation functions */
  // Takes care of proper lowering when key is constant. All other cases are handled in transformDef.
  override def transformStm(stm: Stm[_]): to.Stm[_] = stm match {
    case Stm(sym, hmgeu @ HashMapGetOrElseUpdate(hm, k, v)) if (mapHasConstantKey(hm)) =>
      val valType = hmgeu.typeB.asInstanceOf[PardisType[Any]]
      super.transformStm(Stm(sym.asInstanceOf[Sym[Any]], ReadVal(hm.asInstanceOf[Expression[Any]]))(valType))
    case s @ Stm(sym, hmr @ HashMapRemove(hm, key)) if (mapHasConstantKey(hm)) =>
      val valType = hmr.typeB.asInstanceOf[PardisType[Any]]
      super.transformStm(Stm(sym.asInstanceOf[Sym[Any]], ReadVal(hm)))
    case _ => super.transformStm(stm)
  }
  override def newSym[T: TypeRep](sym: Rep[T]): to.Sym[_] = {
    if (hashMapKinds.contains(sym.asInstanceOf[Sym[Any]]) && mapHasConstantKey(sym)) to.fresh(sym.tp.typeArguments(1)).copyFrom(sym.asInstanceOf[Sym[T]])
    else super.newSym[T](sym)
  }

  override def transformStmToMultiple(stm: Stm[_]): List[to.Stm[_]] = stm match {
    case Stm(sym, SetHead(s))                         => Nil
    case Stm(sym, SetRemove(s, k))                    => Nil
    case Stm(sym, HashMapKeySet(m))                   => Nil
    case Stm(sym, SetToSeq(s))                        => Nil
    case Stm(sym, SetVarArgNew(s))                    => Nil
    case Stm(sym, PardisNewVar(Def(SetVarArgNew(s)))) => Nil
    case Stm(sym, PardisAssign(PardisVar(z), y)) =>
      if (z.tp.name.contains("Set") && y.tp.name.contains("Set")) Nil
      else super.transformStmToMultiple(stm)
    case Stm(sym, PardisReadVar(PardisVar(z))) =>
      if (z.tp.name.contains("Set")) Nil
      else super.transformStmToMultiple(stm)
    case Stm(sym, hmn @ HashMapNew4(_, _)) if (mapHasConstantKey(sym)) =>
      val valType = sym.tp.typeArguments(1).asInstanceOf[PardisType[Any]]
      counterMap(sym) = (__newVar[Int](unit(1)), __newVar[Int](unit(0)))
      val valueBlock = constantKeyMap(sym)
      valueBlock.stmts.foreach(transformStmToMultiple)
      val valueRes = transformExp(valueBlock.res)(valueBlock.res.tp, valueBlock.res.tp)
      subst += sym -> valueRes
      Nil
    case _ => super.transformStmToMultiple(stm)
  }

  def key2Hash(key: Rep[Key], size: Rep[Int]) = {
    val z = infix_hashCode(key)(key.tp)
    // For C hashcode should be always positive -- optimize for C?
    val res = __ifThenElse(z >= unit(0), z % size, (-z % size))
    ReadVal(res)(res.tp)
  }
  def checkForHashSym[T](hm: Rep[T], p: Sym[Any] => Boolean): Boolean = p(hm.asInstanceOf[Sym[Any]])

  override def transformType[T: PardisType]: PardisType[Any] = ({
    val tp = typeRep[T]
    if (tp.name.startsWith("HashMap")) {
      if (tp.typeArguments(1).typeArguments.size != 0) typeArray(tp.typeArguments(1).typeArguments(0))
      else typeArray(tp.typeArguments(1))
    } else if (tp.name.contains("ArrayBuffer")) typePardisVariable(tp.typeArguments(0))
    else super.transformType[T]
  }).asInstanceOf[PardisType[Any]]

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
      case hmn @ HashMapNew4(_, size) => Some(size -> (hmn.typeB).asInstanceOf[PardisType[Any]])
      case _                          => None
    }
  }

  override def transformDef[T: PardisType](node: Def[T]): to.Def[T] = (node match {
    case ps @ PardisStruct(tag, elems, methods) =>
      val init = infix_asInstanceOf(Constant(null))(ps.tp)
      val alreadyInitialized = elems.find(f => f.name == "next").isDefined
      if (alreadyInitialized) node
      else PardisStruct(tag, elems ++ List(PardisStructArg("next", true, init)) ++ List(PardisStructArg("prev", true, init)), methods)(ps.tp)

    case hmn @ HashMapNews(size, typeB) =>
      //Console.err.printf(unit("Initializing map for type %s of size %d\n"), unit(typeB.toString), size)
      if (!typeB.isRecord)
        throw new Exception("To ensure Scala/C intercompatibility of HashMapToArrayTransformer, this " +
          " optimization can now support _only_ maps with values are of record type (see implementation " +
          " for more info). This is the case for all TPCH queries. If you see this message you should " +
          " modify the HashMapToArrayTransformer (you provided " + typeB + " type as value).")
      val arr = arrayNew(size)(typeB)
      val index = __newVar[Int](0)
      __whileDo(readVar(index) < size, {
        // Replacing malloc with structnew for Scala/C intercompatibility
        //val init = toAtom(Malloc(unit(1))(typeB))(typeB.asInstanceOf[PardisType[Pointer[Any]]])
        val s = getStructDef(typeB).get
        val structFields = s.fields.map(fld => PardisStructArg(fld.name, fld.mutable, {
          val dflt = if (generateCCode && fld.tpe.isArray) 0 else DefaultValue(fld.tpe.name)
          infix_asInstanceOf(unit(dflt)(fld.tpe))(fld.tpe)
        }))
        val init = toAtom(transformDef(PardisStruct(s.tag, structFields, List())(s.tp).asInstanceOf[to.Def[T]])(typeB.asInstanceOf[PardisType[T]]))(typeB.asInstanceOf[PardisType[T]])

        arrayUpdate(arr, readVar(index), init)
        val e = arrayApply(arr, readVar(index))(typeB)
        toAtom(PardisStructFieldSetter(e, "next", Constant(null)))
        __assign(index, readVar(index) + unit(1))
        unit()
      })
      counterMap(arr) = (__newVar[Int](unit(0)), __newVar[Int](unit(0)))
      //Console.err.printf(unit("DONE!\n"))
      ReadVal(arr)(arr.tp)

    case hmgeu @ HashMapGetOrElseUpdate(hm, k, v) if !isAggOp(hm) => {
      implicit val manValue = transformType(hm.tp)
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
      val bucket = __newVar(ArrayApply(lhm, hashKey)(manValue))(manValue)
      val e = __newVar(toAtom(PardisStructFieldGetter(ReadVar(bucket)(manValue), "next")(manValue))(manValue))(manValue)
      __whileDo({
        //val eValue = readVar(e)(e.tp)
        infix_!=(readVar(e)(e.tp), Constant(null)) && {
          val eValue = readVar(e)(e.tp)
          //val z = toAtom(PardisStructFieldGetter(e, "key")(key.tp))(key.tp)
          val extractor = getStructDef(eValue.tp).get.fields.find(f => f.tpe == key.tp).get

          val eeeef = field(eValue, extractor.name)(key.tp)
          infix_!=(eeeef.asInstanceOf[Expression[Any]], key.asInstanceOf[Expression[Any]])
        }
      },
        {
          __assign(e, toAtom(PardisStructFieldGetter(readVar(e), "next")(manValue))(manValue))
        })

      ReadVal(__ifThenElse(infix_==(readVar(e), Constant(null)), {
        //val next = toAtom(transformBlockTyped(v)(v.tp, manValue))(manValue)
        val tBlock = transformBlockTyped(v)(v.tp, manValue)
        tBlock.stmts.foreach(transformStmToMultiple)
        val next = tBlock.res
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

    case hmz @ HashMapSize(hm) if (!mapHasConstantKey(hm)) => {
      implicit val manValue = if (isAggOp(hm))
        hm.tp.typeArguments(1).asInstanceOf[TypeRep[Value]]
      else
        hm.tp.typeArguments(1).typeArguments(0).asInstanceOf[TypeRep[Value]]
      val size = arrayLength(transformExp(hm)(hm.tp, typeArray(hmz.typeB)))
      val lhm = transformExp(hm)(hm.tp, typeArray(manValue))
      val counter = getSizeCounterMap(hm)
      ReadVar(counter)
    }

    case HashMapSize(hm) if (mapHasConstantKey(hm)) =>
      // This breaks with partual evaluation
      val sizeCounter = counterMap(hm)._1
      val v = readVar(sizeCounter)
      __assign(sizeCounter, readVar(sizeCounter) - unit(1))
      ReadVal(v)

    case aba @ ArrayBufferAppend(a @ Def(rv @ PardisReadVar(f @ PardisVar(ab))), e) =>
      implicit val manValue = a.tp.typeArguments(0).asInstanceOf[TypeRep[Value]]
      val head = transformExp(a)(a.tp, manValue) //.asInstanceOf[Expression[Pointer[Any]]]

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
