package ch.epfl.data
package dblab.legobase
package optimization
package c

import sc.pardis.ir._
import sc.pardis.types._
import sc.pardis.types.PardisTypeImplicits._
import dblab.legobase.deep._
import sc.pardis.optimization._
import scala.language.implicitConversions
import sc.cscala.CLangTypesDeep._
import sc.cscala.GLibTypes._

/**
 * Transforms Scala collection classes into a C GLib types.
 *
 * HashMap is converted to g_hash_table.
 * Set is converted to g_list
 * TreeSet is converted to g_tree
 * ArrayBuffer is converted to g_array
 *
 * @param IR the polymorphic embedding trait which contains the reified program.
 */
class ScalaCollectionsToGLibTransfomer(override val IR: LoweringLegoBase) extends RecursiveRuleBasedTransformer[LoweringLegoBase](IR) with CTransformer {
  import IR._
  import CNodes._
  import CTypes._
  import GHashTableHeader._
  import GListHeader._
  import GTreeHeader._
  import GArrayHeader._

  // FIXME needs to be rewritten

  override def transformType[T: PardisType]: PardisType[Any] = ({
    val tp = typeRep[T]
    tp match {
      case ArrayBufferType(t)  => typePointer(typeGArray)
      case SeqType(t)          => typePointer(typeLPointer(typeGList))
      case TreeSetType(t)      => typePointer(typeGTree)
      case SetType(t)          => typePointer(typeLPointer(typeGList))
      case OptionType(t)       => typePointer(transformType(t))
      case HashMapType(t1, t2) => typePointer(typeGHashTable)
      case CArrayType(t1)      => tp
      case _                   => super.transformType[T]
    }
  }).asInstanceOf[PardisType[Any]]

  def allocDoubleKey(key: Rep[_]): Rep[LPointer[Double]] = {
    val ptr = CStdLib.malloc[Double](unit(8))
    CLang.pointer_assign(ptr, key.asInstanceOf[Rep[Double]])
    ptr
  }

  /* HashMap Operations */
  rewrite += rule {
    case nm @ HashMapNew() =>
      if (nm.typeA == DoubleType || nm.typeA == PointerType(DoubleType)) {
        def hashFunc = toAtom(transformDef(doLambdaDef((s: Rep[gconstpointer]) => g_double_hash(s)))).asInstanceOf[Rep[GHashFunc]]
        def equalsFunc = toAtom(transformDef(doLambda2Def((s1: Rep[gconstpointer], s2: Rep[gconstpointer]) => g_double_equal(s1, s2)))).asInstanceOf[Rep[GEqualFunc]]
        g_hash_table_new(hashFunc, equalsFunc)
      } else if (nm.typeA.isPrimitive || nm.typeA == StringType || nm.typeA == OptimalStringType) {
        val nA = nm.typeA
        val nB = nm.typeB
        def hashFunc[T: TypeRep] = toAtom(transformDef(doLambdaDef((s: Rep[T]) => infix_hashCode(s)))).asInstanceOf[Rep[GHashFunc]]
        def equalsFunc[T: TypeRep] = toAtom(transformDef(doLambda2Def((s1: Rep[T], s2: Rep[T]) => infix_==(s1, s2)))).asInstanceOf[Rep[GEqualFunc]]
        g_hash_table_new(hashFunc(nA), equalsFunc(nA))
      } else {
        val nA = typePointer(transformType(nm.typeA))
        val nB = typePointer(transformType(nm.typeB))
        def hashFunc[T: TypeRep] = toAtom(transformDef(doLambdaDef((s: Rep[T]) => infix_hashCode(s)))).asInstanceOf[Rep[GHashFunc]]
        def equalsFunc[T: TypeRep] = toAtom(transformDef(doLambda2Def((s1: Rep[T], s2: Rep[T]) => infix_==(s1, s2)))).asInstanceOf[Rep[GEqualFunc]]
        g_hash_table_new(hashFunc(nA), equalsFunc(nA))
      }
  }
  rewrite += rule {
    case HashMapSize(map) => g_hash_table_size(map.asInstanceOf[Rep[LPointer[GHashTable]]])
  }
  rewrite += rule {
    case hmks @ HashMapKeySet(map) =>
      &(g_hash_table_get_keys(map.asInstanceOf[Rep[LPointer[GHashTable]]]))
  }
  rewrite += rule {
    case hmc @ HashMapContains(map, key) =>
      val z = toAtom(transformDef(HashMapApply(map, key)))(typePointer(transformType(hmc.typeB)).asInstanceOf[TypeRep[Any]])
      infix_!=(z, unit(null))
  }
  rewrite += rule {
    case ma @ HashMapApply(map, k) =>
      val key = if (ma.typeA == DoubleType || ma.typeA == PointerType(DoubleType)) CLang.&(apply(k)) else apply(k)
      g_hash_table_lookup(map.asInstanceOf[Rep[LPointer[GHashTable]]], key.asInstanceOf[Rep[gconstpointer]])
  }
  rewrite += rule {
    case mu @ HashMapUpdate(map, k, value) =>
      val key = if (mu.typeA == DoubleType || mu.typeA == PointerType(DoubleType)) allocDoubleKey(apply(k)) else apply(k)
      g_hash_table_insert(map.asInstanceOf[Rep[LPointer[GHashTable]]],
        key.asInstanceOf[Rep[gconstpointer]],
        value.asInstanceOf[Rep[gpointer]])
  }
  rewrite += rule {
    case hmgu @ HashMapGetOrElseUpdate(map, key, value) =>
      val ktp = typePointer(transformType(hmgu.typeA)).asInstanceOf[TypeRep[Any]]
      val vtp = typePointer(transformType(hmgu.typeB)).asInstanceOf[TypeRep[Any]]
      val v = toAtom(transformDef(HashMapApply(map, key)(ktp, vtp))(vtp))(vtp)
      __ifThenElse(infix_==(v, unit(null)), {
        val res = inlineBlock(apply(value))
        toAtom(HashMapUpdate(map, key, res)(ktp, vtp))
        res
      }, v)(v.tp)
  }
  rewrite += rule {
    case mr @ HashMapRemove(map, key) =>
      val x = toAtom(transformDef(HashMapApply(map, key)))(typePointer(transformType(mr.typeB)).asInstanceOf[TypeRep[Any]])
      val keyptr = if (mr.typeA == DoubleType || mr.typeA == PointerType(DoubleType)) allocDoubleKey(key) else key
      g_hash_table_remove(map.asInstanceOf[Rep[LPointer[GHashTable]]], keyptr.asInstanceOf[Rep[gconstpointer]])
      x
  }
  rewrite += rule {
    case hmfe @ HashMapForeach(map, f) =>
      val ktp = typePointer(transformType(hmfe.typeA))
      //val vtp = typePointer(transformType(hmfe.typeB))
      val vtp = f.tp.typeArguments(0).typeArguments(1).asInstanceOf[TypeRep[Any]]
      val keys = __newVar(g_hash_table_get_keys(map.asInstanceOf[Rep[LPointer[GHashTable]]]))
      val nKeys = g_hash_table_size(map.asInstanceOf[Rep[LPointer[GHashTable]]])
      Range(unit(0), nKeys).foreach {
        __lambda { i =>
          val keysRest = __readVar(keys)
          val key = g_list_nth_data(keysRest, unit(0))
          __assign(keys, g_list_next(keysRest))
          val value = g_hash_table_lookup(map.asInstanceOf[Rep[LPointer[GHashTable]]], key)
          inlineFunction(f, __newTuple2(infix_asInstanceOf(key)(ktp), infix_asInstanceOf(value)(vtp)))
        }
      }
  }

  /* Set Operations */
  rewrite += rule { case SetApplyObject1(s) => s }
  rewrite += rule {
    case nm @ SetApplyObject2() =>
      val s = CStdLib.malloc[LPointer[GList]](unit(8))
      CLang.pointer_assign(s, CLang.NULL[GList])
      s
  }
  rewrite += rule {
    case sh @ SetHead(s) =>
      val elemType = if (sh.typeA.isRecord) typeLPointer(sh.typeA) else sh.typeA
      val glist = CLang.*(s.asInstanceOf[Rep[LPointer[LPointer[GList]]]])
      infix_asInstanceOf(g_list_nth_data(glist, unit(0)))(elemType)
  }
  rewrite += rule {
    case SetRemove(s, e) =>
      val glist = CLang.*(s.asInstanceOf[Rep[LPointer[LPointer[GList]]]])
      val newHead = g_list_remove(glist, apply(e).asInstanceOf[Rep[LPointer[Any]]])
      CLang.pointer_assign(s.asInstanceOf[Rep[LPointer[LPointer[GList]]]], newHead)
  }
  rewrite += rule {
    case SetToSeq(set) =>
      set
  }
  rewrite += rule {
    case Set$plus$eq(s, e) =>
      val glist = CLang.*(s.asInstanceOf[Rep[LPointer[LPointer[GList]]]])
      val newHead = g_list_prepend(glist, apply(e).asInstanceOf[Rep[LPointer[Any]]])
      CLang.pointer_assign(s.asInstanceOf[Rep[LPointer[LPointer[GList]]]], newHead)
  }
  rewrite += rule {
    case sfe @ SetForeach(s, f) =>
      val elemType = if (sfe.typeA.isRecord) typeLPointer(sfe.typeA) else sfe.typeA
      val l = __newVar(CLang.*(s.asInstanceOf[Rep[LPointer[LPointer[GList]]]]))
      __whileDo(__readVar(l) __!= CLang.NULL[GList], {
        val elem = infix_asInstanceOf(g_list_nth_data(readVar(l), unit(0)))(elemType)
        __assign(l, g_list_next(readVar(l)))
        inlineFunction(f, elem)
        unit(())
      })
  }
  rewrite += rule {
    case sf @ SetFind(s, f) =>
      val elemType = if (sf.typeA.isRecord) typeLPointer(sf.typeA) else sf.typeA
      val result = __newVar(unit(null).asInstanceOf[Rep[Any]])(elemType.asInstanceOf[TypeRep[Any]])
      val l = __newVar(CLang.*(s.asInstanceOf[Rep[LPointer[LPointer[GList]]]]))
      __whileDo(__readVar(l) __!= CLang.NULL[GList], {
        val elem = infix_asInstanceOf(g_list_nth_data(readVar(l), unit(0)))(elemType)
        __assign(l, g_list_next(readVar(l)))
        val found = inlineFunction(f, elem)
        __ifThenElse(found, {
          __assign(result, elem)
          break()
        }, unit(()))
      })
      optionApplyObject(readVar(result)(elemType.asInstanceOf[TypeRep[Any]]))
  }
  rewrite += rule {
    case se @ SetExists(s, f) =>
      class X
      class Y
      implicit val typeX = se.typeA.asInstanceOf[TypeRep[X]]
      val set = s.asInstanceOf[Rep[Set[X]]]
      val fun = f.asInstanceOf[Rep[((X) => Boolean)]]
      val found = set.find(fun)
      found.nonEmpty
  }
  rewrite += rule {
    case sfl @ SetFoldLeft(s, z, f) =>
      val elemType = if (sfl.typeA.isRecord) typeLPointer(sfl.typeA) else sfl.typeA
      val state = __newVar(z)(sfl.typeB)
      val l = __newVar(CLang.*(s.asInstanceOf[Rep[LPointer[LPointer[GList]]]]))
      __whileDo(__readVar(l) __!= CLang.NULL[GList], {
        val elem = infix_asInstanceOf(g_list_nth_data(readVar(l), unit(0)))(elemType)
        val newState = inlineFunction(f, __readVar(state)(sfl.typeB), elem)
        __assign(state, newState)(sfl.typeB)
        __assign(l, g_list_next(readVar(l)))
      })
      __readVar(state)(sfl.typeB)
  }
  rewrite += rule {
    case SetSize(s) =>
      val l = CLang.*(s.asInstanceOf[Rep[LPointer[LPointer[GList]]]])
      g_list_length(l)
  }
  rewrite += rule {
    case sr @ SetRetain(s, f) =>
      val elemType = if (sr.typeA.isRecord) typeLPointer(sr.typeA) else sr.typeA
      val l = __newVar(CLang.*(s.asInstanceOf[Rep[LPointer[LPointer[GList]]]]))
      val prevPtr = __newVar(s.asInstanceOf[Rep[LPointer[LPointer[GList]]]])

      __whileDo(__readVar(l) __!= CLang.NULL[GList], {
        val elem = infix_asInstanceOf(g_list_nth_data(readVar(l), unit(0)))(elemType)
        val keep = inlineFunction(f, elem)
        __ifThenElse(keep, {
          //__assign(prevPtr, CLang.&(CLang.->[GList, LPointer[GList]](__readVar(l), unit("next"))))
          __assign(prevPtr, CLang.&(field[LPointer[GList]](__readVar[LPointer[GList]](l), "next")))
        }, {
          CLang.pointer_assign(readVar(prevPtr), g_list_next(readVar(l)))
        })
        __assign(l, g_list_next(readVar(l)))
      })
  }

  rewrite += rule {
    case smb @ SetMinBy(s, f) =>
      class X
      class Y
      implicit val typeX = (if (smb.typeA.isRecord) typeLPointer(smb.typeA) else smb.typeA).asInstanceOf[TypeRep[X]]
      implicit val typeY = smb.typeB.asInstanceOf[TypeRep[Y]]
      val set = s.asInstanceOf[Rep[Set[X]]]
      val fun = f.asInstanceOf[Rep[((X) => Y)]]

      val l = __newVar(CLang.*(set.asInstanceOf[Rep[LPointer[LPointer[GList]]]]))
      val first = infix_asInstanceOf[X](g_list_nth_data(__readVar(l), unit(0)))
      val result = __newVar(first)
      val min = __newVar(inlineFunction(fun, __readVar(result)))

      val cmp = OrderingRep(smb.typeB)

      __whileDo(__readVar(l) __!= CLang.NULL[GList], {
        val elem = infix_asInstanceOf[X](g_list_nth_data(__readVar(l), unit(0)))
        __assign(l, g_list_next(readVar(l)))
        val newMin = inlineFunction(fun, elem)
        __ifThenElse(cmp.lt(newMin, __readVar[Y](min)), {
          __assign(min, newMin)
          __assign(result, elem)
        }, unit(()))
      })
      __readVar(result)
  }

  /* TreeSet Operations */
  rewrite += remove { case OrderingNew(o) => () }

  rewrite += rule {
    case ts @ TreeSetNew2(Def(OrderingNew(Def(Lambda2(f, i1, i2, o))))) =>
      val compare = Lambda2(f, i1.asInstanceOf[Rep[LPointer[Any]]], i2.asInstanceOf[Rep[LPointer[Any]]], transformBlock(o))
      g_tree_new(CLang.&(compare))
  }
  rewrite += rule {
    case TreeSet$plus$eq(t, s) => g_tree_insert(t.asInstanceOf[Rep[LPointer[GTree]]], s.asInstanceOf[Rep[gpointer]], s.asInstanceOf[Rep[gpointer]])
  }
  rewrite += rule {
    case TreeSet$minus$eq(self, t) =>
      g_tree_remove(self.asInstanceOf[Rep[LPointer[GTree]]], t.asInstanceOf[Rep[gconstpointer]])
  }
  rewrite += rule {
    case TreeSetSize(t) => g_tree_nnodes(t.asInstanceOf[Rep[LPointer[GTree]]])
  }
  rewrite += rule {
    case op @ TreeSetHead(t) =>
      // def treeHead[A: PardisType, B: PardisType] = doLambda3((s1: Rep[A], s2: Rep[A], s3: Rep[Pointer[B]]) => {
      //   pointer_assign_content(s3.asInstanceOf[Expression[Pointer[Any]]], s2)
      def treeHead[T: TypeRep] = doLambda3((s1: Rep[gpointer], s2: Rep[gpointer], s3: Rep[gpointer]) => {
        CLang.pointer_assign(infix_asInstanceOf[LPointer[T]](s3), infix_asInstanceOf[T](s2))
        unit(1)
      })
      class X
      implicit val elemType = transformType(if (op.typeA.isRecord) typeLPointer(op.typeA) else op.typeA).asInstanceOf[TypeRep[X]]
      val init = CLang.NULL[Any]
      g_tree_foreach(t.asInstanceOf[Rep[LPointer[GTree]]], (treeHead(elemType)).asInstanceOf[Rep[LPointer[(gpointer, gpointer, gpointer) => Int]]], CLang.&(init).asInstanceOf[Rep[gpointer]])
      init.asInstanceOf[Rep[LPointer[Any]]]
      infix_asInstanceOf[X](init)
  }

  /* ArrayBuffer Operations */
  rewrite += rule {
    case abn @ (ArrayBufferNew2()) =>
      class X
      implicit val tpX = abn.tp.typeArguments(0).asInstanceOf[TypeRep[X]]
      g_array_new(0, 1, sizeof[X])
  }
  rewrite += rule {
    case aba @ ArrayBufferApply(a, i) =>
      class X
      implicit val tp = (if (aba.tp.isPrimitive || aba.tp.isPointerType || aba.tp == OptimalStringType) aba.tp else typePointer(aba.tp)).asInstanceOf[TypeRep[X]]
      // System.out.println(s"tp X: $tp")
      g_array_index[X](a.asInstanceOf[Rep[LPointer[GArray]]], i)
  }
  rewrite += rule {
    case ArrayBufferAppend(a, e) =>
      g_array_append_vals(apply(a).asInstanceOf[Rep[LPointer[GArray]]], CLang.&(e.asInstanceOf[Rep[gconstpointer]]), 1)
  }
  rewrite += rule {
    case ArrayBufferSize(a) =>
      CLang.->[GArray, Int](a.asInstanceOf[Rep[LPointer[GArray]]], unit("len"))
  }
  rewrite += rule {
    case ArrayBufferRemove(a, e) =>
      g_array_remove_index(a.asInstanceOf[Rep[LPointer[GArray]]], e)
  }
  rewrite += rule {
    case ArrayBufferMinBy(a, f @ Def(Lambda(fun, input, o))) =>
      val len = transformDef(ArrayBufferSize(a))
      val i = __newVar[Int](1)
      val tp = a.tp.typeArguments(0).typeArguments(0).asInstanceOf[PardisType[Any]]
      val varInit = toAtom(transformDef(ArrayBufferApply(apply(a), unit(0))(tp))(tp))(tp)
      val min = __newVar(varInit)(tp)
      val minBy = __newVar(fun(readVar(min)(tp)).asInstanceOf[Expression[Int]])
      __whileDo(readVar(i) < len, {
        val e = toAtom(transformDef(ArrayBufferApply(apply(a), readVar(i))(tp))(tp))(tp)
        val eminBy = fun(e).asInstanceOf[Expression[Int]]
        __ifThenElse(eminBy < readVar(minBy), {
          __assign(min, e)
          __assign(minBy, eminBy)
        }, unit())
        __assign(i, readVar(i) + unit(1))
      })
      ReadVar(min)(tp)
  }
  rewrite += rule {
    case ArrayBufferFoldLeft(a, cnt, Def(Lambda2(fun, input1, input2, o))) =>
      var idx = __newVar[Int](0)
      val len = transformDef(ArrayBufferSize(a))
      var agg = __newVar(cnt)(cnt.tp)
      __whileDo(readVar(idx) < len, {
        val tp = a.tp.typeArguments(0).typeArguments(0).asInstanceOf[PardisType[Any]]
        val e = toAtom(apply(ArrayBufferApply(apply(a), readVar(idx))(tp)))(tp)
        __assign(agg, fun(readVar(agg)(cnt.tp), e))
        __assign(idx, readVar(idx) + unit(1))
      })
      ReadVar(agg)
  }

  def arrayBufferIndexOf[T: PardisType](a: Expression[IR.ArrayBuffer[T]], elem: Expression[T]): Expression[Int] = {
    val idx = __newVar[Int](unit(-1))
    Range(unit(0), fieldGetter[Int](apply(a), "len")).foreach {
      __lambda { i =>
        val elemNode = apply(ArrayBufferApply(apply(a), i))
        val elem2 = toAtom(elemNode)(elemNode.tp)
        __ifThenElse(elem2 __== elem, {
          __assign(idx, i)
          break
        }, unit())
      }
    }
    readVar(idx)
  }

  rewrite += rule {
    case ArrayBufferIndexOf(a, elem) =>
      // val tp = a.tp.typeArguments(0).typeArguments(0).asInstanceOf[PardisType[Any]]
      val tp = elem.tp.asInstanceOf[PardisType[Any]]
      arrayBufferIndexOf(a, elem)(tp)
  }
  rewrite += rule {
    case ArrayBufferContains(a, elem) => {
      // System.out.println(s"tp: ${a.tp}, ${elem.tp}")
      // val tp = a.tp.typeArguments(0).typeArguments(0).asInstanceOf[PardisType[Any]]
      val tp = elem.tp.asInstanceOf[PardisType[Any]]
      val idx = arrayBufferIndexOf(a, elem)(tp)
      idx __!= unit(-1)
    }
  }

  def __arrayBufferIndexWhere[T: PardisType](a: Rep[IR.ArrayBuffer[T]], pred: Rep[T => Boolean], lastIndex: Boolean): Rep[Int] = {
    // printf(unit(s"indexWhere started with $a and ${apply(a)}"))
    val idx = __newVarNamed[Int](unit(-1), "indexWhere")
    Range(unit(0), fieldGetter[Int](a, "len")).foreach {
      __lambda { i =>
        val elemNode = apply(ArrayBufferApply(a, i))
        val elem = toAtom(elemNode)(elemNode.tp).asInstanceOf[Rep[T]]
        __ifThenElse(if (lastIndex) inlineFunction[T, Boolean](pred, elem) else { inlineFunction[T, Boolean](pred, elem) && (readVar(idx) __== unit(-1)) }, {
          if (lastIndex)
            __assign(idx, i)
          else {
            __assign(idx, i)
            break
          }
        }, unit())
      }
    }
    readVar(idx)
  }

  rewrite += rule {
    case ArrayBufferIndexWhere(a, pred) => {
      val tp = pred.tp.typeArguments(0).asInstanceOf[PardisType[Any]]
      __arrayBufferIndexWhere(a, pred, false)(tp)
    }
  }

  rewrite += rule {
    case ArrayBufferLastIndexWhere(a, pred) => {
      val tp = pred.tp.typeArguments(0).asInstanceOf[PardisType[Any]]
      __arrayBufferIndexWhere(a, pred, true)(tp)
    }
  }

  rewrite += rule {
    case ArrayBufferSortWith(a, sortFunc @ Def(Lambda2(f, i1, i2, o))) => {
      // val tp = a.tp.typeArguments(0).typeArguments(0).asInstanceOf[PardisType[Any]]
      class T
      implicit val tp = i1.tp.asInstanceOf[PardisType[T]]
      val (newI1, newI2) = (fresh(typePointer(tp)), fresh(typePointer(tp)))
      nameAlias[Unit](None, "g_array_sort", List(List(apply(a), Lambda2(f, newI1, newI2, reifyBlock { inlineFunction[T, T, Boolean](sortFunc, *(newI2), *(newI1)) }))))
      apply(a)
    }
  }
}