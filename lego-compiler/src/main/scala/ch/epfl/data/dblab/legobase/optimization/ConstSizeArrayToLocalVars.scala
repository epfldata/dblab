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
import sc.pardis.shallow.utils.DefaultValue
import sc.pardis.quasi.anf._
import quasi._

/**
 * Factory for creating instances of [[ConstSizeArrayToLocalVars]]
 */
object ConstSizeArrayToLocalVars extends TransformerHandler {
  def apply[Lang <: Base, T: PardisType](context: Lang)(block: context.Block[T]): context.Block[T] = {
    new ConstSizeArrayToLocalVars(context.asInstanceOf[LoweringLegoBase]).optimize(block)
  }
}

/**
 * Transforms the elements of an array of a constant and small size into local variables.
 *
 * As a simple example, the following code:
 * {{{
 *      val array = new Array[Int](2)
 *      array(0) = foo()
 *      array(1) = goo()
 *      ...
 *      process(array(0), array(1))
 * }}}
 * is converted to:
 * {{{
 *      val array0 = foo()
 *      val array1 = goo()
 *      ...
 *      process(array0, array1)
 * }}}
 *
 * The interesting case is when a desired array is a field of a record. Then, that array field
 * is converted to several fields in the record representing the elements of that array.
 *
 * As an example, the following code:
 * {{{
 *      val record = Record {
 *        val arr = new Array[Int](2)
 *      }
 *      record.arr(0) = foo()
 *      record.arr(1) = goo()
 *      ...
 *      process(record.arr(0), record.arr(1))
 * }}}
 * is converted to:
 * {{{
 *      val record = Record {
 *        var arr0: Int = _
 *        var arr1: Int = _
 *      }
 *      record.arr0 = foo()
 *      record.arr1 = goo()
 *      ...
 *      process(record.arr0, record.arr1)
 * }}}
 *
 *
 * @param IR the polymorphic embedding trait which contains the reified program.
 */
class ConstSizeArrayToLocalVars(override val IR: LoweringLegoBase) extends RecursiveRuleBasedTransformer[LoweringLegoBase](IR) with StructCollector[LoweringLegoBase] {
  import IR._

  val constSizeArrays = scala.collection.mutable.Set[Rep[Any]]()
  val constSizeArraysSize = scala.collection.mutable.Map[Rep[Any], Int]()
  val structsConstSizeArrayFieldSize = scala.collection.mutable.Map[(TypeRep[Any], String), Int]()
  val arrayVars = scala.collection.mutable.Map[Rep[Any], Array[Var[Any]]]()

  val escapedVarArrays = scala.collection.mutable.Map[Rep[Any], Var[Any]]()

  def isConstSizeArray[T](a: Expression[T]): Boolean = constSizeArrays.contains(a.asInstanceOf[Rep[Any]])
  def typeMayBeConstSizeArray[T](t: TypeRep[T]): Boolean = constSizeArrays.exists(x => x.tp == t.asInstanceOf[TypeRep[Any]])
  def structHasSingletonArrayAsField(s: PardisStruct[Any]) =
    s.elems.find(e => isConstSizeArray(e.init) && e.init.tp.isArray).nonEmpty
  def structSymHasSingletonArrayAsField[T](s: Rep[T]) = {
    getStructDef(s.tp) match {
      case Some(sd) => sd.fields.find(e => e.tpe.isArray && typeMayBeConstSizeArray(e.tpe)).nonEmpty
      case None     => false
    }
  }
  def addSingleton[T, S](s: Rep[T], v: TypeRep[S]): Unit =
    constSizeArrays += s.asInstanceOf[Rep[Any]]

  val SIZE_THRESHOLD = 10

  analysis += statement {
    case sym -> (an @ ArrayNew(Constant(v))) =>
      if (v < SIZE_THRESHOLD) {
        constSizeArraysSize(sym.asInstanceOf[Rep[Any]]) = v
        addSingleton(sym, sym.tp.typeArguments(0))
      }
    case sym -> PardisStructImmutableField(s, f) if sym.tp.isArray && structSymHasSingletonArrayAsField(s) =>
      addSingleton(sym, sym.tp.typeArguments(0))
  }

  /**
   * Returns the default value for the given type.
   * For the primitive types returns their corresponding default value (e.g. for
   * Int it returns 0) and for other types it returns null.
   */
  def getDefaultValue[T](tp: PardisType[T]): Rep[Any] = {
    val dfltV = sc.pardis.shallow.utils.DefaultValue(tp.name)
    if (dfltV != null) unit(0)(tp.asInstanceOf[PardisType[Int]])
    else unit(dfltV)(tp.asInstanceOf[PardisType[Any]])
  }

  rewrite += statement {
    case sym -> (an @ ArrayNew(Constant(v))) if isConstSizeArray(sym) => {
      val vars = new Array[Var[Any]](v)
      val default = getDefaultValue(an.typeT)
      for (i <- 0 until v) {
        vars(i.toInt) = __newVar(default)(an.typeT)
      }
      arrayVars(sym) = vars
      default
    }

    case sym -> (ps @ PardisStruct(tag, elems, methods)) if structHasSingletonArrayAsField(ps) =>
      val newElems = elems.flatMap(e =>
        if (e.init.tp.isArray && isConstSizeArray(e.init)) {
          // We know that every struct which as a constant array field with size > 1 has new Array has the init value
          // for that field
          val arraySize = constSizeArraysSize.get(e.init).getOrElse(1)
          structsConstSizeArrayFieldSize(sym.tp -> e.name) = arraySize
          if (arraySize > 1) {
            val init = getDefaultValue(e.init.tp.typeArguments(0))
            val newElems = for (index <- 0 until arraySize) yield {
              val newInit = readVar(__newVar(init)(init.tp))(init.tp)
              PardisStructArg(e.name + { if (index != 0) s"_$index" else "" }, true, newInit)
            }
            newElems.toList
          } else { // if (arraySize == 1) 
            val init = e.init match {
              case Constant(null) => getDefaultValue(e.init.tp.typeArguments(0))
              case _              => apply(e.init)
            }
            val newInit = readVar(__newVar(init)(init.tp))(init.tp)
            List(PardisStructArg(e.name, true, newInit))
          }
        } else List(e))
      toAtom(PardisStruct(tag, newElems, methods)(ps.tp))(ps.tp)
  }

  rewrite += rule {
    case au @ ArrayUpdate(a @ Def(PardisStructImmutableField(s, f)), Constant(i), v) if isConstSizeArray(a) =>
      val postFix = if (i == 0) "" else s"_$i"
      fieldSetter(s, s"$f$postFix", v)
    case aa @ ArrayApply(a @ Def(PardisStructImmutableField(s, f)), Constant(i)) if isConstSizeArray(a) =>
      val postFix = if (i == 0) "" else s"_$i"
      fieldGetter(s, s"$f$postFix")(aa.tp)
  }

  rewrite += statement {
    case sym -> NewVar(a @ Def(PardisStructImmutableField(s, f))) if isConstSizeArray(a) && structsConstSizeArrayFieldSize(s.tp, f) == 1 =>
      val res = __newVar(fieldGetter(s, f)(a.tp.typeArguments(0).asInstanceOf[TypeRep[Any]]))(a.tp.typeArguments(0).asInstanceOf[TypeRep[Any]])
      escapedVarArrays(sym) = res
      res.e
  }

  rewrite += rule {
    case ReadVar(v) if escapedVarArrays.contains(v.e) =>
      val newV = escapedVarArrays(v.e)
      readVar(newV)(newV.e.tp.typeArguments(0).asInstanceOf[TypeRep[Any]])
  }

  rewrite += rule {
    case au @ ArrayUpdate(a, Constant(i), v) if isConstSizeArray(a) && arrayVars.contains(a) =>
      __assign(arrayVars(a)(i), v)
    case aa @ ArrayApply(a, Constant(i)) if isConstSizeArray(a) && arrayVars.contains(a) =>
      readVar(arrayVars(a)(i))(a.tp.typeArguments(0).asInstanceOf[TypeRep[Any]])
  }
}
