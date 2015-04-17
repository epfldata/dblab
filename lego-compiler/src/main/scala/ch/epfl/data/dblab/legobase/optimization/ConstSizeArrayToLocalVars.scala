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
 * The interesting case is when a desired array is a field of a struct. Then, that array field
 * is converted to several fields representing the elements of that array.
 *
 * TODO maybe add an example
 *
 * @param IR the polymorphic embedding trait which contains the reified program.
 */
class ConstSizeArrayToLocalVars(override val IR: LoweringLegoBase) extends RecursiveRuleBasedTransformer[LoweringLegoBase](IR) with StructCollector[LoweringLegoBase] {
  import IR._

  val constSizeArrays = scala.collection.mutable.Map[Rep[Any], TypeRep[Any]]()
  val constSizeArraysSize = scala.collection.mutable.Map[Rep[Any], Int]()
  val arrayVars = scala.collection.mutable.Map[Rep[Any], Array[Var[Any]]]()
  def isSingletonArray[T](a: Expression[T]): Boolean = constSizeArrays.contains(a.asInstanceOf[Rep[Any]])
  def isPotentiallySingletonArray[T](t: TypeRep[T]): Boolean = constSizeArrays.exists(x => x._1.tp == t.asInstanceOf[TypeRep[Any]])
  def structHasSingletonArrayAsField(s: PardisStruct[Any]) =
    s.elems.find(e => isSingletonArray(e.init) && e.init.tp.isArray).nonEmpty
  def structSymHasSingletonArrayAsField[T](s: Rep[T]) = {
    getStructDef(s.tp) match {
      case Some(sd) => sd.fields.find(e => e.tpe.isArray && isPotentiallySingletonArray(e.tpe)).nonEmpty
      case None     => false
    }
  }
  def addSingleton[T, S](s: Rep[T], v: TypeRep[S]) =
    constSizeArrays(s.asInstanceOf[Rep[Any]]) = v.asInstanceOf[TypeRep[Any]]

  analysis += statement {
    // case sym -> (an @ ArrayNew(Constant(1))) => addSingleton(sym.tp, sym.tp.typeArguments(0))
    case sym -> (an @ ArrayNew(Constant(v))) =>
      // System.out.println(s"${scala.Console.GREEN}Const array with size $v${scala.Console.BLACK}")
      if (v < 10) {
        // if (v > 1 && v < 10) {
        constSizeArraysSize(sym.asInstanceOf[Rep[Any]]) = v
        addSingleton(sym, sym.tp.typeArguments(0))
      }
    case sym -> PardisStructImmutableField(s, f) if sym.tp.isArray && structSymHasSingletonArrayAsField(s) =>
      addSingleton(sym, sym.tp.typeArguments(0))
    // case sym -> (ps @ PardisStruct(_, elems, _)) if structHasSingletonArrayAsField(ps) => {
    //   elems.foreach(e =>
    //     if (e.init.tp.isArray && isSingletonArray(e.init))
    //       addSingleton(sym.tp, sym.tp))
    // }
  }

  def getDefaultValue[T](tp: PardisType[T]) = {
    val dfltV = sc.pardis.shallow.utils.DefaultValue(tp.name)
    if (dfltV != null) unit(0)(tp.asInstanceOf[PardisType[Int]])
    else unit(dfltV)(tp.asInstanceOf[PardisType[Any]])
  }

  rewrite += statement {
    // case sym -> (an @ ArrayNew(Constant(1))) => getDefaultValue(an.typeT)
    case sym -> (an @ ArrayNew(Constant(v))) if isSingletonArray(sym) => {
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
        if (e.init.tp.isArray && isSingletonArray(e.init)) {
          // System.out.println(s"init node ${scala.Console.RED}${e.init.correspondingNode}${scala.Console.BLACK}")
          // We know that every struct which as a constant array field with size > 1 has new Array has the init value
          // for that field
          val arraySize = constSizeArraysSize.get(e.init).getOrElse(1)
          if (arraySize > 1) {
            // this works only on specific cases which is the case for TPCH queries
            val init = getDefaultValue(e.init.tp.typeArguments(0))
            val newElems = for (index <- 0 until arraySize) yield {
              val newInit = readVar(__newVar(init)(init.tp))(init.tp)
              PardisStructArg(e.name + { if (index != 0) s"_$index" else "" }, true, newInit)
            }
            newElems.toList
          } else {
            val init = e.init match {
              case Constant(null) => getDefaultValue(e.init.tp.typeArguments(0))
              case _              => apply(e.init)
            }
            // val newInit = if (!e.mutable) readVar(__newVar(init)(init.tp))(init.tp) else init
            val newInit = readVar(__newVar(init)(init.tp))(init.tp)
            // val newInit = init
            List(PardisStructArg(e.name, true, newInit))
          }
        } else List(e))
      toAtom(PardisStruct(tag, newElems, methods)(ps.tp))(ps.tp)
  }

  rewrite += rule {
    case au @ ArrayUpdate(a @ Def(PardisStructImmutableField(s, f)), Constant(i), v) if isSingletonArray(a) =>
      val postFix = if (i == 0) "" else s"_$i"
      fieldSetter(s, s"$f$postFix", v)
    case aa @ ArrayApply(a @ Def(PardisStructImmutableField(s, f)), Constant(i)) if isSingletonArray(a) =>
      val postFix = if (i == 0) "" else s"_$i"
      fieldGetter(s, s"$f$postFix")(aa.tp)
  }

  rewrite += rule {
    case au @ ArrayUpdate(a, Constant(i), v) if isSingletonArray(a) && arrayVars.contains(a) =>
      __assign(arrayVars(a)(i), v)
    case aa @ ArrayApply(a, Constant(i)) if isSingletonArray(a) && arrayVars.contains(a) =>
      readVar(arrayVars(a)(i))(a.tp.typeArguments(0).asInstanceOf[TypeRep[Any]])
  }

  override def newSym[T: TypeRep](sym: Rep[T]): to.Sym[_] = {
    if (isSingletonArray(sym)) to.fresh(constSizeArrays(sym.asInstanceOf[Rep[Any]])).copyFrom(sym.asInstanceOf[Sym[T]])
    else super.newSym[T](sym)
  }
}