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

object SingletonArrayToValueTransformer extends TransformerHandler {
  def apply[Lang <: Base, T: PardisType](context: Lang)(block: context.Block[T]): context.Block[T] = {
    new SingletonArrayToValueTransformer(context.asInstanceOf[LoweringLegoBase]).optimize(block)
  }
}

class SingletonArrayToValueTransformer(override val IR: LoweringLegoBase) extends RecursiveRuleBasedTransformer[LoweringLegoBase](IR) {
  import IR._

  val singletonArrays = scala.collection.mutable.Map[TypeRep[Any], TypeRep[Any]]()
  def isSingletonArray[T](a: Expression[T]): Boolean = singletonArrays.contains(a.tp.asInstanceOf[TypeRep[Any]])
  def structHasSingletonArrayAsField(s: PardisStruct[Any]) =
    s.elems.find(e => isSingletonArray(e.init) && e.init.tp.isArray).nonEmpty
  def addSingleton[T, S](t: TypeRep[T], v: TypeRep[S]) =
    singletonArrays(t.asInstanceOf[TypeRep[Any]]) = v.asInstanceOf[TypeRep[Any]]

  analysis += statement {
    case sym -> (an @ ArrayNew(Constant(1))) => addSingleton(sym.tp, sym.tp.typeArguments(0))
    case sym -> PardisStructImmutableField(s, f) if sym.tp.isArray && isSingletonArray(s) =>
      addSingleton(sym.tp, sym.tp.typeArguments(0))
    case sym -> (ps @ PardisStruct(_, elems, _)) if structHasSingletonArrayAsField(ps) => {
      elems.foreach(e =>
        if (e.init.tp.isArray && isSingletonArray(e.init))
          addSingleton(sym.tp, sym.tp))
    }
  }

  def getDefaultValue[T](tp: PardisType[T]) = {
    val dfltV = pardis.shallow.utils.DefaultValue(tp.name)
    if (dfltV != null) unit(0)(tp.asInstanceOf[PardisType[Int]])
    else unit(dfltV)(tp.asInstanceOf[PardisType[Any]])
  }

  rewrite += statement {
    case sym -> (an @ ArrayNew(Constant(1))) => getDefaultValue(an.typeT)

    case sym -> (ps @ PardisStruct(tag, elems, methods)) if structHasSingletonArrayAsField(ps) =>
      val newElems = elems.map(e =>
        if (e.init.tp.isArray && isSingletonArray(e.init)) {
          val init = e.init match {
            case Constant(null) => getDefaultValue(e.init.tp.typeArguments(0))
            case _              => apply(e.init)
          }
          // val newInit = if (!e.mutable) readVar(__newVar(init)(init.tp))(init.tp) else init
          val newInit = readVar(__newVar(init)(init.tp))(init.tp)
          // val newInit = init
          PardisStructArg(e.name, true, newInit)
        } else e)
      toAtom(PardisStruct(tag, newElems, methods)(ps.tp))(ps.tp)
  }

  rewrite += rule {
    case au @ ArrayUpdate(a @ Def(PardisStructImmutableField(s, f)), i, v) if isSingletonArray(a) =>
      fieldSetter(s, f, v)
    case aa @ ArrayApply(a @ Def(PardisStructImmutableField(s, f)), i) if isSingletonArray(a) =>
      fieldGetter(s, f)(aa.tp)
  }

  override def newSym[T: TypeRep](sym: Rep[T]): to.Sym[_] = {
    if (isSingletonArray(sym)) to.fresh(singletonArrays(sym.tp.asInstanceOf[TypeRep[Any]])).copyFrom(sym.asInstanceOf[Sym[T]])
    else super.newSym[T](sym)
  }
}