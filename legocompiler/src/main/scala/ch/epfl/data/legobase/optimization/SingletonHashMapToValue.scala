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

object SingletonHashMapToValueTransformer extends TransformerHandler {
  def apply[Lang <: Base, T: PardisType](context: Lang)(block: context.Block[T]): context.Block[T] = {
    new SingletonHashMapToValueTransformer(context.asInstanceOf[LoweringLegoBase]).optimize(block)
  }
}

class SingletonHashMapToValueTransformer(override val IR: LoweringLegoBase) extends RecursiveRuleBasedTransformer[LoweringLegoBase](IR) {
  import IR._

  val constantKeyMap = collection.mutable.Map[Sym[Any], Block[Any]]()
  val singletonHashMaps = scala.collection.mutable.ArrayBuffer[Rep[Any]]()
  def isSingletonHashMap[T](a: Rep[T]): Boolean = singletonHashMaps.contains(a.asInstanceOf[Rep[Any]])
  def addSingleton[T, S](sym: Rep[T], block: Block[S]) = {
    constantKeyMap += sym.asInstanceOf[Sym[Any]] -> block.asInstanceOf[Block[Any]]
    singletonHashMaps += sym.asInstanceOf[Rep[Any]]
  }

  analysis += rule {
    case hmgeu @ HashMapGetOrElseUpdate(hm, Constant(_), value) => {
      addSingleton(hm, value)
      ()
    }
  }

  rewrite += rule {
    case (hmgeu @ HashMapGetOrElseUpdate(hm, k, v)) if (isSingletonHashMap(hm)) =>
      apply(hm)
    case (hmr @ HashMapRemove(hm, key)) if (isSingletonHashMap(hm)) =>
      apply(hm)
  }

  rewrite += rule {
    case HashMapForeach(hm, f) if (isSingletonHashMap(hm)) => {
      val hmValue = apply(hm)
      inlineFunction(f, Tuple2(unit(null), hmValue))
    }
  }

  rewrite += statement {
    case sym -> (hmn @ HashMapNew()) if (isSingletonHashMap(sym)) =>
      val valueBlock = constantKeyMap(sym)
      inlineBlock(valueBlock)
  }

  override def newSym[T: TypeRep](sym: Rep[T]): to.Sym[_] = {
    if (isSingletonHashMap(sym)) fresh(sym.tp.typeArguments(1)).copyFrom(sym.asInstanceOf[Sym[T]])
    else super.newSym[T](sym)
  }
}
