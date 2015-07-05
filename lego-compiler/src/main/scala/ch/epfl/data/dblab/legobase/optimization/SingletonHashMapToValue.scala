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
import sc.pardis.quasi.anf._
import quasi._

object SingletonHashMapToValueTransformer extends TransformerHandler {
  def apply[Lang <: Base, T: PardisType](context: Lang)(block: context.Block[T]): context.Block[T] = {
    new SingletonHashMapToValueTransformer(context.asInstanceOf[LoweringLegoBase]).optimize(block)
  }
}

/**
 * Converts a HashMap with only one constant key to a local variable of its
 * corresponding value.
 *
 * Example:
 * {{{
 *    val hm = new HashMap[String, Array[Int]]
 *    while(...) {
 *      val array = hm.getOrElseUpdate("Total", new Array[Int](5))
 *      array(0) = foo()
 *      ...
 *      array(4) = goo()
 *    }
 *    hm.foreach({ case (k, v) =>
 *      process(v)
 *    })
 * }}}
 * is converted to:
 * {{{
 *    val array = new Array[Int](5)
 *    while(...) {
 *      array(0) = foo()
 *      ...
 *      array(4) = goo()
 *    }
 *    process(array)
 * }}}
 */
class SingletonHashMapToValueTransformer(override val IR: LoweringLegoBase)
  extends RecursiveRuleBasedTransformer[LoweringLegoBase](IR) {
  import IR._

  val singletonHashMapValues = collection.mutable.Map[Sym[Any], Block[Any]]()
  val singletonHashMapKeys = collection.mutable.Map[Sym[Any], String]()
  val singletonHashMaps = scala.collection.mutable.Set[Rep[Any]]()
  val multiValuedHashMaps = scala.collection.mutable.Set[Rep[Any]]()

  def isSingletonHashMap[T](a: Rep[T]): Boolean =
    singletonHashMaps.contains(a.asInstanceOf[Rep[Any]])

  /**
   * Adds the pair of keys and values for the given HashMap symbol.
   */
  def addSingleton[T, S](sym: Rep[T], key: String, block: Block[S]) = {
    val hm = sym.asInstanceOf[Sym[Any]]
    val value = block.asInstanceOf[Block[Any]]
    singletonHashMapKeys.get(hm) match {
      case Some(key2) if key != key2 =>
        // If the HashMap has different constant keys, this optimization does not
        // handle it
        multiValuedHashMaps += hm
      case _ =>
    }
    singletonHashMapValues(hm) = value
    singletonHashMapKeys(hm) = key
    singletonHashMaps += hm
  }

  /**
   * Considers only the HashMaps with one constant key.
   */
  override def postAnalyseProgram[T: TypeRep](node: Block[T]): Unit = {
    singletonHashMaps --= multiValuedHashMaps
  }

  analysis += rule {
    case dsl"($hm: HashMap[Any, Any]).getOrElseUpdate(${ Constant(key: String) }, $value)" => {
      addSingleton(hm, key, value)
      ()
    }
  }

  rewrite += rule {
    case dsl"($hm: HashMap[Any, Any]).getOrElseUpdate($key, $value)" if isSingletonHashMap(hm) => {
      apply(hm)
    }
  }

  rewrite += rule {
    case dsl"($hm: HashMap[Any, Any]).remove($key)" if isSingletonHashMap(hm) => {
      apply(hm)
    }
  }

  rewrite += rule {
    case dsl"($hm: HashMap[Any, Any]).foreach($func)" if isSingletonHashMap(hm) => {
      val hmValue = apply(hm)
      inlineFunction(func, Tuple2(unit(null), hmValue))
    }
  }

  rewrite += statement {
    // TODO requires support from the quasi engine
    // found   : [T]ch.epfl.data.sc.pardis.ir.Expression[Seq[T]]
    // required: SingletonHashMapToValueTransformer.this.IR.Seq[Nothing]
    // case sym -> dsl"new HashMap[Any, Any]" if isSingletonHashMap(sym) =>
    case sym -> HashMapNew() if (isSingletonHashMap(sym)) =>

      val valueBlock = singletonHashMapValues(sym)
      inlineBlock(valueBlock)
  }
}
