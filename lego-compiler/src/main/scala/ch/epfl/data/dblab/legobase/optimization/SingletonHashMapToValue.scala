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
 *
 * Preconditions:
 * a) There should be only one constant key for the HashMap.
 * b) The value for that particular key should be assigned only by `getOrElseUpdate`
 * and only invokation of this method in only one statement.
 * c) The value should not have dependencies on the statements happening between
 * `new HashMap` and the invokation of `getOrElseUpdate`.
 * d) Only in the case of using the following 3 methods
 * of a HashMap this transformation can be applied: 1) getOrElseUpdate 2) foreach
 * 3) remove.
 * e) In foreach method, there should be no use of the key, which means only the
 * value should be used.
 */
class SingletonHashMapToValueTransformer(override val IR: LoweringLegoBase)
  extends RecursiveRuleBasedTransformer[LoweringLegoBase](IR) {
  import IR._

  /**
   * Contains the value for a specific HashMap symbol
   */
  val singletonHashMapValues = collection.mutable.Map[Sym[Any], Block[Any]]()
  /**
   * Contains the constant key for a specific HashMap symbol
   */
  val singletonHashMapKeys = collection.mutable.Map[Sym[Any], Any]()
  /**
   * Contains the list of HashMaps which have one or more than constant keys.
   * After the analysis phase (in the method `postAnalyseProgram`) the HashMaps
   * with more than one constant keys (which are stored in the `multiValuedHashMaps`)
   * are removed from this list.
   */
  val singletonHashMaps = scala.collection.mutable.Set[Rep[Any]]()
  /**
   * Contains the list of HashMaps which have more than one constant keys.
   */
  val multiValuedHashMaps = scala.collection.mutable.Set[Rep[Any]]()

  def isSingletonHashMap[T](a: Rep[T]): Boolean =
    singletonHashMaps.contains(a.asInstanceOf[Rep[Any]])

  /**
   * Adds the pair of keys and values for the given HashMap symbol.
   */
  def addSingleton[T, S](sym: Rep[T], key: Any, block: Block[S]) = {
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
    case dsl"($hm: HashMap[Any, Any]).getOrElseUpdate(${ Constant(key) }, $value)" => {
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
