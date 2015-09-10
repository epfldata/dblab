package ch.epfl.data
package dblab.legobase
package optimization
package monad

import scala.language.implicitConversions
import sc.pardis.ir._
import reflect.runtime.universe.{ TypeTag, Type }
import sc.pardis.optimization._
import deep._
import sc.pardis.types._
import sc.pardis.types.PardisTypeImplicits._
import sc.pardis.shallow.utils.DefaultValue
import compiler._

/**
 * Pushes the filter operation of filteredGroupBy into the computation happening
 * after the groupBy.
 */
class QueryMonadPostHoisting(override val IR: LegoBaseExp) extends RuleBasedTransformer[LegoBaseExp](IR) {
  import IR._

  class K
  class V
  class S

  val hoistedFilteredGroupBy = scala.collection.mutable.Map[Rep[Any], QueryFilteredGroupBy[Any, Any]]()

  rewrite += statement {
    case sym -> (node @ QueryFilteredGroupBy(monad, pred, par)) => {
      implicit val typeK = par.tp.typeArguments(1).asInstanceOf[TypeRep[K]]
      implicit val typeV = par.tp.typeArguments(0).asInstanceOf[TypeRep[V]]
      hoistedFilteredGroupBy(sym) = node
      monad.asInstanceOf[Rep[Query[V]]].groupBy(par.asInstanceOf[Rep[V => K]])
    }
  }

  rewrite += rule {
    case GroupedQueryMapValues(groupedMonad, func) if hoistedFilteredGroupBy.contains(groupedMonad) =>
      implicit val typeK = groupedMonad.tp.typeArguments(0).asInstanceOf[TypeRep[K]]
      implicit val typeV = groupedMonad.tp.typeArguments(1).asInstanceOf[TypeRep[V]]
      implicit val typeS = func.tp.typeArguments(1).asInstanceOf[TypeRep[S]]
      val QueryFilteredGroupBy(_, pred, _) = hoistedFilteredGroupBy(groupedMonad).asInstanceOf[QueryFilteredGroupBy[V, K]]
      apply(groupedMonad).asInstanceOf[Rep[GroupedQuery[K, V]]].mapValues({
        __lambda { list =>
          inlineFunction(func.asInstanceOf[Rep[Query[V] => S]], list.filter(pred))
        }
      })

  }

}
