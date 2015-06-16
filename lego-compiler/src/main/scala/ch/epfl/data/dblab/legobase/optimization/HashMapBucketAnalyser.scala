package ch.epfl.data
package dblab.legobase
package optimization

import schema._
import scala.language.implicitConversions
import sc.pardis.ir._
import reflect.runtime.universe.{ TypeTag, Type }
import sc.pardis.optimization._
import deep._
import sc.pardis.types._
import sc.pardis.types.PardisTypeImplicits._
import sc.pardis.deep.scalalib._
import sc.pardis.deep.scalalib.collection._

/**
 * An analyser for computing the number of necessary buckets
 */
trait HashMapBucketAnalyser[Lang <: HashMapOps] { this: RuleBasedTransformer[Lang] =>
  val IR: Lang
  val schema: Schema
  import IR._

  case class StructFieldInfo(structType: TypeRep[Any], field: String)

  val hashMapsStructFieldInfo = scala.collection.mutable.Map[Rep[Any], StructFieldInfo]()

  analysis += rule {
    case HashMapGetOrElseUpdate(hm, Def(StructImmutableField(s, field)), _) => {
      hashMapsStructFieldInfo += hm -> StructFieldInfo(s.tp, field)
      ()
    }
  }
}
