package ch.epfl.data
package dblab.legobase
package optimization
package monad

import schema._
import scala.language.implicitConversions
import sc.pardis.ir._
import reflect.runtime.universe.{ TypeTag, Type }
import sc.pardis.optimization._
import deep._
import sc.pardis.types._
import sc.pardis.types.PardisTypeImplicits._
import sc.pardis.shallow.utils.DefaultValue
import scala.collection.mutable

/**
 * Lowers query monad operations using continuation-passing style.
 */
class QueryMonadCPSLowering(val schema: Schema, override val IR: LegoBaseExp) extends RuleBasedTransformer[LegoBaseExp](IR) with StructCollector[LegoBaseExp] {
  import IR._

  abstract class QueryCPS[T: TypeRep] {
    def foreach(k: Rep[T] => Rep[Unit]): Rep[Unit]
    def map[S: TypeRep](f: Rep[T] => Rep[S]): QueryCPS[S] = (k: Rep[S] => Rep[Unit]) => {
      foreach(e => k(f(e)))
    }
    def filter(p: Rep[T] => Rep[Boolean]): QueryCPS[T] = (k: Rep[T] => Rep[Unit]) => {
      foreach(e => __ifThenElse(p(e), k(e), unit()))
    }
    def count: Rep[Int] = {
      val size = __newVarNamed[Int](unit(0), "size")
      foreach(e => {
        __assign(size, readVar(size) + unit(1))
      })
      readVar(size)
    }
  }

  object QueryCPS {
    def apply[T: TypeRep](arr: Rep[Array[T]]): QueryCPS[T] =
      QueryCPS { (k: Rep[T] => Rep[Unit]) =>
        array_foreach(arr, k)
      }
    implicit def apply[T: TypeRep](k: (Rep[T] => Rep[Unit]) => Rep[Unit]): QueryCPS[T] =
      new QueryCPS[T] {
        def foreach(k2: Rep[T] => Rep[Unit]): Rep[Unit] = k(k2)
      }
  }

  implicit def queryToCps[T](sym: Rep[Query[T]]): QueryCPS[T] =
    cpsMap(sym.asInstanceOf[Rep[Any]]).asInstanceOf[QueryCPS[T]]

  val cpsMap = mutable.Map[Rep[Any], QueryCPS[Any]]()

  rewrite += statement {
    case sym -> QueryNew2(array) =>
      val cps = QueryCPS(array)(array.tp.typeArguments(0).asInstanceOf[TypeRep[Any]])
      cpsMap += sym -> cps
      sym
  }

  rewrite += statement {
    case sym -> QueryFilter(monad, p) =>
      val Def(Lambda(pred, _, _)) = p
      val cps = monad.filter(pred)
      cpsMap += sym -> cps
      // System.out.println(s"$sym -> $cps added to map")
      sym
  }

  rewrite += rule {
    case QueryCount(monad) =>
      queryToCps(monad).count
  }
}
