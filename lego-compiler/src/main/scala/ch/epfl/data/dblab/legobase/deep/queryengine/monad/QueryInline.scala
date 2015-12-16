package ch.epfl.data
package dblab.legobase
package deep.queryengine.monad

import sc.pardis.types.PardisTypeImplicits._
import sc.pardis.ir._

trait QueryInline extends QueryOps with sc.pardis.deep.scalalib.ScalaPredef { this: GroupedQueryOps =>
  override def queryPrintRows[T](self: Rep[Query[T]], printFunc: Rep[((T) => Unit)], limit: Rep[Int])(implicit typeT: TypeRep[T]): Rep[Unit] = {
    val query = limit match {
      case Constant(-1) => self
      case _            => self.take(limit)
    }
    val rows = __newVarNamed(unit(0), "rows")
    query.foreach(__lambda { elem =>
      __app(printFunc).apply(elem)
      __assign(rows, readVar(rows) + unit(1))
    })
    // TODO investigate why [Int] is needed here!
    // Here is a possible minimalistic example:
    // def foo(args: Manifest[Any]*) = args
    // def m[T: Manifest](a: T) = manifest[T]
    // foo(m(2), m(""))
    // res2: Seq[Manifest[Any]] = WrappedArray(Any, Any)
    printf(unit("(%d rows)\n"), readVar[Int](rows))
  }
}