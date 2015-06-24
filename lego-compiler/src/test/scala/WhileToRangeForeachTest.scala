package ch.epfl.data
package dblab.legobase
package quasi

import org.scalatest.{ FlatSpec, ShouldMatchers }
import prettyprinter._
import optimization._
import deep._
import sc.pardis.optimization._
import sc.pardis.types.PardisTypeImplicits._
import sc.pardis.quasi.anf._

class WhileToRangeForeachTest extends FlatSpec with ShouldMatchers {

  implicit val IR = new LoweringLegoBase {}

  "simple example" should "work" in {
    val exp = {
      import IR._
      reifyBlock {
        val x = __newVar(unit(0))
        __whileDo(readVar(x) < unit(10), {
          println(readVar(x))
          __assign(x, readVar(x) + unit(1))
        })
      }
    }
    val newExp = new WhileToRangeForeachTransformer(IR).optimize(exp)
    newExp match {
      case dsl"__block{ Range(0, 10).foreach($f) }" =>
    }
  }
}
