package ch.epfl.data
package dblab.legobase
package quasi

import org.scalatest.{ FlatSpec, ShouldMatchers }
import prettyprinter._
import optimization._
import deep._
import sc.pardis.optimization._
import sc.pardis.types.PardisTypeImplicits._

class LegoQuasiTest extends FlatSpec with ShouldMatchers {

  implicit val IR = new LoweringLegoBase {}

  "int comparsion" should "work" in {
    val exp = {
      import IR._
      val x = __newVar(unit(2))
      // val y = GenericEngine.parseDate(unit("1998-09-02"));
      val y = Loader.fileLineCount(unit("tmp"))
      readVar(x) < y
    }
    exp match {
      case dsl"($x: Int) < ($y: Int)" =>
    }
  }
}
