package ch.epfl.data
package dblab
package legobase
package deep
package quasi

import org.scalatest.{ FlatSpec, ShouldMatchers }
import prettyprinter._
import transformers._
import deep._
import sc.pardis.optimization._
import sc.pardis.types.PardisTypeImplicits._
import sc.pardis.quasi.anf._
import legobase.deep.LegoBaseQueryEngineExp

class WhileToRangeForeachTest extends FlatSpec with ShouldMatchers {

  implicit val IR = new LegoBaseQueryEngineExp {}

  "simple example" should "work" in {
    val exp = {
      IR.reifyBlock {
        dsl"""var x = 0
        while(x < 10) {
          println(x)
          x += 1
        }
        """
      }
    }
    val newExp = new WhileToRangeForeachTransformer(IR).optimize(exp)
    newExp match {
      case dsl"__block{ Range(0, 10).foreach($f) }" =>
    }
  }

  "while with step 2 and start 5" should "work" in {
    val exp = {
      IR.reifyBlock {
        dsl"""var x = 5
        while(x < 10) {
          println(x)
          x += 2
        }
        """
      }
    }
    val newExp = new WhileToRangeForeachTransformer(IR).optimize(exp)
    newExp match {
      case dsl"__block{ new Range(5, 10, 2).foreach($f) }" =>
    }
  }

  "decrementing range while" should "not be transformed" in {
    val exp = {
      IR.reifyBlock {
        dsl"""var x = 10
        while(x > 0) {
          println(x)
          x -= 1
        }
        """
      }
    }
    val newExp = new WhileToRangeForeachTransformer(IR).optimize(exp)
    assert(newExp == exp)
  }

  "an infinite while loop" should "not be transformed" in {
    val exp: IR.Block[Unit] = {
      IR.reifyBlock {
        dsl"""var x = 0
        while(x < 10) {
          var x = 4
          println(x)
          x += 1
        }
        """
      }
    }
    val newExp = new WhileToRangeForeachTransformer(IR).optimize(exp)
    assert(newExp == exp)
  }

  "while loop with additional mutation of the index variable" should "not be transformed" in {
    val exp: IR.Block[Unit] = {
      IR.reifyBlock {
        dsl"""var x = 0
        while(x < 10) {
          x += 2
          println(x)
          x += 1
        }
        """
      }
    }
    val newExp = new WhileToRangeForeachTransformer(IR).optimize(exp)
    assert(newExp == exp)
  }
}
