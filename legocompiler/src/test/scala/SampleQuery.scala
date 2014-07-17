package ch.epfl.data
package legobase
package deep

import org.scalatest.{ FlatSpec, ShouldMatchers }
import prettyprinter._

class SampleQuery extends FlatSpec with ShouldMatchers {
  def test1() {
    def sample1() = new DeepDSL {
      def sample1 = {
        val number = unit(1)
        val sum = number + number
        sum
      }
    }.sample1

    println(sample1)

    val lq = new LiftedQueries()
    val block = lq.Q1

    // println(block)
    // LegoGenerator.apply(block)

    val loweringContext = new LoweringLegoBase {}

    val lowering = new Lowering {
      val from = lq.context
      val to = loweringContext
    }

    val loweredBlock = lowering.transformProgram(block)
    // val loweredBlock = block

    /* it's written like this because of early definition: http://stackoverflow.com/questions/4712468/in-scala-what-is-an-early-initializer */
    val dce = new {
      val IR = loweringContext
    } with DCELegoBase {
    }

    val dceBlock = dce.optimize(loweredBlock)
    // val dceBlock = loweredBlock

    val ir2Program = new { val IR = loweringContext } with IRToProgram {
    }

    val finalProgram = ir2Program.createProgram(dceBlock)

    println(finalProgram)
    LegoGenerator.apply(finalProgram)
  }
  test1()
}
