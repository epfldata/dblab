package ch.epfl.data
package legobase
package deep

import org.scalatest.{ FlatSpec, ShouldMatchers }
import prettyprinter._

class SampleQuery extends FlatSpec with ShouldMatchers {
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

  val ir2Program = new IRToProgram {
    val IR = loweringContext
  }

  val finalProgram = ir2Program.createProgram(loweredBlock)

  println(finalProgram)
  LegoGenerator.apply(finalProgram)
}
