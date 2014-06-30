package ch.epfl.data
package legobase
package deep

import org.scalatest.{ FlatSpec, ShouldMatchers }
import pardis.prettyprinter._

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
  println(block)

  CodeGenerator.generate(block)
}
