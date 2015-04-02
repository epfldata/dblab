package ch.epfl.data
package dblab.legobase
package deep

import org.scalatest.{ FlatSpec, ShouldMatchers }
import prettyprinter._
import optimization._
import sc.pardis.optimization._

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
  }

  test1()
}
