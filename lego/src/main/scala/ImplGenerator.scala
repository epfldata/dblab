/*package ch.epfl.data
package legobase

import autolifter._
import queryengine.volcano._

object ImplGenerator {
  def main(args: Array[String]) {
    // genOps
    genQueries
  }

  def genOps {
    // impl.ImplLifter.liftClass[ScanOp[_]]
    // println("====")
    // impl.ImplLifter.liftClass[MapOp[_]]
    // println("====")
    // impl.ImplLifter.liftClass[PrintOp[_]]
    // println("====")
    // impl.ImplLifter.liftClass[SortOp[_]]
    // println("====")
    // impl.ImplLifter.liftClass[Operator[_]]
    // println("====")
    // impl.ImplLifter.liftClass[AggOp[_, _]]
    // println("====")
    // impl.ImplLifter.liftClass[SelectOp[_]]
  }

  def genQueries {
    impl.ImplLifter.liftClass[Q1]
    println("====")
    impl.ImplLifter.liftClass[Q2]
  }
}
*/ 