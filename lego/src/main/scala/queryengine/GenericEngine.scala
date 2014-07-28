package ch.epfl.data
package legobase
package queryengine

case class AGGRecord[B](
  val key: B,
  val aggs: Array[Double])

case class WindowRecord[B, C](
  val key: B,
  val wnd: C)

object GenericEngine {
  def newAGGRecord[B: Manifest](k: B, a: Array[Double]) = new AGGRecord(k, a)
  def newWindowRecord[B, C](k: B, w: C) = new WindowRecord(k, w)
  //def parseDate(x: String): Long
  //def parseString(x: String): Array[Byte]

}
