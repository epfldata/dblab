package ch.epfl.data
package legobase
package queryengine

class ArrayByteOps(val arr: Array[Byte]) extends AnyVal {
  // FIXME
  def diff(o: Array[Byte]): Int =
    // TODO: REFACTOR!
    (arr zip o).foldLeft(0)((res, e) => { if (res == 0) e._1 - e._2 else res })
  def compare(o: Array[Byte]): Int = if (arr.sameElements(o)) 0 else 1
  def ===(o: Array[Byte]): Boolean = compare(o) == 0
  def =!=(o: Array[Byte]): Boolean = compare(o) == 1
  def string: String = new String(arr.map(_.toChar))
}
