package ch.epfl.data
package legobase
package queryengine

class ArrayByteOps(val arr: Array[Byte]) extends AnyVal {
  // FIXME
  def compare(o: Array[Byte]): Int = if (arr.sameElements(o)) 0 else 1
  def ===(o: Array[Byte]): Boolean = compare(o) == 0
  def string: String = new String(arr.map(_.toChar))
}
