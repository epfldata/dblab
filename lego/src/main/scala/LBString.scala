package ch.epfl.data
package legobase

case class LBString(data: Array[Byte]) {
  override def hashCode(): Int = data.foldLeft(0)((sum, e) => sum + e)
  override def equals(o: Any): Boolean = {
    val arr2 = o.asInstanceOf[LBString]
    data.sameElements(arr2.data)
  }
  def apply(i: Int): Byte = data(i)
  def startsWith(o: LBString): Boolean = data.startsWith(o.data)
  def containsSlice(o: LBString): Boolean = data.containsSlice(o.data)
  def endsWith(o: LBString): Boolean = data.endsWith(o.data)
  def slice(start: Int, end: Int): LBString = LBString(data.slice(start, end))
  def indexOfSlice(o: LBString, i: Int): Int = data.indexOfSlice(o.data, i)
  def diff(o: LBString): Int =
    // TODO: REFACTOR!
    (data zip o.data).foldLeft(0)((res, e) => { if (res == 0) e._1 - e._2 else res })
  def compare(o: LBString): Int = if (data.sameElements(o.data)) 0 else 1
  def ===(o: LBString): Boolean = compare(o) == 0
  def =!=(o: LBString): Boolean = compare(o) == 1
  def string: String = new String(data.map(_.toChar))
  def zip(o: LBString) = data.zip(o.data)
  def foldLeft(c: Int)(f: (Int, Byte) => Int): Int = data.foldLeft(c)(f)
}
