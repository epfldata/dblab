package ch.epfl.data
package legobase
package queryengine
import ch.epfl.data.pardis.shallow.{ CaseClassRecord }

case class AGGRecord[B](
  val key: B,
  val aggs: Array[Double]) extends CaseClassRecord {
  def getField(key: String): Option[Any] = key match {
    case "key"  => Some(key)
    case "aggs" => Some(aggs)
    case _      => None
  }
}

case class WindowRecord[B, C](
  val key: B,
  val wnd: C) extends CaseClassRecord {
  def getField(key: String): Option[Any] = key match {
    case "key"  => Some(key)
    case "aggs" => Some(wnd)
    case _      => None
  }
}

case class GroupByClass(val L_RETURNFLAG: java.lang.Character, val L_LINESTATUS: java.lang.Character) extends CaseClassRecord {
  def getField(key: String): Option[Any] = key match {
    case "L_RETURNFLAG" => Some(L_RETURNFLAG)
    case "L_LINESTATUS" => Some(L_LINESTATUS)
    case _              => None
  }
}

object GenericEngine {
  def newAGGRecord[B: Manifest](k: B, a: Array[Double]) = new AGGRecord(k, a)
  def newWindowRecord[B, C](k: B, w: C) = new WindowRecord(k, w)
  //def parseDate(x: String): Long
  //def parseString(x: String): Array[Byte]

}
