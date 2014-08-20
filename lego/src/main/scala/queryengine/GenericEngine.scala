package ch.epfl.data
package legobase
package queryengine

object GenericEngine {
  def newAGGRecord[B](k: B, a: Array[Double]) = new AGGRecord(k, a)
  def newWindowRecord[B, C](k: B, w: C) = new WindowRecord(k, w)
  //def parseDate(x: String): Long
  //def parseString(x: String): Array[Byte]
  def runQuery[T](query: => T): T = {
    // if (profile) {
    utils.Utilities.time(query, "finish")
    // } else {
    //   query
    // }
  }

  def dateToString(long: Long): String = dateToString(new java.util.Date(long))

  def dateToString(dt: java.util.Date): String = {
    (dt.getYear + 1900) + "" +
      {
        val m = dt.getMonth + 1
        if (m < 10) "0" + m else m
      } + "" +
      {
        val d = dt.getDate
        if (d < 10) "0" + d else d
      }
  }

  val sdf = new java.text.SimpleDateFormat("yyyy-MM-dd")

  def parseDate(x: String): Long = {
    sdf.parse(x).getTime
  }
  def parseString(x: String): LBString = LBString(x.getBytes)
}
