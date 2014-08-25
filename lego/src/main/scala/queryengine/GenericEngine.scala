package ch.epfl.data
package legobase
package queryengine

// FIXME just to cheat on auto-lifter
trait GenericEngine {

}

object GenericEngine {
  def runQuery[T](query: => T): T = {
    // if (profile) {
    utils.Utilities.time(query, "finish")
    // } else {
    //   query
    // }
  }

  def dateToString(long: Long): String = {
    val dt = new java.util.Date(long)
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

  def dateToYear(long: Long): Int = {
    new java.util.Date(long).getYear + 1900
  }

  private val simpleDateFormatter = new java.text.SimpleDateFormat("yyyy-MM-dd")

  def parseDate(x: String): Long = {
    simpleDateFormatter.parse(x).getTime
  }
  def parseString(x: String): LBString = LBString(x.getBytes)
}
