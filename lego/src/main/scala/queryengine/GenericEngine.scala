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

  def dateToString(long: Int): String = long.toString

  def dateToYear(long: Int): Int = {
    long / 10000
    //new java.util.Date(long).getYear + 1900
  }

  private val simpleDateFormatter = new java.text.SimpleDateFormat("yyyy-MM-dd")

  def parseDate(x: String): Int = {
    val d = simpleDateFormatter.parse(x)
    ((d.getYear + 1900) * 10000) + ((d.getMonth + 1) * 100) + d.getDate
  }
  def parseString(x: String): LBString = LBString(x.getBytes)
}
