package QueryEngine

object GenericEngine {
	case class AGGRecord[B](
		val key: B,
		val aggs: Array[Double]
	)
	def newAGGRecord[B:Manifest](k: B, a: Array[Double]) = new AGGRecord(k, a)
	//def parseDate(x: String): Long
	//def parseString(x: String): Array[Byte]
}
