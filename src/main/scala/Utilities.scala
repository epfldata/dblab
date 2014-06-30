package Utilities

object utilities {
	def time[A](a: => A, msg: String) = {
		val start = System.nanoTime
		val result = a
		val end = (System.nanoTime - start) / (1000 * 1000)
		System.out.println("Operation " + msg + " completed in %d milliseconds".format(end))
		result
    }
	def printToFile(f: java.io.File)(op: java.io.PrintWriter => Unit) {
  		val p = new java.io.PrintWriter(f)
  		try { op(p) } finally { p.close() }
	}
}
