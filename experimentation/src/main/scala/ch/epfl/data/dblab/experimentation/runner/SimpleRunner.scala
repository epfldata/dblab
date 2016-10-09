package ch.epfl.data
package dblab
package experimentation
package runner

import ch.epfl.data.dblab.config.Config
import schema._

trait SimpleRunner extends QueryRunner {
  Config.checkResults = false

  def getResultFileName(query: String): String = ""
  def getQueries(args: Array[String]): List[String] = List("Query")
  def getSchema(args: Array[String]): Schema = null
  def preprocessArgs(args: Array[String]): Unit = {}
  def getOutputName(query: String): String = "output.txt"
}
