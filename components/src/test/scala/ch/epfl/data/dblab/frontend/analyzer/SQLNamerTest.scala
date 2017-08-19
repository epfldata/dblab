package ch.epfl.data
package dblab
package frontend
package analyzer

import org.scalatest._
import Matchers._
import parser.SQLParser
import parser.SQLAST._
import parser.DDLAST._
import schema._

class SQLNamerTest extends FlatSpec {

  def parseAndNameQuery(queryFile: java.io.File) = {
    val parser = SQLParser
    val sqlParserTree = parser.parseStream(scala.io.Source.fromFile(queryFile).mkString)
    val sqlProgram = sqlParserTree.asInstanceOf[IncludeStatement]
    val tables = sqlProgram.streams.toList.map(x => x.asInstanceOf[CreateStream])
    val ddlInterpreter = new DDLInterpreter(new Catalog(scala.collection.mutable.Map()))
    val query = sqlProgram.body
    starExpressionCount(query) should not be (0)
    val schema = ddlInterpreter.interpret(UseSchema("DBToaster") :: tables)
    val namedQuery = new SQLNamer(schema).nameQuery(query)
    // println(schema)
    namedQuery
  }

  def starExpressionCount(queryTree: TopLevelStatement): Int = {
    queryTree match {
      case st: SelectStatement =>
        st.projections match {
          case ExpressionProjections(lst) =>
            lst.collect({ case (StarExpression(_), _) => 1 }).sum
        }
      case _ => 0
    }
  }

  "SQLNamer" should "infer the names correctly for a simple select all query" in {
    val folder = "experimentation/dbtoaster/queries/simple"
    val file = new java.io.File(s"$folder/r_selectstar.sql")
    // println(s"naming $file")
    val namedQuery = parseAndNameQuery(file)
    // println(namedQuery)
    starExpressionCount(namedQuery) should be(0)
  }
}
