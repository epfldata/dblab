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
    val tables = sqlProgram.streams.toList.collect { case x: CreateStream => x }
    val ddlInterpreter = new DDLInterpreter(new Catalog(scala.collection.mutable.Map()))
    val query = sqlProgram.body
    // println(query)
    // starExpressionCount(query) should not be (0)
    val schema = ddlInterpreter.interpret(UseSchema("DBToaster") :: tables)
    val namedQuery = new SQLNamer(schema).nameQuery(query)
    // println(schema)
    namedQuery
  }

  def starExpressionCount(queryTree: TopLevelStatement): Int = {
    queryTree match {
      case st: SelectStatement =>
        val countProj = st.projections match {
          case ExpressionProjections(lst) =>
            lst.collect({ case (StarExpression(_), _) => 1 }).sum
        }
        val countTarget = st.joinTree.map(_.extractSubqueries.map(x => starExpressionCount(x.subquery)).sum).getOrElse(0)
        countProj + countTarget
      case _ => 0
    }
  }

  val simpleQueriesFolder = "experimentation/dbtoaster/queries/simple"
  val tpchQueriesFolder = "experimentation/dbtoaster/queries/tpch"

  "SQLNamer" should "infer the names correctly for a simple select all query" in {
    val file = new java.io.File(s"$simpleQueriesFolder/r_selectstar.sql")
    val namedQuery = parseAndNameQuery(file)
    starExpressionCount(namedQuery) should be(0)
  }

  "SQLNamer" should "infer the names correctly for a join select all query" in {
    val file = new java.io.File(s"$simpleQueriesFolder/rs_eqineq.sql")
    val namedQuery = parseAndNameQuery(file)
    starExpressionCount(namedQuery) should be(0)
  }

  "SQLNamer" should "infer the names correctly for a joined select part start query" in {
    val file = new java.io.File(s"$simpleQueriesFolder/rs_selectpartstar.sql")
    val namedQuery = parseAndNameQuery(file)
    starExpressionCount(namedQuery) should be(0)
  }

  "SQLNamer" should "infer the names correctly for a select all of subquery" in {
    val file = new java.io.File(s"$simpleQueriesFolder/r_starofnested.sql")
    val namedQuery = parseAndNameQuery(file)
    // println(namedQuery)
    starExpressionCount(namedQuery) should be(0)
  }

  "SQLNamer" should "infer the names correctly for all simple queries" in {
    val f = new java.io.File(simpleQueriesFolder)
    val files = if (!f.exists) {
      throw new Exception(s"$f does not exist!")
    } else
      f.listFiles.map(simpleQueriesFolder + "/" + _.getName).toList
    for (file <- files) {
      val namedQuery = parseAndNameQuery(new java.io.File(file))
      starExpressionCount(namedQuery) should be(0)
    }
  }

  "SQLNamer" should "infer the names correctly for all tpch queries" in {
    val f = new java.io.File(tpchQueriesFolder)
    val files = if (!f.exists) {
      throw new Exception(s"$f does not exist!")
    } else
      f.listFiles.filter(_.getName.startsWith("query")).map(tpchQueriesFolder + "/" + _.getName).toList
    for (file <- files) {
      // println(s"naming $file")
      val namedQuery = parseAndNameQuery(new java.io.File(file))
      starExpressionCount(namedQuery) should be(0)
    }
  }
}
