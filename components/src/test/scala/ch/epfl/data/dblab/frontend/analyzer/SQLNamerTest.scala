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
    // unnamedCount(query) should not be (0)
    val schema = ddlInterpreter.interpret(UseSchema("DBToaster") :: tables)
    val namedQuery = new SQLNamer(schema).nameQuery(query)
    // println(schema)
    namedQuery
  }

  def unnamedCount(queryExpr: Expression): Int = {
    queryExpr match {
      case st: SelectStatement =>
        val countProj = st.projections match {
          case ExpressionProjections(lst) =>
            lst.collect({
              case (StarExpression(_), _) => 1
              case (_, None)              => 1
              case (e, _)                 => unnamedCount(e)
            }).sum
        }
        val countTarget = st.joinTree.map(_.extractSubqueries.map(x => unnamedCount(x.subquery)).sum).getOrElse(0)
        val countWhere = st.where match {
          case None    => 0
          case Some(v) => unnamedCount(v)
        }
        countProj + countTarget + countWhere
      case UnionIntersectSequence(e1, e2, _) => unnamedCount(e1) + unnamedCount(e2)
      case FieldIdent(None, _, _)            => 1
      case ExpressionShape(_, children)      => children.map(unnamedCount).sum
      case _                                 => 0
    }
  }

  val simpleQueriesFolder = "experimentation/dbtoaster/queries/simple"
  val tpchQueriesFolder = "experimentation/dbtoaster/queries/tpch"

  "SQLNamer" should "infer the names correctly for a simple select all query" in {
    val file = new java.io.File(s"$simpleQueriesFolder/r_selectstar.sql")
    val namedQuery = parseAndNameQuery(file)
    unnamedCount(namedQuery) should be(0)
  }

  "SQLNamer" should "infer the names correctly for a join select all query" in {
    val file = new java.io.File(s"$simpleQueriesFolder/rs_eqineq.sql")
    val namedQuery = parseAndNameQuery(file)
    unnamedCount(namedQuery) should be(0)
  }

  "SQLNamer" should "infer the names correctly for a joined select part start query" in {
    val file = new java.io.File(s"$simpleQueriesFolder/rs_selectpartstar.sql")
    val namedQuery = parseAndNameQuery(file)
    unnamedCount(namedQuery) should be(0)
  }

  "SQLNamer" should "infer the names correctly for a select all of subquery" in {
    val file = new java.io.File(s"$simpleQueriesFolder/r_starofnested.sql")
    val namedQuery = parseAndNameQuery(file)
    // println(namedQuery)
    unnamedCount(namedQuery) should be(0)
  }

  "SQLNamer" should "infer the names correctly for a select all in an exists subquery" in {
    val file = new java.io.File(s"$simpleQueriesFolder/r_simplenest.sql")
    val namedQuery = parseAndNameQuery(file)
    // println(namedQuery)
    unnamedCount(namedQuery) should be(0)
  }

  "SQLNamer" should "infer the names correctly in renaming" in {
    val file = new java.io.File(s"$simpleQueriesFolder/r_nestedrename.sql")
    val namedQuery = parseAndNameQuery(file)
    // println(namedQuery)
    unnamedCount(namedQuery) should be(0)
  }

  "SQLNamer" should "infer the names correctly for all simple queries" in {
    val f = new java.io.File(simpleQueriesFolder)
    val files = if (!f.exists) {
      throw new Exception(s"$f does not exist!")
    } else
      f.listFiles.map(simpleQueriesFolder + "/" + _.getName).toList
    for (file <- files) {
      // println(s"naming $file")
      val namedQuery = parseAndNameQuery(new java.io.File(file))
      unnamedCount(namedQuery) should be(0)
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
      unnamedCount(namedQuery) should be(0)
    }
  }
}
