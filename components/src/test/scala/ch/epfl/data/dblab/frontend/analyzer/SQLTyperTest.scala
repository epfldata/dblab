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

class SQLTyperTest extends FlatSpec {

  def parseAndNameAndTypeQuery(queryFile: java.io.File) = {
    val parser = SQLParser
    val sqlParserTree = parser.parseStream(scala.io.Source.fromFile(queryFile).mkString)
    val sqlProgram = sqlParserTree.asInstanceOf[IncludeStatement]
    val tables = sqlProgram.streams.toList.collect { case x: CreateStream => x }
    val ddlInterpreter = new DDLInterpreter(new Catalog(scala.collection.mutable.Map()))
    val query = sqlProgram.body
    println(query)
    // unnamedCount(query) should not be (0)
    val schema = ddlInterpreter.interpret(UseSchema("DBToaster") :: tables)
    val namedQuery = new SQLNamer(schema).nameQuery(query)
    val typedQuery = new SQLTyper(schema).typeQuery(namedQuery)
    // println(schema)
    typedQuery
  }

  def untypedCount(queryExpr: Expression): Int = {
    queryExpr match {
      case st: SelectStatement =>
        val countProj = st.projections match {
          case ExpressionProjections(lst) =>
            lst.map({
              case (e, _) if e.tpe == null => throw new Exception(s"$e (:${e.getClass}) has null type. Query: $queryExpr")
              case (e, _)                  => untypedCount(e)
            }).sum
        }
        val countTarget = st.joinTree.map(_.extractSubqueries.map(x => untypedCount(x.subquery)).sum).getOrElse(0)
        val countWhere = st.where match {
          case None    => 0
          case Some(v) => untypedCount(v)
        }
        countProj + countTarget + countWhere
      case UnionIntersectSequence(e1, e2, _) => untypedCount(e1) + untypedCount(e2)
      case FieldIdent(None, _, _) if queryExpr.tpe == null => 1
      case ExpressionShape(_, children) if queryExpr.tpe == null => 1
      case ExpressionShape(_, children) => children.map(untypedCount).sum
      case _ => 0
    }
  }

  val simpleQueriesFolder = "experimentation/dbtoaster/queries/simple"
  val tpchQueriesFolder = "experimentation/dbtoaster/queries/tpch"

  "SQLTyper" should "infer the types correctly for all simple queries" in {
    val f = new java.io.File(simpleQueriesFolder)
    val files = if (!f.exists) {
      throw new Exception(s"$f does not exist!")
    } else
      f.listFiles.map(simpleQueriesFolder + "/" + _.getName).toList
    for (file <- files) {
      println(s"naming $file")
      val typedQuery = parseAndNameAndTypeQuery(new java.io.File(file))
      untypedCount(typedQuery) should be(0)
    }
  }

  "SQLTyper" should "infer the types correctly for all tpch queries" in {
    val f = new java.io.File(tpchQueriesFolder)
    val files = if (!f.exists) {
      throw new Exception(s"$f does not exist!")
    } else
      f.listFiles.filter(_.getName.startsWith("query")).map(tpchQueriesFolder + "/" + _.getName).toList
    for (file <- files) {
      // println(s"naming $file")
      val typedQuery = parseAndNameAndTypeQuery(new java.io.File(file))
      untypedCount(typedQuery) should be(0)
    }
  }
}
