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

  "SQLNamer" should "infer the names correctly for a simple select all query" in {
    val parser = SQLParser
    val folder = "experimentation/dbtoaster/queries/simple"
    val file = new java.io.File(s"$folder/r_selectstar.sql")
    println(s"naming $file")
    val sqlParserTree = parser.parseStream(scala.io.Source.fromFile(file).mkString)
    val sqlProgram = sqlParserTree.asInstanceOf[IncludeStatement]
    val tables = sqlProgram.streams.toList.map(x => x.asInstanceOf[CreateStream])
    val ddlInterpreter = new DDLInterpreter(new Catalog(scala.collection.mutable.Map()))
    val query = sqlProgram.body
    val schema = ddlInterpreter.interpret(UseSchema("DBToaster") :: tables)
    val namedQuery = new SQLNamer(schema).nameQuery(query)
    println(schema)
    println(namedQuery)
  }
}
