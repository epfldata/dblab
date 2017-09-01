package ch.epfl.data
package dblab
package dbtoaster

import schema._
import frontend.parser._
import frontend.optimizer._
import frontend.analyzer._
import utils.Utilities._
import java.io.PrintStream

import ch.epfl.data.dblab.frontend.parser.CalcAST._
import ch.epfl.data.dblab.frontend.parser.DDLAST.UseSchema
import ch.epfl.data.dblab.frontend.parser.SQLAST._
import frontend.parser.OperatorAST._
import config._
import schema._
import sc.pardis.language.Language

object Driver {
  /**
   * The starting point of DBToaster
   *
   * @param args the setting arguments passed through command line
   */
  def main(args: Array[String]) {
    if (args.length < 1) {
      System.out.println("ERROR: Invalid number (" + args.length + ") of command line arguments!")
      System.out.println("USAGE: run <SQL/Calc query> -l SQL|M3|CALC -O")
      System.out.println("Example: run experimentation/tpch-sql/Q6.sql")
      System.exit(1)
    }
    val options = nextOption(Map(), args.toList)
    val queryFiles = options.get('queries) match {
      case Some(l: List[String]) => l
      case x                     => throw new Exception(s"No queries provided: $x")
    }
    val outputLang: Language = options.get('lang).map(_.toString().toUpperCase()) match {
      case Some("CALC") => Calc
      case Some("M3")   => M3
      case Some("SQL")  => SQL
      case _ =>
        throw new Exception("No proper -l defined!")
    }
    val shouldOptimize = options.get('opt).map(_.asInstanceOf[Boolean]).getOrElse(false)

    for (q <- queryFiles) {
      def getCalc(): List[CalcExpr] = if (q.endsWith(".calc")) {
        CalcParser.parse(scala.io.Source.fromFile(q).mkString)
      } else {
        val sqlParserTree = SQLParser.parseStream(scala.io.Source.fromFile(q).mkString)
        val sqlProgram = sqlParserTree.asInstanceOf[IncludeStatement]
        val tables = sqlProgram.streams.toList.map(x => x.asInstanceOf[CreateStream]) // ok ?
        val ddlInterpreter = new DDLInterpreter(new Catalog(scala.collection.mutable.Map()))
        val query = sqlProgram.body
        println(query)
        val schema = ddlInterpreter.interpret(UseSchema("DBToaster") :: tables)

        def listOfQueries(q: TopLevelStatement): List[TopLevelStatement] = {
          q match {
            case u: UnionIntersectSequence if u.connectionType.equals(SEQUENCE) =>
              List(u.top) ++ listOfQueries(u.bottom)
            case x => List(x)
          }
        }
        val queries = listOfQueries(query)

        queries.flatMap({ q =>
          val namedQuery = new SQLNamer(schema).nameQuery(q)
          val calc_expr = SQLToCalc.CalcOfQuery(None, tables, namedQuery)
          calc_expr.map({ case (tgt_name, tgt_calc) => tgt_calc })
        })
      }
      outputLang match {
        case Calc =>
          val ParserTree = getCalc()

          println("BEFORE: \n" + ParserTree.foldLeft("")((acc, cur) => s"${acc} \n${prettyprint(cur)}"))
          if (shouldOptimize) {
            val optimizer = CalcOptimizer
            // println("MIDDLE")
            println("AFTER: \n" + ParserTree.foldLeft("")((acc, cur) => s"${acc} \n${CalcAST.prettyprint(optimizer.normalize(optimizer.nestingRewrites(cur)))}"))
          }
        case lang =>
          throw new Exception(s"Outputing language $lang is not supported yet!")

      }
    }
  }

  type OptionMap = Map[Symbol, Any]

  def nextOption(map: OptionMap, list: List[String]): OptionMap = {
    list match {
      case Nil => map
      case "-O" :: tail =>
        nextOption(map ++ Map('opt -> true), tail)
      case "-l" :: value :: tail =>
        nextOption(map ++ Map('lang -> value), tail)
      case string :: tail =>
        println(s"foo-$string")
        nextOption(map ++ Map('queries -> (map.getOrElse('queries, List()).asInstanceOf[List[String]] :+ string)), tail)
    }
  }
}