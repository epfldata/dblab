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
import ch.epfl.data.sc.pardis.annotations.mutable
import ch.epfl.data.sc.pardis.types.{ IntType, Tpe }
import frontend.parser.OperatorAST._
import config._
import schema._
import sc.pardis.language.Language

import scala.collection.mutable.ArrayBuffer

object Driver {
  /**
   * The starting point of DBToaster
   *
   * @param args the setting arguments passed through command line
   */
  def main(args: Array[String]) {
    if (args.length < 1) {
      System.out.println("ERROR: Invalid number (" + args.length + ") of command line arguments!")
      System.out.println("USAGE: run <SQL/Calc query> -l SQL|CALC|PLAN|M3 -O")
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
      case Some("PLAN") => CalcPlan
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
        // println(query)
        val schema = ddlInterpreter.interpret(UseSchema("DBToaster") :: tables)

        def listOfQueries(q: TopLevelStatement): List[TopLevelStatement] = {
          q match {
            case u: UnionIntersectSequence if u.connectionType.equals(SEQUENCE) =>
              List(u.top) ++ listOfQueries(u.bottom)
            case x => List(x)
          }
        }
        val queries = listOfQueries(query)

        val sqlToCalc = new SQLToCalc(schema)

        // sql_to_calc.init()
        val namer = new SQLNamer(schema)
        val typer = new SQLTyper(schema)
        // val tpchSchema = TPCHSchema.getSchema("experimentation/dbtoaster/queries/sf0.001/", 0.0001)
        // val calcCoster = new CalcCosting(tpchSchema)

        queries.flatMap({ q =>
          val namedQuery = namer.nameQuery(q)
          val typedQuery = typer.typeQuery(namedQuery)
          val calcExpr = sqlToCalc.calcOfQuery(None, typedQuery)

          // println()
          // println("Costing : ")
          // println()
          //          println(schema)
          //          println()
          //          println(tpchSchema)

          // calcExpr.foreach({
          //   case (name, exp) =>
          //     println(name + ":")
          //     println(calcCoster.cost(exp))
          // })
          // println()
          // println()
          // println()

          calcExpr.map({ case (tgt_name, tgt_calc) => tgt_calc })
        })
      }
      def getCalcPlan(calcExprs: List[CalcExpr]): (Plan, List[CalcQuery]) = {
        val queries = calcExprs.collect({ case ce: CalcQuery => ce })
        CalcCompiler.compile(Some(1), queries, Schema(ArrayBuffer(), Statistics()))
      }
      outputLang match {
        case Calc =>
          val calcExprs = getCalc()
          for (calcExpr <- calcExprs) {
            calcExpr match {
              case CalcQuery(x, y) => println(s"$x:\n${prettyprint(y)}")
              case _               =>
            }
          }
        case CalcPlan =>
          println("Outputing PLAN")
          val calcExprs = getCalc()
          val (plan, qs) = getCalcPlan(calcExprs)
          for (cds <- plan.list) {
            println("description")
            println(cds.description)
            println("triggers")
            println(cds.triggers)

          }
        // println(plan)
        // println(qs)
        case M3 =>
          println("Outputing M3")
          val calcExprs = getCalc()
          val (plan, qs) = getCalcPlan(calcExprs)
          val m3 = PlanToM3.planToM3(Schema(ArrayBuffer(), Statistics()), plan)
          println(m3)
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