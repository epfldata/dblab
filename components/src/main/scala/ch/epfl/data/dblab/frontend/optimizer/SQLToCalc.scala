package ch.epfl.data
package dblab
package frontend
package optimizer

import parser.SQLAST._
import parser.CalcAST._
import schema._
import sc.pardis.types._

/**
  * Converts SQL queries into a calculus representation.
  *
  * @author Mohsen Ferdosi
  */

class SQLToCalc(schema: Schema) {

  //TODO calc of query
  //def CalcOfQuery ( tables: List[createStream] , sqlTree: SelectStatement) : List[(String,CalcExpr)] = {
  //}

//  def CalcOfSelect( tables: List[createStream] , sqlTree: SelectStatement) : List[(String,CalcExpr)] = {
//    val sources = sqlTree.joinTree
//    val source_calc = calc_of_source( tables , sources )
//
//  }

  //TODO sources shouldn't be optional and maybe list ?
  def calc_of_source ( tables: List[createStream], sources: Option[Relation] ): CalcExpr = {

    val rels = sources match {
      case Some(x) => x.extractTables.toList
      case _ => List()
    }

    val subqs = sources match {
      case Some(x) => x.extractSubqueries.toList
      case _ => List()
    }


    CalcProd(
        rels.map( x =>
          Rel("Rel",
          x.name, {
              val table = find_table( x.name , tables ).getOrElse(throw new Exception(s"Relation ${x.name} does not exist"))
              table.cols.toList.map( y => var_of_sql_var( x.name , y._1 , y._2 ) )
            }
          ,"")
        )
    )
    //TODO subqs



  }

  //TODO calc of condition
//  def calc_of_condition ( tables: List[createStream], sources: Option[Relation] , cond: Option[Expression] ): CalcExpr = {
//
//      //TODO rcr_e
//  }

  //TODO optional params
//  def calc_of_sql_expr( tables: List[createStream] , sources: Relation, expr: Expression ): CalcExpr = {
//
//    val ( calc, contains_target ) = expr match {
//      case
//    }
//
//  }


  def var_of_sql_var ( R_name: String , col_name:String , tp: String ): VarT = {
    tp match {
      case "INT" => VarT( R_name+"_"+col_name, IntType )
      //TODO rest
    }
  }

  def find_table ( rel_name: String, tables: List[createStream] ): Option[createStream] = {
      tables.find( x => x.name == rel_name )
  }

}
