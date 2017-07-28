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

  def CalcOfSelect( tables: List[createStream] , sqlTree: SelectStatement) : List[(String,CalcExpr)] = {
    val sources = sqlTree.joinTree
    val targets = sqlTree.projections
    val source_calc = calc_of_source( tables , sources )
    // int of extractAliases is removed and the order is changed to OCaml's order
    val (agg_tgts, noagg_tgts) = targets.extractAliases().map(e => (e._2,e._1)).toList.partition( x => is_agg_expr(x._2) )


    //TODO
    //agg_tgts.map()
    List()
  }

  // sources shouldn't be optional and maybe list ?
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

  def calc_of_condition ( tables: List[createStream], sources: Relation , cond: Option[Expression] ): CalcExpr = {
    
    //TODO push_down_nots
    cond match{
      //TODO and , or
      case b:BinaryOperator =>
        val e1_calc = calc_of_sql_expr(None,None,tables,sources,b.left)
        val e2_calc = calc_of_sql_expr(None,None,tables,sources,b.right)

        val (e1_val,e1_calc2) = lift_if_necessary(None,None,e1_calc)
        val (e2_val,e2_calc2) = lift_if_necessary(None,None,e2_calc)

        cond.get match{
          case _:Equals =>
            mk_aggsum( List(), CalcProd( List(e1_calc2, e2_calc2 , Cmp( Eq , e1_val , e2_val  ) ) ) )

            // TODO rest cases
        }
    }
    // TODO rest cases
  }

  //TODO type of optional params ?
  def calc_of_sql_expr( tgt_var: Option[Unit] , materialize_query: Option[Unit], tables: List[createStream], sources: Relation, expr: Expression )
  : CalcExpr = {

    val ( calc, contains_target ) = expr match {
      //TODO rest cases
      case f:FieldIdent  =>
        (make_CalcExpr(make_ArithExpr(var_of_sql_var(f.qualifier.get, f.name, f.symbol.toString ))),false) // TODO symbol to string ?
    }

    //TODO tgt_var lift
    (tgt_var,contains_target) match{
      case _ =>  calc

    }
  }


  def var_of_sql_var ( R_name: String , col_name:String , tp: String ): VarT = {
    tp match {
      case "INT" => VarT( R_name+"_"+col_name, IntType )
      //TODO rest
    }
  }

  //TODO type of optional params ?
  def lift_if_necessary ( t: Option[Unit] , vt: Option[Unit] , calc: CalcExpr ): ( ArithExpr , CalcExpr ) = {
    combine_values(None,None,calc) match {
      case c: CalcValue => ( c.v , CalcValue( ArithConst( IntLiteral(1)) ) )
      // TODO rest cases
    }
  }

  def mk_aggsum( gb_vars: List[VarT] , expr: CalcExpr ): CalcExpr = {
    expr
    // TODO check if need to be changed using schema_of_expr
    //AggSum(new_gb_vars,expr)
  }

  //TODO lift_group_by_vars_are_inputs
//  def schema_of_expr ( expr:CalcExpr ): ( List[VarT] , List[VarT]) = {
//
//
//  }

  def find_table ( rel_name: String, tables: List[createStream] ): Option[createStream] = {
      tables.find( x => x.name == rel_name )
  }

  // Arithmetic.mk_var
  def make_ArithExpr ( v: VarT ): ArithExpr = {
      ArithVar(v)
  }

  // C.make_value
  def make_CalcExpr ( v: ArithExpr ): CalcExpr = {
    CalcValue(v)
  }

  // ********SQL********* :
  def is_agg_expr ( expr: Expression ): Boolean = {
    // There is a isAggregateOpExpr in scala but seems different
    expr match {
      case _:LiteralExpression => false
      case _:FieldIdent => false
      case b:BinaryOperator => is_agg_expr(b.left) || is_agg_expr(b.right)
      case u:UnaryOperator => is_agg_expr(u.expr)
      case _:Aggregation => true
      case f:FunctionExp => f.inputs.map( x => is_agg_expr(x) ).foldLeft(false)( (sum , cur) => sum||cur )
      //TODO nested_q , cases in OCaml and others in Scala
    }
  }


  // ********CalculusTransforms********** :
  def combine_values ( aggresive: Option[Boolean] , peer_group: Option[List[Unit]] , big_expr: CalcExpr ): CalcExpr = {
    big_expr
    //TODO
  }

}
