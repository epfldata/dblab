package ch.epfl.data
package dblab
package frontend
package optimizer

import ch.epfl.data.dblab.frontend.parser.CalcAST
import parser.SQLAST._
import parser.CalcAST._
import schema._
import sc.pardis.types._

/**
 * Converts SQL queries into a calculus representation.
 *
 * @author Mohsen Ferdosi
 */

object SQLToCalc {

  //TODO calc of query
  def CalcOfQuery(query_name: Option[String], tables: List[createStream], query: TopLevelStatement): List[(String, CalcExpr)] = {

    val re_hv_query = rewrite_having_query(tables, query)
    val agg_query = cast_query_to_aggregate(tables, re_hv_query)

    agg_query match {
      case _: SelectStatement => CalcOfSelect(query_name, tables, agg_query.asInstanceOf[SelectStatement])
    }
  }

  def CalcOfSelect(query_name: Option[String], tables: List[createStream], sqlTree: SelectStatement): List[(String, CalcExpr)] = {
    val targets = sqlTree.projections
    val sources = sqlTree.joinTree
    val cond = sqlTree.where
    val gb_vars = sqlTree.groupBy
    val source_calc = calc_of_source(tables, sources)
    println(s"#############source_calc : ")
    //println(CalcAST.prettyprint(source_calc))
    println(source_calc)
    val cond_calc = calc_of_condition(tables, sources.get, cond)
    println(cond_calc)
    // int of extractAliases is removed and the order is changed to OCaml's order
    val (agg_tgts, noagg_tgts) = targets.extractAliases().map(e => (e._2, e._1)).toList.partition(x => is_agg_expr(x._2))

    // TODO list.split of noogg_tgts for noagg_vars and noagg_terms
    val noagg_vars = List()
    val noagg_terms = List()

    val noagg_calc = CalcProd(noagg_terms)
    val new_gb_vars = List() //TODO make new_gb_vars

    def mq(agg_type: Aggregation, agg_calc: CalcExpr): CalcExpr = {
      val ret = agg_type match {
        case s: Sum =>
          AggSum(new_gb_vars, CalcProd(List(source_calc, cond_calc, agg_calc, noagg_calc)))
        //TODO other cases
      }
      ret
    }

    println(agg_tgts)

    agg_tgts.map({
      case (tgt_name, unnormalized_tgt_expr) =>
        println(tgt_name)
        println(unnormalized_tgt_expr)
        val tgt_expr = normalize_agg_target_expr(None, gb_vars, unnormalized_tgt_expr)
        (tgt_name, calc_of_sql_expr(None, Some(mq), tables, sources.get, tgt_expr))
    })

  }

  // sources shouldn't be optional and maybe list ?
  def calc_of_source(tables: List[createStream], sources: Option[Relation]): CalcExpr = {

    val rels = sources match {
      case Some(x) => x.extractTables.toList
      case _       => List()
    }

    val subqs = sources match {
      case Some(x) => x.extractSubqueries.toList
      case _       => List()
    }

    CalcProd(
      rels.map(x =>
        Rel("Rel",
          x.name, {
            val table = find_table(x.name, tables).getOrElse(throw new Exception(s"Relation ${x.name} does not exist"))
            table.cols.toList.map(y => var_of_sql_var(x.name, y._1, y._2))
          }, "")))
    //TODO subqs
  }

  def calc_of_condition(tables: List[createStream], sources: Relation, cond: Option[Expression]): CalcExpr = {

    //TODO push_down_nots
    cond match {
      //TODO and , or
      case Some(b: BinaryOperator) =>
        println()
        println(b)
        println(b.left)

        val e1_calc = calc_of_sql_expr(None, None, tables, sources, b.left)
        val e2_calc = calc_of_sql_expr(None, None, tables, sources, b.right)

        val (e1_val, e1_calc2) = lift_if_necessary(None, None, e1_calc)
        val (e2_val, e2_calc2) = lift_if_necessary(None, None, e2_calc)

        cond.get match {
          case _: Equals =>
            mk_aggsum(List(), CalcProd(List(e1_calc2, e2_calc2, Cmp(Eq, e1_val, e2_val))))

          // TODO rest cases
        }
    }
    // TODO rest cases
  }

  //TODO type of optional params ?
  def calc_of_sql_expr(tgt_var: Option[Unit], materialize_query: Option[(Aggregation, CalcExpr) => CalcExpr],
                       tables: List[createStream], sources: Relation, expr: Expression): CalcExpr = {

    def rcr_e(is_agg: Option[Boolean], e: Expression): CalcExpr = {
      if (is_agg.isDefined && is_agg.get) {
        calc_of_sql_expr(None, None, tables, sources, e)
      } else {
        calc_of_sql_expr(None, materialize_query, tables, sources, e)
      }
    }

    val (calc, contains_target) = expr match {

      case f: FieldIdent =>
        //(make_CalcExpr(make_ArithExpr(var_of_sql_var(f.qualifier.get, f.name, f.symbol.toString))), false) // TODO symbol to string ?
        println(f)
        //(make_CalcExpr(make_ArithExpr(var_of_sql_var(f.qualifier.get, f.name, "INTEGER"))), false) // TODO this is for test
        (make_CalcExpr(make_ArithExpr(var_of_sql_var("X", f.name, "INTEGER"))), false) // TODO this is for test2

      case b: BinaryOperator =>
        b match {
          case m: Multiply =>
            (CalcProd(List(rcr_e(None, m.left), rcr_e(None, m.right))), false)
          case a: Add =>
            val (e1_calc, e2_calc) = (is_agg_expr(a.left), is_agg_expr(a.right)) match {
              case (true, true) | (false, false) => (rcr_e(None, a.left), rcr_e(None, a.right))
              //TODO other cases + extend_sum_with_agg
            }
            (CalcSum(List(e1_calc, e2_calc)), false)

          //TODO other cases
        }

      case a: Aggregation =>
        a match {
          case s: Sum =>
            val agg_expr = s.expr
            (materialize_query match {
              case Some(mq) => mq(a, rcr_e(Some(true), agg_expr))
              //case None =>  //TODO failwith
            }, false)

          //TODO other cases
        }

      //TODO other cases

    }

    //TODO tgt_var lift
    (tgt_var, contains_target) match {
      case _ => calc

    }
  }

  def cast_query_to_aggregate(tables: List[createStream], query: TopLevelStatement): TopLevelStatement = {
    query
    // TODO
  }

  def var_of_sql_var(R_name: String, col_name: String, tp: String): VarT = {

    println(R_name)
    println(col_name)
    println(tp)

    tp match {
      case "INTEGER" => VarT(R_name + "_" + col_name, IntType)
      //TODO rest
    }
  }

  def lift_if_necessary(t: Option[Unit], vt: Option[Unit], calc: CalcExpr): (ArithExpr, CalcExpr) = {
    combine_values(None, None, calc) match {
      case c: CalcValue => (c.v, CalcValue(ArithConst(IntLiteral(1))))
      // TODO rest cases
    }
  }

  def mk_aggsum(gb_vars: List[VarT], expr: CalcExpr): CalcExpr = {
    expr
    // TODO check if need to be changed using schema_of_expr
    //AggSum(new_gb_vars,expr)
  }

  //TODO lift_group_by_vars_are_inputs
  //  def schema_of_expr ( expr:CalcExpr ): ( List[VarT] , List[VarT]) = {
  //
  //
  //  }

  def find_table(rel_name: String, tables: List[createStream]): Option[createStream] = {
    tables.find(x => x.name == rel_name)
  }

  // Arithmetic.mk_var
  def make_ArithExpr(v: VarT): ArithExpr = {
    ArithVar(v)
  }

  // C.make_value
  def make_CalcExpr(v: ArithExpr): CalcExpr = {
    CalcValue(v)
  }

  def normalize_agg_target_expr(vars_bound: Option[Boolean], gb_vars: Option[GroupBy], expr: Expression): Expression = {
    expr
    //TODO
  }

  // ********SQL********* :
  def is_agg_expr(expr: Expression): Boolean = {
    // There is a isAggregateOpExpr in scala but seems different
    expr match {
      case _: LiteralExpression => false
      case _: FieldIdent        => false
      case b: BinaryOperator    => is_agg_expr(b.left) || is_agg_expr(b.right)
      case u: UnaryOperator     => is_agg_expr(u.expr)
      case _: Aggregation       => true
      case f: FunctionExp       => f.inputs.map(x => is_agg_expr(x)).foldLeft(false)((sum, cur) => sum || cur)
      //TODO nested_q , cases in OCaml and others in Scala
    }
  }

  def rewrite_having_query(tables: List[createStream], query: TopLevelStatement): TopLevelStatement = {
    query
    // TODO
  }

  // ********CalculusTransforms********** :
  def combine_values(aggresive: Option[Boolean], peer_group: Option[List[Unit]], big_expr: CalcExpr): CalcExpr = {
    big_expr
    //TODO
  }

}
