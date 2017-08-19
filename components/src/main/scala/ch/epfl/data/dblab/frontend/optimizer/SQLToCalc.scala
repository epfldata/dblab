package ch.epfl.data
package dblab
package frontend
package optimizer

import ch.epfl.data.dblab.frontend.parser.CalcAST
import parser.SQLAST._
import parser.CalcAST._
import schema._
import sc.pardis.types._
//import scala.reflect.runtime.{ universe => ru }
//import ru._ //TODO this symbol ?

/**
 * Converts SQL queries into a calculus representation.
 *
 * @author Mohsen Ferdosi
 */

object SQLToCalc {

  def CalcOfQuery(query_name: Option[String], tables: List[createStream], query: TopLevelStatement): List[(String, CalcExpr)] = {

    println("query:\n" + query)
    val re_hv_query = rewrite_having_query(tables, query)
    println("re_hv_query:\n" + re_hv_query)
    val agg_query = cast_query_to_aggregate(tables, re_hv_query)
    println("agg_query:\n" + agg_query)

    agg_query match {
      case _: SelectStatement => CalcOfSelect(query_name, tables, agg_query.asInstanceOf[SelectStatement])
      //TODO union case
    }
  }

  def CalcOfSelect(query_name: Option[String], tables: List[createStream], sqlTree: SelectStatement): List[(String, CalcExpr)] = {

    val targets = sqlTree.projections
    val sources = sqlTree.joinTree
    val cond = sqlTree.where
    val gb_vars = sqlTree.groupBy.map(_.keys.collect { case x: FieldIdent => x }).getOrElse(Nil).toList
    val source_calc = calc_of_source(tables, sources)
    val cond_calc = calc_of_condition(tables, sources.get, cond)
    println("source :\n" + source_calc)
    //println(CalcAST.prettyprint(source_calc))
    println("cond : \n" + cond_calc)
    // int of extractAliases is removed and the order is changed to OCaml's order

    val (agg_tgts, noagg_tgts) = targets.extractAliases().map(e => (e._2, e._1)).toList.partition(x => is_agg_expr(x._2))

    println(agg_tgts)
    println(noagg_tgts)

    val (noagg_vars, noagg_terms) = noagg_tgts.map({
      case (base_tgt_name, tgt_expr) =>
        //TODO sql error failwith
        val tgt_name = if (query_name.isEmpty) base_tgt_name else var_of_sql_var(query_name.get, base_tgt_name, "ANY").name
        println(tgt_name)
        tgt_expr match {
          case f: FieldIdent if (query_name.isEmpty && f.name == tgt_name) ||
            (var_of_sql_var(f.qualifier.get, f.name, f.symbol match { case Symbol(b) => b }).name == tgt_name) =>
            //(var_of_sql_var(f.qualifier.get, f.name, f.symbol match { case Symbol(b) => b }), CalcValue(ArithConst(IntLiteral(1))))
            (var_of_sql_var(f.qualifier.get, f.name, "INTEGER"), CalcValue(ArithConst(IntLiteral(1))))

          case _ => ???
          //val tgt_var = ( tgt_name, ())
          //TODO default -> expr_type
        }
    }).unzip

    println(gb_vars)

    val noagg_calc = CalcProd(noagg_terms)
    val new_gb_vars = noagg_vars ++ gb_vars.flatMap(f => if (!noagg_tgts.exists({
      case (tgt_name, tgt_expr) =>
        if (f.qualifier.isEmpty && tgt_name == f.name) true
        else if (tgt_expr.equals(f)) true
        else false
    }))
      List(var_of_sql_var(f.qualifier.get, f.name, f.symbol match { case Symbol(b) => b }))
    else List())

    def mq(agg_type: Aggregation, agg_calc: CalcExpr): CalcExpr = {

      val ret = agg_type match {
        case s: Sum =>
          mk_aggsum(new_gb_vars, CalcProd(List(source_calc, cond_calc, agg_calc, noagg_calc)))
        case _: CountAll =>
          mk_aggsum(new_gb_vars, CalcProd(List(source_calc, cond_calc, noagg_calc)))

        //TODO other cases
      }
      println("  r  e  t : ")
      println(ret)
      ret
    }

    agg_tgts.map({
      case (tgt_name, unnormalized_tgt_expr) =>
        //        println(tgt_name)
        //        println(unnormalized_tgt_expr)
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
            table.cols.toList.map(y => var_of_sql_var(x.alias.get, y._1, y._2))
          }, ""))
        ++
        List())

    //        subqs.map( {x =>
    //          val q = x.subquery
    //          val ref_name = x.alias
    //          if ( is_agg_query(q))
    //          {
    //            CalcProd( CalcOfQuery(ref_name,tables,q).map({case(tgt_name,subq) =>
    //              mk_domain_restricted_lift ( var_of_sql_var( ref_name,tgt_name,"INTEGER") )
    //            })
    //          }
    //        })
    //TODO uncomment when mk_domain_restricted_lift is defined
  }

  def calc_of_condition(tables: List[createStream], sources: Relation, cond: Option[Expression]): CalcExpr = {

    //TODO push_down_nots
    cond match {
      //TODO or
      case Some(a: And) =>
        CalcProd(List(calc_of_condition(tables, sources, Some(a.left)), calc_of_condition(tables, sources, Some(a.right))))
      case Some(b: BinaryOperator) =>

        val e1_calc = calc_of_sql_expr(None, None, tables, sources, b.left)
        val e2_calc = calc_of_sql_expr(None, None, tables, sources, b.right)

        val (e1_val, e1_calc2) = lift_if_necessary(e1_calc)
        val (e2_val, e2_calc2) = lift_if_necessary(e2_calc)

        b match {
          case _: Equals         => mk_aggsum(List(), CalcProd(List(e1_calc2, e2_calc2, Cmp(Eq, e1_val, e2_val))))
          case _: NotEquals      => mk_aggsum(List(), CalcProd(List(e1_calc2, e2_calc2, Cmp(Neq, e1_val, e2_val))))
          case _: LessOrEqual    => mk_aggsum(List(), CalcProd(List(e1_calc2, e2_calc2, Cmp(Lte, e1_val, e2_val))))
          case _: LessThan       => mk_aggsum(List(), CalcProd(List(e1_calc2, e2_calc2, Cmp(Lt, e1_val, e2_val))))
          case _: GreaterOrEqual => mk_aggsum(List(), CalcProd(List(e1_calc2, e2_calc2, Cmp(Gte, e1_val, e2_val))))
          case _: GreaterThan    => mk_aggsum(List(), CalcProd(List(e1_calc2, e2_calc2, Cmp(Gt, e1_val, e2_val))))
        }
      //      case e: parser.SQLAST.Exists =>
      //        val q = e.expr
      //        val q_calc_unlifted = CalcOfQuery(None,tables,q).head._2
      //        mk_exists(mk_aggsum(List(),q_calc_unlifted))
      //TODO failwith

      case _ => CalcValue(ArithConst(IntLiteral(1)))
    }
    // TODO rest cases
  }

  //TODO type of tgt_var ?
  def calc_of_sql_expr(tgt_var: Option[Unit], materialize_query: Option[(Aggregation, CalcExpr) => CalcExpr],
                       tables: List[createStream], sources: Relation, expr: SQLNode): CalcExpr = {

    def rcr_e(is_agg: Option[Boolean], e: Expression): CalcExpr = {
      if (is_agg.isDefined && is_agg.get) {
        calc_of_sql_expr(None, None, tables, sources, e)
      } else {
        calc_of_sql_expr(None, materialize_query, tables, sources, e)
      }
    }

    println("inja :    ")
    println(expr)

    val (calc, contains_target) = expr match {

      case l: LiteralExpression =>
        (make_CalcExpr(ArithConst(l)), false)

      case f: FieldIdent =>
        //(make_CalcExpr(make_ArithExpr(var_of_sql_var(f.qualifier.get, f.name, f.symbol match { case Symbol(b) => b } ))), false) // TODO symbol to string ?
        //        (make_CalcExpr(make_ArithExpr(var_of_sql_var(f.qualifier.get, f.name, "INTEGER"))), false) // TODO this is for test
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
          case _: CountAll =>
            (materialize_query match {
              case Some(mq) => mq(a, rcr_e(Some(true), IntLiteral(1)))
              //case None =>  //TODO failwith
            }, false)

          //TODO other cases
        }

      case s: SelectStatement =>
        //val q = s.subquery.asInstanceOf[SelectStatement]
        val q = s
        //TODO sql.error failwith
        if (is_agg_query(q))
          (CalcOfQuery(None, tables, q).head._2, false)
        else
          (CalcProd(List(calc_of_condition(tables, sources, q.where),
            rcr_e(None, q.projections.extractExpretions().head._1))), false)
      //TODO other cases

    }

    //TODO tgt_var lift
    (tgt_var, contains_target) match {
      case _ => calc

    }
  }

  def cast_query_to_aggregate(tables: List[createStream], query: TopLevelStatement): TopLevelStatement = {

    println(is_agg_query(query))

    if (is_agg_query(query)) {
      query
    } else {
      query match {
        case s: SelectStatement =>
          val targets = s.projections.extractAliases().toList.map(x => (x._1, Some(x._2)))
          val sources = s.joinTree

          val lst = targets ++ List((CountAll(), Some("COUNT")))
          val new_targets = ExpressionProjections(lst) // TODO distinct is Ocaml?

          val new_gb_vars = GroupBy(targets.map({
            case (target_expr, target_name) =>

              val target_source = target_expr match {
                case f: FieldIdent => if (f.name == target_name.get) f.qualifier else None
                case _             => None
              }
              FieldIdent(target_source, target_name.get, expr_type(None, target_expr, tables, sources.get))
          }))
          SelectStatement(s.withs, new_targets, s.joinTree, s.where, Some(new_gb_vars),
            s.having, s.orderBy, s.limit, s.aliases)
      }
    }
    // TODO union
  }

  def var_of_sql_var(R_name: String, col_name: String, tp: String): VarT = {

    tp match {
      case "INTEGER" => VarT(R_name + "_" + col_name, IntType)
      case "STRING"  => VarT(R_name + "_" + col_name, StringType)
      case "ANY"     => VarT(R_name + "_" + col_name, AnyType)

      //TODO rest cases
    }
  }

  def lift_if_necessary(calc: CalcExpr, t: Option[String] = Some("agg"), vt: Option[Tpe] = Some(AnyType)): (ArithExpr, CalcExpr) = {

    println(t)
    combine_values(None, None, calc) match {
      case c: CalcValue => (c.v, CalcValue(ArithConst(IntLiteral(1))))
      case _ =>
        val v = tmp_var(t.get, escalate_type(None, vt.get, type_of_expr(calc)))
        (make_ArithExpr(v), Lift(v, calc))
    }
  }

  def tmp_var(vn: String, vt: Tpe): VarT = {
    val v = VarT("inline " + vn, vt)
    v
    //TODO inline
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

  def normalize_agg_target_expr(vars_bound: Option[Boolean], gb_vars: List[FieldIdent], expr: Expression): Expression = {
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

  def is_agg_query(stmt: TopLevelStatement): Boolean = {

    stmt match {
      case s: SelectStatement =>
        s.projections.extractExpretions().map(x => x._1).toList.exists(x => is_agg_expr(x))
      //TODO union
    }

  }

  def rewrite_having_query(tables: List[createStream], query: TopLevelStatement): TopLevelStatement = {
    query
    // TODO
  }

  def expr_type(strict: Option[Boolean], expr: Expression, tables: List[createStream], sources: Relation): Symbol = {
    'INTEGER
    // TODO
  }

  // ********Calcules********** :
  def mk_aggsum(gb_vars: List[VarT], expr: CalcExpr): CalcExpr = {
    // TODO use schema_of_expr from Parand's code
    expr
    //AggSum(new_gb_vars,expr)
  }

  def type_of_expr(expr: CalcExpr): Tpe = {
    IntType
  }

  // ********Type********** :
  def escalate_type(opname: Option[String] = Some("<op>"), a: Tpe, b: Tpe): Tpe = {

    (a, b) match {
      case (at, bt) if at.equals(bt) => at
      case (AnyType, t)              => t
      case (t, AnyType)              => t
      // TODO other cases
    }
  }

  // ********CalculusTransforms********** :
  def combine_values(aggresive: Option[Boolean], peer_group: Option[List[Unit]], big_expr: CalcExpr): CalcExpr = {
    big_expr
    //TODO
  }

  //   ********CalculusDomains********** :

  //  def mk_exists ( expr:CalcExpr ) : CalcExpr = {
  //    val dom_expr = maintain(expr)
  //
  //  }

  //    def mk_domain_restricted_lift ( lift_v:VarT , lift_expr:CalcExpr ): CalcExpr = {
  //      val lift = Lift(lift_v,lift_expr)
  //
  //
  //    }

}
