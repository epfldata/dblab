package ch.epfl.data
package dblab
package frontend
package optimizer

import ch.epfl.data.dblab.frontend.parser.CalcAST
import parser.SQLAST._
import parser.CalcAST._
import optimizer.CalcOptimizer._
//import schema._
import sc.pardis.types._
//import scala.reflect.runtime.{ universe => ru }
//import ru._ //TODO this symbol ?

/**
 * Converts SQL queries into a calculus representation.
 *
 * @author Mohsen Ferdosi
 */

object SQLToCalc {

  def CalcOfQuery(query_name: Option[String], tables: List[CreateStream], query: TopLevelStatement): List[(String, CalcExpr)] = {

    println("query:\n" + query)
    val re_hv_query = rewrite_having_query(tables, query)
    println("re_hv_query:\n" + re_hv_query)
    val agg_query = cast_query_to_aggregate(tables, re_hv_query)
    println("agg_query:\n" + agg_query)

    agg_query match {

      case u: UnionIntersectSequence =>
        def rcr(stmt: TopLevelStatement): List[(String, CalcExpr)] = {
          CalcOfQuery(query_name, tables, stmt)
        }
        def lift_stmt(name: String, stmt: CalcExpr): CalcExpr = {
          CalcProd(List(mk_exists(stmt), Lift(VarT(name, typeOfExpression(stmt)), stmt)))
        }
        (rcr(u.bottom) zip rcr(u.top)).map({
          case ((n1, e1), (n2, e2)) =>
            (n1, CalcSum(List(lift_stmt(n1, e1), lift_stmt(n1, e2))))
        })

      case s: SelectStatement => CalcOfSelect(query_name, tables, s)
    }
  }

  def CalcOfSelect(query_name: Option[String], tables: List[CreateStream], sqlTree: SelectStatement): List[(String, CalcExpr)] = {

    val targets = sqlTree.projections
    val sources = sqlTree.joinTree
    val cond = sqlTree.where
    val gb_vars = sqlTree.groupBy.map(_.keys.collect { case x: FieldIdent => x }).getOrElse(Nil).toList
    val source_calc = calc_of_source(tables, sources)
    val cond_calc = calc_of_condition(tables, sources, cond)
    //    println("source :\n" + source_calc)
    println("source :\n" + CalcAST.prettyprint(source_calc))
    //    println("cond : \n" + cond_calc)
    println("cond :\n" + CalcAST.prettyprint(cond_calc))
    // int of extractAliases is removed and the order is changed to OCaml's order

    val (agg_tgts, noagg_tgts) = targets.extractAliases().map(e => (e._2, e._1)).toList.partition(x => is_agg_expr(x._2))

    println(agg_tgts)
    println(noagg_tgts)

    val (noagg_vars, noagg_terms) = noagg_tgts.map({
      case (base_tgt_name, tgt_expr) =>
        //TODO sql error failwith
        val tgt_name = if (query_name.isEmpty) base_tgt_name else var_of_sql_var(query_name.get, base_tgt_name, "ANY").name
        //        println(tgt_name)
        //        println(tgt_expr.asInstanceOf[FieldIdent].qualifier)
        tgt_expr match {
          case f: FieldIdent if (query_name.isEmpty && f.name == tgt_name) ||
            (var_of_sql_var(f.qualifier.get, f.name, "INTEGER").name == tgt_name) => //TODO f.symbol
            //(var_of_sql_var(f.qualifier.get, f.name, f.symbol match { case Symbol(b) => b }), CalcValue(ArithConst(IntLiteral(1))))
            (var_of_sql_var(f.qualifier.get, f.name, "INTEGER"), CalcValue(ArithConst(IntLiteral(1)))) //TODO type

          case _ =>
            val tgt_var = VarT(tgt_name, IntType) // TODO type should infer with expr_type
            (tgt_var, calc_of_sql_expr(Some(tgt_var), None, tables, sources, tgt_expr))
        }
    }).unzip

    println("NO Aggregate :\n\n")
    println(noagg_vars)

    val noagg_calc = CalcProd(noagg_terms)
    val new_gb_vars = noagg_vars ++ gb_vars.flatMap(f => if (!noagg_tgts.exists({
      case (tgt_name, tgt_expr) =>
        if (f.qualifier.isEmpty && tgt_name == f.name) true
        else if (tgt_expr.equals(f)) true
        else false
    })) {
      println(f)
      //      List(var_of_sql_var(f.qualifier.get, f.name, f.symbol match { case Symbol(b) => b }))
      List(var_of_sql_var(f.qualifier.get, f.name, "INTEGER")) // TODO type
    } else List())

    def mq(agg_type: Aggregation, agg_calc: CalcExpr): CalcExpr = {

      val ret = agg_type match {
        case _: Sum =>
          println(prettyprint(noagg_calc))
          mk_aggsum(new_gb_vars, CalcProd(List(source_calc, cond_calc, agg_calc, noagg_calc)))
        case _: CountAll =>
          //          println("8888888888888888888888888 : ")
          //          println(new_gb_vars)
          //          println(source_calc)
          //          println(cond_calc)
          //          println(noagg_calc)
          mk_aggsum(new_gb_vars, CalcProd(List(source_calc, cond_calc, noagg_calc)))
        case _: CountExpr =>
          mk_aggsum(new_gb_vars, mk_exists(CalcProd(List(source_calc, cond_calc, noagg_calc))))

        //TODO other cases
      }
      println("  r  e  t : ")
      println(prettyprint(ret))
      ret
    }

    agg_tgts.map({
      case (tgt_name, unnormalized_tgt_expr) =>
        val tgt_expr = normalize_agg_target_expr(None, gb_vars, unnormalized_tgt_expr)
        (tgt_name, calc_of_sql_expr(None, Some(mq), tables, sources, tgt_expr))
    })
  }

  def calc_of_source(tables: List[CreateStream], sources: Option[Relation]): CalcExpr = {

    val rels = sources match {
      case Some(x) => x.extractTables.toList
      case _       => List()
    }

    val subqs = sources match {
      case Some(x) => x.extractImmediateSubqueries.toList
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
        subqs.map({ x =>
          val q = x.subquery
          val ref_name = x.alias
          if (is_agg_query(q)) {
            CalcProd(CalcOfQuery(Some(ref_name), tables, q).map({
              case (tgt_name, subq) =>
                mk_domain_restricted_lift(var_of_sql_var(ref_name, tgt_name, "INTEGER"), subq)
              //mk_domain_restricted_lift ( var_of_sql_var( ref_name,tgt_name,typeOfExpression(subq)),subq) // TODO
            }))
          } else {
            val c = CalcOfQuery(Some(ref_name), tables, q)
            if (c.length == 1)
              c.head._2
            else {
              println("BUG : calc of query produced unexpected number of targets for a non-aggregate nested query")
              c.head._2 // TODO how to just trow and error ?
            }
          }
        }))
  }

  def calc_of_condition(tables: List[CreateStream], sources: Option[Relation], cond: Option[Expression]): CalcExpr = {

    def rcr_c(c: Option[Expression]): CalcExpr = {
      calc_of_condition(tables, sources, c)
    }

    def rcr_et(tgt_var: Option[VarT], e: Expression): CalcExpr = {
      calc_of_sql_expr(tgt_var, None, tables, sources, e)
    }

    //TODO push_down_nots
    cond match {
      case Some(a: And) =>
        CalcProd(List(rcr_c(Some(a.left)), rcr_c(Some(a.right))))

      case Some(o: Or) =>
        def extract_ors(inner: Expression): List[Expression] =
          {
            inner match {
              case o: Or =>
                extract_ors(o.left) ++ extract_ors(o.right)
              case c => List(c)
            }
          }

        val sum_calc = CalcSum((extract_ors(o.left) ++ extract_ors(o.right)).map(x => rcr_c(Some(x))))
        println("sum_Calc:\n" + prettyprint(sum_calc))
        val (or_val, or_calc) = lift_if_necessary(sum_calc, Some("or"), Some(IntType))
        mk_aggsum(List(), CalcProd(List(or_calc, Cmp(Gt, or_val, ArithConst(IntLiteral(0))))))

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

      case Some(e: parser.SQLAST.Exists) =>
        val q = e.expr
        val q_calc_unlifted = CalcOfQuery(None, tables, q).head._2
        mk_exists(mk_aggsum(List(), q_calc_unlifted))
      //TODO failwith

      case Some(Not(e: parser.SQLAST.Exists)) =>
        val q = e.expr
        val q_calc_unlifted = CalcOfQuery(None, tables, q).head._2
        mk_not_exists(mk_aggsum(List(), q_calc_unlifted))
      //TODO failwith

      case Some(i: In) =>
        val expr = i.elem
        val l = i.set.toList
        val t = IntType // TODO change with the commented line
        //val t = sql_expr_type(None,expr,tables,sources)
        val v = tmp_var("in", t)
        val (expr_val, expr_calc) = lower_if_value(rcr_et(Some(v), expr))
        l.distinct match {
          case List() =>
            CalcValue(ArithConst(IntLiteral(0)))
          case hd :: List() =>
            CalcProd(List(expr_calc, Cmp(Eq, ArithConst(hd), expr_val))) //
          case ul =>
            CalcProd(List(expr_calc, CmpOrList(expr_val, ul)))

        }
      case _ => CalcValue(ArithConst(IntLiteral(1)))
    }
    // TODO rest cases
  }

  def calc_of_sql_expr(tgt_var: Option[VarT], materialize_query: Option[(Aggregation, CalcExpr) => CalcExpr],
                       tables: List[CreateStream], sources: Option[Relation], expr: Expression): CalcExpr = { // expr was SQL node

    def rcr_e(e: Expression, is_agg: Option[Boolean] = Some(false)): CalcExpr = {
      if (is_agg.isDefined && is_agg.get) {
        calc_of_sql_expr(None, None, tables, sources, e)
      } else {
        calc_of_sql_expr(None, materialize_query, tables, sources, e)
      }
    }

    def extend_sum_with_agg(calc_expr: CalcExpr): CalcExpr = {

      materialize_query match {
        case Some(m) =>
          val count_agg = m(CountAll(), CalcValue(ArithConst(IntLiteral(1)))) // expr will never use
          val count_sch = SchemaOfExpression(count_agg)._2
          CalcProd(List(mk_aggsum(count_sch, CalcAST.Exists(count_agg)), calc_expr))
      }
    }

    val (calc, contains_target) = expr match {

      case l: LiteralExpression =>
        (make_CalcExpr(ArithConst(l)), false)

      case f: FieldIdent =>
        (make_CalcExpr(make_ArithExpr(var_of_sql_var(f.qualifier.get, f.name, "INTEGER"))), false) //TODO type expr

      case b: BinaryOperator =>
        b match {
          case m: Multiply =>
            (CalcProd(List(rcr_e(m.left), rcr_e(m.right))), false)
          case a: Add =>
            val (e1_calc, e2_calc) = (is_agg_expr(a.left), is_agg_expr(a.right)) match {
              case (true, true) | (false, false) => (rcr_e(a.left), rcr_e(a.right))
              case (false, true)                 => (extend_sum_with_agg(rcr_e(a.left)), rcr_e(a.right))
              case (true, false)                 => (rcr_e(a.left), extend_sum_with_agg(rcr_e(a.right)))
            }
            (CalcSum(List(e1_calc, e2_calc)), false)
          case d: Divide =>
            val e1 = d.left
            val e2 = d.right
            val ce1 = rcr_e(e1)
            val ce2 = rcr_e(e2)
            val (e2_val, e2_calc) = lift_if_necessary(ce2)
            val needs_order_flip = !is_agg_expr(e1) && is_agg_expr(e2)
            val nestedSchema = SchemaOfExpression(ce1)._2.union(SchemaOfExpression(ce2)._2)
            (mk_aggsum(nestedSchema, CalcProd(if (needs_order_flip) List(CalcProd(List(e2_calc, ce1)))
            else List(CalcProd(List(ce1, e2_calc)))
              ++ List(CalcValue(ArithFunc("/", List(e2_val), FloatType))))), false)

          //TODO other cases
        }

      case a: Aggregation =>
        a match {
          case s: Sum =>
            val agg_expr = s.expr
            (materialize_query match {
              case Some(mq) => mq(a, rcr_e(agg_expr, Some(true)))
              //case None =>  //TODO failwith
            }, false)
          case _: CountAll =>
            (materialize_query match {
              case Some(mq) => mq(a, rcr_e(IntLiteral(1), Some(true)))
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
            rcr_e(q.projections.extractExpretions().head._1))), false)
      //TODO other cases

    }

    (tgt_var, contains_target) match {
      case (Some(v), false) => Lift(v, calc)
      case _                => calc
    }
  }

  def cast_query_to_aggregate(tables: List[CreateStream], query: TopLevelStatement): TopLevelStatement = {

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
              FieldIdent(target_source, target_name.get, sql_expr_type(None, target_expr, tables, sources))
          }))
          SelectStatement(s.withs, new_targets, s.joinTree, s.where, Some(new_gb_vars),
            s.having, s.orderBy, s.limit, s.aliases)

        case u: UnionIntersectSequence =>
          def rcr(stmt: TopLevelStatement): TopLevelStatement = {
            cast_query_to_aggregate(tables, stmt)
          }
          UnionIntersectSequence(rcr(u.bottom), rcr(u.top), UNION)
      }
    }
  }

  def lower_if_value(calc: CalcExpr): (ArithExpr, CalcExpr) = {
    calc match {
      case Lift(v, CalcValue(x)) =>
        (x, CalcValue(ArithConst(IntLiteral(1))))
      case Lift(v, c) =>
        (ArithVar(v), c)
      case AggSum(v :: List(), c) =>
        (ArithVar(v), calc)
      // TODO failwith
    }
  }

  def var_of_sql_var(R_name: String, col_name: String, tp: String): VarT = {

    tp match {
      case "INTEGER" => VarT(R_name + "_" + col_name, IntType)
      case "STRING"  => VarT(R_name + "_" + col_name, StringType)
      case "ANY"     => VarT(R_name + "_" + col_name, AnyType)
      case "FLOAT"   => VarT(R_name + "_" + col_name, FloatType)

      //TODO rest cases
    }
  }

  def lift_if_necessary(calc: CalcExpr, t: Option[String] = Some("agg"), vt: Option[Tpe] = Some(AnyType)): (ArithExpr, CalcExpr) = {

    combine_values(None, None, calc) match {
      case c: CalcValue => (c.v, CalcValue(ArithConst(IntLiteral(1))))
      case _ =>
        println("^^^^^^^^^^^^^")
        println(t)
        println(vt)
        val v = tmp_var(t.get, escalate_type(None, vt.get, type_of_expr(calc)))
        (make_ArithExpr(v), Lift(v, calc))
    }
  }

  var varId = 0
  def tmp_var(vn: String, vt: Tpe): VarT = {
    varId += 1
    val v = VarT("__sql_inline_" + vn + varId, vt)
    v
  }

  var domId = 0
  def mk_dom_var(l: List[Unit], vt: Tpe): VarT = {
    // TODO l ?
    domId += 1
    val v = VarT("__domain_" + domId, vt)
    v
  }

  def find_table(rel_name: String, tables: List[CreateStream]): Option[CreateStream] = {
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
      case s: SelectStatement   => false
      //TODO nested_q , cases in Ocaml and others in Scala
    }
  }

  def is_agg_query(stmt: TopLevelStatement): Boolean = {

    stmt match {
      case s: SelectStatement =>
        s.projections.extractExpretions().map(x => x._1).toList.exists(x => is_agg_expr(x))
      case u: UnionIntersectSequence => is_agg_query(u.bottom) && is_agg_query(u.top)
    }
  }

  def rewrite_having_query(tables: List[CreateStream], query: TopLevelStatement): TopLevelStatement = {
    query
    // TODO
  }

  def sql_expr_type(strict: Option[Boolean], expr: Expression, tables: List[CreateStream], sources: Option[Relation]): Symbol = {
    'INTEGER
    // TODO
  }

  // ********Calculus********** :

  def mk_aggsum(gb_vars: List[VarT], expr: CalcExpr): CalcExpr = {
    val expr_ovars = SchemaOfExpression(expr)._2
    val new_gb_vars = expr_ovars.intersect(gb_vars)
    //
    //    println("   mk_aggsum    :")
    //    println(prettyprint(expr))
    //    println(gb_vars)
    //    println(expr_ovars)
    //    println(new_gb_vars)

    if (expr_ovars.equals(new_gb_vars))
      expr
    else
      AggSum(new_gb_vars, expr)
  }

  def type_of_expr(expr: CalcExpr): Tpe = {
    IntType
  }

  def CalculusFold[A](sumFn: (Schema_t, List[A]) => A, prodFn: (Schema_t, List[A]) => A, negFn: (Schema_t, A) => A,
                      leafFn: (Schema_t, CalcExpr) => A, e: CalcExpr, scope: Option[List[VarT]] = Some(List()),
                      schema: Option[List[VarT]] = Some(List())): A = {

    def rcr(e_scope: Option[List[VarT]], e_shema: Option[List[VarT]], e2: CalcExpr): A = {
      CalculusFold(sumFn, prodFn, negFn, leafFn, e2)
    }

    e match {

      case s: CalcSum =>
        val terms = s.exprs
        sumFn(Schema_t(scope, schema), terms.map(x => rcr(scope, schema, x)))

      case p: CalcProd =>
        val terms = p.exprs
        def fun(prev: List[CalcExpr], curr: CalcExpr, next: List[CalcExpr]): A = {
          rcr(
            Some((scope ++ prev.map(x => SchemaOfExpression(x)._2)).foldLeft(List[VarT]())((a, b) => a.union(b))),
            Some((schema ++ next.map({ x =>
              val (xin, xout) = SchemaOfExpression(x)
              xin.union(xout)
            })).foldLeft(List[VarT]())((a, b) => a.union(b))),
            curr)
        }
        prodFn(Schema_t(scope, schema), scan_map(fun, terms))

      case n: CalcNeg =>
        val term = n.expr
        negFn(Schema_t(scope, schema), rcr(scope, schema, term))

      case _ =>
        leafFn(Schema_t(scope, schema), e)
    }
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
  def combine_values(aggressive: Option[Boolean], peer_group: Option[List[Unit]], big_expr: CalcExpr): CalcExpr = {
    big_expr
    //TODO
  }

  //   ********CalculusDomains********** :

  def maintain(formula: CalcExpr): CalcExpr = {

    CalculusFold[CalcExpr](
      (_, sl) => CalcSum({
        if (sl.isEmpty) List()
        else {
          sl.filter(x => !x.equals(CalcValue(ArithConst(IntLiteral(1))))) match {
            case List() => List(CalcValue(ArithConst(IntLiteral(1))))
            case x      => x
          }
        }
      }),
      (_, x) => CalcProd(x),
      (_, x) => x,
      (_, lf) =>
        lf match {
          case r: Rel                  => r
          case _: CalcValue            => CalcValue(ArithConst(IntLiteral(1)))
          case AggSum(gb_vars, subexp) => mk_aggsum(gb_vars, maintain(subexp))
          case CalcAST.Exists(subexp)  => mk_exists(subexp)
          case x                       => x //TODO CalcValue(Lift) ?
        }, formula)
  }

  def mk_exists(expr: CalcExpr): CalcExpr = {
    val dom_expr = maintain(expr)
    //    println("**********************************")
    //    println(prettyprint(expr))
    //    println(prettyprint(dom_expr))
    CalcAST.Exists(dom_expr)
  }

  def mk_not_exists(expr: CalcExpr): CalcExpr = {
    val dom_expr = maintain(expr)
    val dom_var = mk_dom_var(List(), IntType)
    val ovars = SchemaOfExpression(expr)._2
    mk_aggsum(ovars, CalcProd(List(Lift(dom_var, dom_expr), Cmp(Eq, ArithConst(IntLiteral(0)), ArithVar(dom_var)))))
  }

  def mk_domain_restricted_lift(lift_v: VarT, lift_expr: CalcExpr): CalcExpr = {
    val lift = Lift(lift_v, lift_expr)
    val ovars = SchemaOfExpression(lift_expr)._2
    if (ovars.isEmpty) lift
    else CalcProd(List(mk_exists(lift_expr), lift))
  }

  //   ********List********** :

  def scan_map[A, B](f: (List[A], A, List[A]) => B, l: List[A]): List[B] = {
    def iterate(prev: List[A], curr_next: List[A]): List[B] = {
      curr_next match {
        case List()       => List()
        case curr :: next => List(f(prev, curr, next)) ++ iterate(prev ++ List(curr), next)
      }
    }
    iterate(List(), l)
  }

}