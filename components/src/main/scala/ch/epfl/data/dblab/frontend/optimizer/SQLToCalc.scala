package ch.epfl.data
package dblab
package frontend
package optimizer

import ch.epfl.data.dblab.frontend.parser.{ CalcAST, SQLAST }
import parser.SQLAST.{ CountExpr, LiteralExpression, _ }
import parser.CalcAST.{ Rel, _ }
import optimizer.CalcOptimizer._
import schema._
import sc.pardis.types._
import ch.epfl.data.dblab.frontend.optimizer.CalcUtils._
//import scala.reflect.runtime.{ universe => ru }
import SQLUtils._

/**
 * Converts SQL queries into a calculus representation.
 *
 * @author Mohsen Ferdosi
 */

class SQLToCalc(schema: Schema) {

  def init(): Unit = {
    declareStdFunction("listmax", x => IntType) //TODO
    declareStdFunction("substring", x => StringType) //TODO
    declareStdFunction("cast_int", x => IntType) //TODO
    declareStdFunction("cast_date", x => DateType) //TODO
    declareStdFunction("cast_float", x => FloatType) //TODO
    declareStdFunction("cast_String", x => StringType) //TODO
    declareStdFunction("date_part", x => StringType) //TODO
    declareStdFunction("year", x => StringType) //TODO uppercase in query9!
  }

  def calcOfQuery(query_name: Option[String], query: TopLevelStatement): List[(String, CalcExpr)] = {

    println("query:\n" + query)
    val re_hv_query = rewriteHavingQuery(query)
    println("re_hv_query:\n" + re_hv_query)
    val agg_query = castQueryToAggregate(re_hv_query)
    println("agg_query:\n" + agg_query)

    agg_query match {

      case u: UnionIntersectSequence =>
        def rcr(stmt: TopLevelStatement): List[(String, CalcExpr)] = {
          calcOfQuery(query_name, stmt)
        }
        def lift_stmt(name: String, stmt: CalcExpr): CalcExpr = {
          CalcProd(List(mkExists(stmt), Lift(VarT(name, typeOfExpression(stmt)), stmt)))
        }
        (rcr(u.bottom) zip rcr(u.top)).map({
          case ((n1, e1), (n2, e2)) =>
            (n1, CalcSum(List(lift_stmt(n1, e1), lift_stmt(n1, e2))))
        })

      case s: SelectStatement => calcOfSelect(query_name, s)
    }
  }

  def calcOfSelect(query_name: Option[String], sqlTree: SelectStatement): List[(String, CalcExpr)] = {

    val targets = sqlTree.projections
    val sources = sqlTree.joinTree
    val cond = sqlTree.where
    val gb_vars = sqlTree.groupBy.map(_.keys.collect { case x: FieldIdent => x }).getOrElse(Nil).toList
    val source_calc = calcOfSource(sources)
    val cond_calc = calcOfCondition(sources, cond)
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
        val tgt_name = if (query_name.isEmpty) base_tgt_name else varOfSqlVar(query_name.get, base_tgt_name, AnyType).name
        //        println(tgt_name)
        //        println(tgt_expr.asInstanceOf[FieldIdent].qualifier)
        tgt_expr match {
          case f: FieldIdent if (query_name.isEmpty && f.name == tgt_name) ||
            (varOfSqlVar(f.qualifier.get, f.name, IntType).name == tgt_name) => //TODO f.symbol
            //(var_of_sql_var(f.qualifier.get, f.name, f.symbol match { case Symbol(b) => b }), CalcValue(ArithConst(IntLiteral(1))))
            (varOfSqlVar(f.qualifier.get, f.name, IntType), CalcValue(ArithConst(IntLiteral(1)))) //TODO type

          case _ =>
            val tgt_var = VarT(tgt_name, IntType) // TODO type should infer with expr_type
            (tgt_var, calcOfSqlExpr(Some(tgt_var), None, sources, tgt_expr))
        }
    }).unzip

    println("gb vars  :\n")
    println(gb_vars)

    val noagg_calc = CalcProd(noagg_terms)
    val new_gb_vars = noagg_vars ++ gb_vars.flatMap(f => if (!noagg_tgts.exists({
      case (tgt_name, tgt_expr) =>
        if (f.qualifier.isEmpty && tgt_name == f.name) true
        //        else if (tgt_expr.equals(f)) true // TODO : type is not inferred yet
        else if (tgt_expr.isInstanceOf[FieldIdent] && tgt_expr.asInstanceOf[FieldIdent].qualifier.equals(f.qualifier) && // TODO it should be convertable
          tgt_expr.asInstanceOf[FieldIdent].name.equals(f.name)) true
        else false
    })) {
      //      println(f)
      //      List(var_of_sql_var(f.qualifier.get, f.name, f.symbol match { case Symbol(b) => b }))
      List(varOfSqlVar(f.qualifier.get, f.name, IntType)) // TODO type
    } else List())

    def mq(agg_type: Aggregation, agg_calc: CalcExpr): CalcExpr = {

      val ret = agg_type match {
        case _: Sum =>
          println(prettyprint(noagg_calc))
          mkAggsum(new_gb_vars, CalcProd(List(source_calc, cond_calc, agg_calc, noagg_calc)))

        case _: CountAll =>
          mkAggsum(new_gb_vars, CalcProd(List(source_calc, cond_calc, noagg_calc)))

        case CountExpr(Distinct(StarExpression(_))) =>
          mkAggsum(new_gb_vars, mkExists(CalcProd(List(source_calc, cond_calc, noagg_calc))))

        case CountExpr(Distinct(fields)) => // fields is a list in ocaml but an expression in scala
          val f = fields.asInstanceOf[FieldIdent]
          mkAggsum(new_gb_vars, mkExists(mkAggsum(
            new_gb_vars.union(List(varOfSqlVar(f.qualifier.get, f.name, IntType))), //TODO type infer
            CalcProd(List(source_calc, cond_calc, noagg_calc)))))

        case _: CountExpr =>
          mkAggsum(new_gb_vars, mkExists(CalcProd(List(source_calc, cond_calc, noagg_calc))))

        case _: Avg =>
          val count_var = tmpVar("average_count", IntType)
          mkAggsum(new_gb_vars, CalcProd(List(
            mkAggsum(new_gb_vars, CalcProd(List(source_calc, cond_calc, agg_calc, noagg_calc))),
            Lift(count_var, mkAggsum(new_gb_vars, CalcProd(List(source_calc, cond_calc, noagg_calc)))),
            CalcProd(List(
              CalcValue(ArithFunc("/", List(ArithFunc("listmax", List(ArithConst(IntLiteral(1)),
                ArithVar(count_var)), IntType)), FloatType)),
              Cmp(Neq, ArithConst(IntLiteral(0)), ArithVar(count_var)))))))
      }
      println("  r  e  t : ")
      println(prettyprint(ret))
      ret
    }

    agg_tgts.map({
      case (tgt_name, unnormalized_tgt_expr) =>
        val tgt_expr = normalizeAggTargetExpr(None, gb_vars, unnormalized_tgt_expr)
        (tgt_name, calcOfSqlExpr(None, Some(mq), sources, tgt_expr))
    })
  }

  def calcOfSource(sources: Option[Relation]): CalcExpr = {

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
            val table = findTable(x.name).getOrElse(throw new Exception(s"Relation ${x.name} does not exist"))
            //          table.attributes.map(y => varOfSqlVar(x.alias.get, y.name, y.dataType)) //TODO after fixing typing
            table.attributes.map(y => varOfSqlVar(x.alias.get, y.name, IntType))
          }, ""))

        ++
        subqs.map({ x =>
          val q = x.subquery
          val ref_name = x.alias
          if (isAggQuery(q)) {
            CalcProd(calcOfQuery(Some(ref_name), q).map({
              case (tgt_name, subq) =>
                mkDomainRestrictedLift(varOfSqlVar(ref_name, tgt_name, IntType), subq)
              //mk_domain_restricted_lift ( var_of_sql_var( ref_name,tgt_name,typeOfExpression(subq)),subq) // TODO
            }))
          } else {
            val c = calcOfQuery(Some(ref_name), q)
            if (c.length == 1)
              c.head._2
            else {
              println("BUG : calc of query produced unexpected number of targets for a non-aggregate nested query")
              c.head._2 // TODO how to just trow and error ?
            }
          }
        }))
  }

  def calcOfCondition(sources: Option[Relation], cond: Option[Expression]): CalcExpr = {

    def rcr_c(c: Option[Expression]): CalcExpr = {
      calcOfCondition(sources, c)
    }

    def rcr_et(tgt_var: Option[VarT], e: Expression): CalcExpr = {
      calcOfSqlExpr(tgt_var, None, sources, e)
    }

    def calcOfLike(wrapper: ArithExpr => CalcExpr, expr: Expression, like: Expression) = {

      like match {
        case StringLiteral(s) =>
          val like_regexp = "^" + s.replaceAll("%", ".*") + "$"
          val (expr_val, expr_calc) = liftIfNecessary(calcOfSqlExpr(None, None, sources, expr), Some("like"))
          //TODO failwith
          CalcProd(List(expr_calc,
            wrapper(ArithFunc("regexp_match", List(ArithConst(StringLiteral(like_regexp)), expr_val), IntType))))

        case _ => ???
      }
    }

    pushDownNots(cond) match {

      case Some(Like(expr, like_str)) =>
        calcOfLike(x => Cmp(Eq, ArithConst(IntLiteral(0)), x), expr, like_str)

      case Some(Not(Like(expr, like_str))) =>
        calcOfLike(x => Cmp(Neq, ArithConst(IntLiteral(0)), x), expr, like_str)

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
        val (or_val, or_calc) = liftIfNecessary(sum_calc, Some("or"), Some(IntType))
        mkAggsum(List(), CalcProd(List(or_calc, Cmp(Gt, or_val, ArithConst(IntLiteral(0))))))

      case Some(b: BinaryOperator) =>
        val e1_calc = calcOfSqlExpr(None, None, sources, b.left)
        val e2_calc = calcOfSqlExpr(None, None, sources, b.right)

        val (e1_val, e1_calc2) = liftIfNecessary(e1_calc)
        val (e2_val, e2_calc2) = liftIfNecessary(e2_calc)

        b match {
          case _: Equals         => mkAggsum(List(), CalcProd(List(e1_calc2, e2_calc2, Cmp(Eq, e1_val, e2_val))))
          case _: NotEquals      => mkAggsum(List(), CalcProd(List(e1_calc2, e2_calc2, Cmp(Neq, e1_val, e2_val))))
          case _: LessOrEqual    => mkAggsum(List(), CalcProd(List(e1_calc2, e2_calc2, Cmp(Lte, e1_val, e2_val))))
          case _: LessThan       => mkAggsum(List(), CalcProd(List(e1_calc2, e2_calc2, Cmp(Lt, e1_val, e2_val))))
          case _: GreaterOrEqual => mkAggsum(List(), CalcProd(List(e1_calc2, e2_calc2, Cmp(Gte, e1_val, e2_val))))
          case _: GreaterThan    => mkAggsum(List(), CalcProd(List(e1_calc2, e2_calc2, Cmp(Gt, e1_val, e2_val))))
        }

      case Some(e: parser.SQLAST.Exists) =>
        val q = e.expr
        val q_calc_unlifted = calcOfQuery(None, q).head._2
        mkExists(mkAggsum(List(), q_calc_unlifted))
      //TODO failwith

      case Some(Not(e: parser.SQLAST.Exists)) =>
        val q = e.expr
        val q_calc_unlifted = calcOfQuery(None, q).head._2
        mkNotExists(mkAggsum(List(), q_calc_unlifted))
      //TODO failwith

      case Some(Not(InList(expr, l))) =>
        //        val t =
        val v = Some(tmpVar("in", IntType)) // TODO Expr type
        val (expr_val, expr_calc) = lowerIfValue(rcr_et(v, expr))
        CalcProd(List(expr_calc) ++ l.distinct.map(x => Cmp(Neq, ArithConst(x), expr_val)))

      case Some(i: InList) =>
        val expr = i.e
        val l = i.list
        val t = IntType // TODO change with the commented line after typing
        //val t = sql_expr_type(None,expr,tables,sources)
        val v = tmpVar("in", t)
        val (expr_val, expr_calc) = lowerIfValue(rcr_et(Some(v), expr))
        l.distinct match {
          case List() =>
            CalcValue(ArithConst(IntLiteral(0)))
          case hd :: List() =>
            CalcProd(List(expr_calc, Cmp(Eq, ArithConst(hd), expr_val))) //
          case ul =>
            CalcProd(List(expr_calc, CmpOrList(expr_val, ul.map(ArithConst))))

        }

      case _ => CalcValue(ArithConst(IntLiteral(1)))
    }
  }

  def calcOfSqlExpr(tgt_var: Option[VarT], materialize_query: Option[(Aggregation, CalcExpr) => CalcExpr],
                    sources: Option[Relation], expr: Expression): CalcExpr = { // expr was SQL node

    def rcr_e(e: Expression, is_agg: Option[Boolean] = Some(false)): CalcExpr = {
      if (is_agg.isDefined && is_agg.get) {
        calcOfSqlExpr(None, None, sources, e)
      } else {
        calcOfSqlExpr(None, materialize_query, sources, e)
      }
    }

    def extendSumWithAgg(calc_expr: CalcExpr): CalcExpr = {

      materialize_query match {
        case Some(m) =>
          val count_agg = m(CountAll(), CalcValue(ArithConst(IntLiteral(1)))) // expr will never use
          val count_sch = schemaOfExpression(count_agg)._2
          CalcProd(List(mkAggsum(count_sch, CalcAST.Exists(count_agg)), calc_expr))
      }
    }

    def casesAsList(caseExp: Case): List[(Expression, Expression)] = {
      caseExp.elsep match {
        case c: Case => List((caseExp.cond, caseExp.thenp)) ++ casesAsList(c)
        case _       => List((caseExp.cond, caseExp.thenp))
      }
    }

    val (calc, contains_target) = expr match {

      case l: LiteralExpression =>
        (makeCalcExpr(ArithConst(l)), false)

      case f: FieldIdent =>
        println(f)
        println(f.name)
        (makeCalcExpr(makeArithExpr(varOfSqlVar(f.qualifier.get, f.name, IntType))), false) //TODO type expr

      case b: BinaryOperator =>
        b match {
          case m: Multiply =>
            (CalcProd(List(rcr_e(m.left), rcr_e(m.right))), false)

          case a @ (_: Add | _: Subtract) =>
            val (e1_calc, e2_base_calc) = (is_agg_expr(a.left), is_agg_expr(a.right)) match {
              case (true, true) | (false, false) => (rcr_e(a.left), rcr_e(a.right))
              case (false, true)                 => (extendSumWithAgg(rcr_e(a.left)), rcr_e(a.right))
              case (true, false)                 => (rcr_e(a.left), extendSumWithAgg(rcr_e(a.right)))
            }
            val e2_calc = if (a.isInstanceOf[Subtract]) CalcNeg(e2_base_calc) else e2_base_calc
            (CalcSum(List(e1_calc, e2_calc)), false)

          case d: Divide =>
            val e1 = d.left
            val e2 = d.right
            val ce1 = rcr_e(e1)
            val ce2 = rcr_e(e2)
            val (e2_val, e2_calc) = liftIfNecessary(ce2)
            val needs_order_flip = !is_agg_expr(e1) && is_agg_expr(e2)
            val nestedSchema = schemaOfExpression(ce1)._2.union(schemaOfExpression(ce2)._2)
            (mkAggsum(nestedSchema, CalcProd(if (needs_order_flip) List(CalcProd(List(e2_calc, ce1)))
            else List(CalcProd(List(ce1, e2_calc)))
              ++ List(CalcValue(ArithFunc("/", List(e2_val), FloatType))))), false)
        }

      case a: Aggregation =>
        a match {
          case s: Sum =>
            val agg_expr = s.expr
            (materialize_query match {
              case Some(mq) => mq(a, rcr_e(agg_expr, Some(true)))
              //case None =>  //TODO failwith
            }, false)
          case a: Avg =>
            val agg_expr = a.expr
            (materialize_query match {
              case Some(mq) => mq(a, rcr_e(agg_expr, Some(true)))
              //case None =>  //TODO failwith
            }, false)
          case CountExpr(Distinct(StarExpression(c))) =>
            (materialize_query match {
              case Some(mq) => mq(a, rcr_e(IntLiteral(1), Some(true)))
              //case None =>  //TODO failwith
            }, false)
          case CountExpr(Distinct(d)) =>
            (materialize_query match {
              case Some(mq) => mq(a, rcr_e(d, Some(true)))
              //case None =>  //TODO failwith
            }, false)
          case c: CountExpr =>
            val agg_expr = c.expr
            (materialize_query match {
              case Some(mq) => mq(a, rcr_e(agg_expr, Some(true)))
              //case None =>  //TODO failwith
            }, false)
          case _: CountAll =>
            (materialize_query match {
              case Some(mq) => mq(a, rcr_e(IntLiteral(1), Some(true)))
              //case None =>  //TODO failwith
            }, false)
        }

      case s: SelectStatement =>
        //val q = s.subquery.asInstanceOf[SelectStatement]
        val q = s
        //TODO sql.error failwith
        if (isAggQuery(q))
          (calcOfQuery(None, q).head._2, false)
        else
          (CalcProd(List(calcOfCondition(sources, q.where),
            rcr_e(q.projections.extractExpretions().head._1))), false)

      case ExternalFunctionExp(fn, fargs) =>

        val (lifted_args_and_gb_vars, arg_calc) = fargs.map({ arg =>
          val raw_arg_calc = rcr_e(arg)
          val (arg_val, arg_calc_local) = liftIfNecessary(raw_arg_calc)
          ((arg_val, schemaOfExpression(raw_arg_calc)._2), (is_agg_expr(arg), arg_calc_local))
        }).unzip
        val (lifted_args, gb_vars) = lifted_args_and_gb_vars.unzip
        val (agg_args, non_agg_args) = arg_calc.partition(x => x._1)

        val agg_res =
          tgt_var match {
            case None    => List()
            case Some(t) => List(t)
          }

        val args_list = {
          val args_expr = CalcProd((agg_args ++ non_agg_args).map(x => x._2))
          //          if ( args_expr.equals( CalcValue(ArithConst(IntLiteral(1))) ) )
          if (args_expr.equals(CalcProd(List(CalcValue(ArithConst(IntLiteral(1))))))) //TODO check equality with 1
            List()
          else
            prodList(args_expr)
        }

        val FunctionDeclaration(impl_name, imple_type) = getFunctionDeclaration(schema)(fn, lifted_args.map(x => IntType))

        (mkAggsum(agg_res ++ gb_vars.flatten, CalcProd(args_list ++ List({
          val calc = CalcValue(ArithFunc(impl_name, lifted_args, imple_type))
          if (agg_res == List())
            calc
          else
            Lift(agg_res.head, calc)
        }))), agg_res != List())

      case c: Case =>
        val cases = casesAsList(c)
        val (ret_calc, else_cond) = cases.foldLeft(CalcValue(ArithConst(IntLiteral(0))).asInstanceOf[CalcExpr], BoolLiteral(true).asInstanceOf[Expression])({ (a, b) =>
          val (ret_calc, prev_cond) = a
          val (curr_cond, curr_term) = b
          val full_cond = sqlMakeAnd(prev_cond, curr_cond)

          (CalcSum(List(ret_calc, CalcProd(List(calcOfCondition(sources, Some(full_cond)), rcr_e(curr_term))))),
            sqlMakeAnd(prev_cond, Not(curr_cond)))
        })
        (CalcSum(List(ret_calc, CalcProd(List(calcOfCondition(sources, Some(else_cond)), rcr_e(c.elsep))))), false)
    }

    (tgt_var, contains_target) match {
      case (Some(v), false) => Lift(v, calc)
      case _                => calc
    }
  }

  def castQueryToAggregate(query: TopLevelStatement): TopLevelStatement = {

    if (isAggQuery(query)) {
      query
    } else {
      query match {
        case s: SelectStatement =>
          val targets = s.projections.extractAliases().toList.map(x => (x._1, Some(x._2)))
          val sources = s.joinTree

          val cnt = {
            if (targets.exists(x => x._1.isInstanceOf[Distinct]))
              CountExpr(Distinct(StarExpression(None)))
            else
              CountAll()
          }

          val tgt = targets.map({ x =>
            (x._1 match {
              case d: Distinct =>
                d.e
              case y => y
            }, x._2)
          })

          val lst = tgt ++ List((cnt, Some("COUNT")))
          val new_targets = ExpressionProjections(lst) // TODO distinct is Ocaml?

          val new_gb_vars = GroupBy(targets.map({
            case (target_expr, target_name) =>

              val target_source = target_expr match {
                case f: FieldIdent => if (f.name == target_name.get) f.qualifier else None
                case _             => None
              }
              FieldIdent(target_source, target_name.get, sqlExprType(None, target_expr, sources))
          }))
          SelectStatement(s.withs, new_targets, s.joinTree, s.where, Some(new_gb_vars),
            s.having, s.orderBy, s.limit, s.aliases)

        case u: UnionIntersectSequence =>
          def rcr(stmt: TopLevelStatement): TopLevelStatement = {
            castQueryToAggregate(stmt)
          }
          UnionIntersectSequence(rcr(u.bottom), rcr(u.top), UNION)
      }
    }
  }

  def lowerIfValue(calc: CalcExpr): (ArithExpr, CalcExpr) = {
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

  def varOfSqlVar(R_name: String, col_name: String, tp: Tpe): VarT = {
    VarT(R_name + "_" + col_name, tp)
  }

  def liftIfNecessary(calc: CalcExpr, t: Option[String] = Some("agg"), vt: Option[Tpe] = Some(AnyType)): (ArithExpr, CalcExpr) = {

    combineValues(None, None, calc) match {
      case c: CalcValue => (c.v, CalcValue(ArithConst(IntLiteral(1))))
      case _ =>
        println("^^^^^^^^^^^^^")
        println(t)
        println(vt)
        val v = tmpVar(t.get, escalateType(None, vt.get, typeOfExpr(calc)))
        (makeArithExpr(v), Lift(v, calc))
    }
  }

  var varId = 0
  def tmpVar(vn: String, vt: Tpe): VarT = {
    varId += 1
    val v = VarT("__sql_inline_" + vn + varId, vt)
    v
  }

  var domId = 0
  def mkDomVar(l: List[Unit], vt: Tpe): VarT = {
    // TODO l ?
    domId += 1
    val v = VarT("__domain_" + domId, vt)
    v
  }

  def findTable(rel_name: String): Option[Table] = {
    schema.tables.find(x => x.name == rel_name)
  }

  // Arithmetic.mk_var
  def makeArithExpr(v: VarT): ArithExpr = {
    ArithVar(v)
  }

  // C.make_value
  def makeCalcExpr(v: ArithExpr): CalcExpr = {
    CalcValue(v)
  }

  def normalizeAggTargetExpr(vars_bound: Option[Boolean], gb_vars: List[FieldIdent], expr: Expression): Expression = {
    expr
    //TODO
  }

  // ********SQL********* :

  def isAggQuery(stmt: TopLevelStatement): Boolean = {

    stmt match {
      case s: SelectStatement =>
        s.projections.extractExpretions().map(x => x._1).toList.exists(x => is_agg_expr(x))
      case u: UnionIntersectSequence => isAggQuery(u.bottom) && isAggQuery(u.top)
    }
  }

  def rewriteHavingQuery(query: TopLevelStatement): TopLevelStatement = {
    query
    // TODO
  }

  def sqlExprType(strict: Option[Boolean], expr: Expression, sources: Option[Relation]): Symbol = {
    'INTEGER
    // TODO
  }

  def pushDownNots(cond: Option[Expression]): Option[Expression] = {

    cond match {
      case (Some(Not(Not(c))))                => Some(c)
      case (Some(Not(e: EqualityOperator)))   => Some(Inverse(e))
      case (Some(Not(e: InEqualityOperator))) => Some(Inverse(e))
      case (Some(Not(And(c1, c2))))           => pushDownNots(Some(Or(Not(c1), Not(c2))))
      case (Some(Not(Or(c1, c2))))            => pushDownNots(Some(And(Not(c1), Not(c2))))
      case (Some(Not(BoolLiteral(c))))        => Some(BoolLiteral(!c))
      case (Some(And(c1, c2)))                => Some(And(pushDownNots(Some(c1)).get, pushDownNots(Some(c2)).get))
      case (Some(Or(c1, c2)))                 => Some(Or(pushDownNots(Some(c1)).get, pushDownNots(Some(c2)).get))
      case s                                  => s
    }
  }

  def sqlMakeAnd(lhs: Expression, rhs: Expression): Expression = {
    lhs match {
      case BoolLiteral(true)  => rhs
      case BoolLiteral(false) => BoolLiteral(false)
      case _ =>
        rhs match {
          case BoolLiteral(true)  => lhs
          case BoolLiteral(false) => BoolLiteral(false)
          case _                  => And(lhs, rhs)
        }
    }
  }

  // ********Calculus********** :

  def mkAggsum(gb_vars: List[VarT], expr: CalcExpr): CalcExpr = {
    val expr_ovars = schemaOfExpression(expr)._2
    val new_gb_vars = expr_ovars.intersect(gb_vars)

    if (expr_ovars.equals(new_gb_vars))
      expr
    else
      AggSum(new_gb_vars, expr)
  }

  def typeOfExpr(expr: CalcExpr): Tpe = {
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
            Some((scope ++ prev.map(x => schemaOfExpression(x)._2)).foldLeft(List[VarT]())((a, b) => a.union(b))),
            Some((schema ++ next.map({ x =>
              val (xin, xout) = schemaOfExpression(x)
              xin.union(xout)
            })).foldLeft(List[VarT]())((a, b) => a.union(b))),
            curr)
        }
        prodFn(Schema_t(scope, schema), scanMap(fun, terms))

      case n: CalcNeg =>
        val term = n.expr
        negFn(Schema_t(scope, schema), rcr(scope, schema, term))

      case _ =>
        leafFn(Schema_t(scope, schema), e)
    }
  }

  // ********Type********** :
  def escalateType(opname: Option[String] = Some("<op>"), a: Tpe, b: Tpe): Tpe = {

    (a, b) match {
      case (at, bt) if at.equals(bt) => at
      case (AnyType, t)              => t
      case (t, AnyType)              => t
      // TODO other cases
    }
  }

  // ********CalculusTransforms********** :
  def combineValues(aggressive: Option[Boolean], peer_group: Option[List[Unit]], big_expr: CalcExpr): CalcExpr = {
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
          case AggSum(gb_vars, subexp) => mkAggsum(gb_vars, maintain(subexp))
          case CalcAST.Exists(subexp)  => mkExists(subexp)
          case x                       => x //TODO CalcValue(Lift) ?
        }, formula)
  }

  def mkExists(expr: CalcExpr): CalcExpr = {
    val dom_expr = maintain(expr)
    //    println("**********************************")
    //    println(prettyprint(expr))
    //    println(prettyprint(dom_expr))
    CalcAST.Exists(dom_expr)
  }

  def mkNotExists(expr: CalcExpr): CalcExpr = {
    val dom_expr = maintain(expr)
    val dom_var = mkDomVar(List(), IntType)
    val ovars = schemaOfExpression(expr)._2
    mkAggsum(ovars, CalcProd(List(Lift(dom_var, dom_expr), Cmp(Eq, ArithConst(IntLiteral(0)), ArithVar(dom_var)))))
  }

  def mkDomainRestrictedLift(lift_v: VarT, lift_expr: CalcExpr): CalcExpr = {
    val lift = Lift(lift_v, lift_expr)
    val ovars = schemaOfExpression(lift_expr)._2
    if (ovars.isEmpty) lift
    else CalcProd(List(mkExists(lift_expr), lift))
  }

  //   ********Ring********** :

  def prodList(e: CalcExpr): List[CalcExpr] = {
    e match {
      case CalcProd(l) => l
      case _           => List(e)
    }
  }

  //   ********List********** :

  def scanMap[A, B](f: (List[A], A, List[A]) => B, l: List[A]): List[B] = {
    def iterate(prev: List[A], curr_next: List[A]): List[B] = {
      curr_next match {
        case List()       => List()
        case curr :: next => List(f(prev, curr, next)) ++ iterate(prev ++ List(curr), next)
      }
    }
    iterate(List(), l)
  }

  //   ********Function********** :

  def declaration(fn: String, argtypes: List[Tpe]): FunctionDeclaration = {
    println(fn)
    schema.functions(fn.toUpperCase()).apply(argtypes)
  }

  def declareStdFunction(name: String, //fn: (List[LiteralExpression],Tpe) => LiteralExpression ,
                         typing_rule: List[Tpe] => Tpe): Unit = {
    schema.functions += name.toUpperCase() -> (l => FunctionDeclaration(name.toUpperCase(), typing_rule(l)))
  }
}