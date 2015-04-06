package ch.epfl.data
package legobase
package utils
package parser

import scala.util.parsing.combinator.RegexParsers

/**
 * A simple SQL parser. 
 * Based on: https://github.com/stephentu/scala-sql-parser
 */
object Parser extends StandardTokenParsers {

    def parse(statement : String): Option[SelectStmt] = {
    phrase(parseSelectStatement)(new lexical.Scanner(sql)) match {
      case Success(r, q) => Option(r)
      case _ => None
    }
  }

    def parseSelectStatement: Parser[SelectStatement] = (
        "SELECT" ~> parseProjections ~ "FROM" ~ parseRelations ~ parseWhere.? ~ parseGroupBy.? ~ parseOrderBy.? <~ ";".? ^^ { case pro ~ _ ~ tab ~ whe ~ grp ~ ord => SelectStatement(pro, tab, whe, grp, ord) }
    )

    def parseProjections: Parser[Projections] = (
        "*" ^^^ AllColumns()
        | rep1sep(parseAliasedExpression, ",") ^^ { case lst => ExpressionProjections(lst) }
    )

    def parseAliasedExpression: Parser[(Expression, Option[String])] = (
        parseExpression ~ ("AS" ~> ident).? ^^ { case expr ~ alias => (expr, alias)}
    )

    def parseExpression: Parser[Expression] = parseOr

    def parseOr: Parser[Expression] =
        parseAnd * ( "OR" ^^^ { (a: Expression, b: Expression) => Or(a, b) } )

    def parseAnd: Parser[Expression] =
        parseSimpleExpression * ( "AND" ^^^ { (a: Expression, b: Expression) => And(a, b) } )

    def parseSimpleExpression: Parser[Expression] = (
        parseAddition ~ rep(
          ("=" | "<>" | "!=" | "<" | "<=" | ">" | ">=") ~ parseAddition ^^ {
            case op ~ right => (op, right)
          }
          | "BETWEEN" ~ parseAddition ~ "AND" ~ parseAddition ^^ {
            case op ~ a ~ _ ~ b => (op, a, b)
          }
          | "NOT IN" ~ "(" ~ (parseSelectStatement | rep1sep(parseExpression, ",")) ~ ")" ^^ {
            case op ~ _ ~ a ~ _ => ("IN", a, True)
          }
          | "IN" ~ "(" ~ (parseSelectStatement | rep1sep(parseExpression, ",")) ~ ")" ^^ {
            case op ~ _ ~ a ~ _ => (op, a, False)
          }
          | "NOT LIKE" ~ parseAddition ^^ { case op ~ a => ("LIKE", a, True) }
          | "LIKE" ~ parseAddition ^^ { case op ~ a => (op, a, False) }
        ) ^^ {
          case left ~ elems =>
            elems.foldLeft(left) {
              case (acc, (("=", right: Expression))) => Equals(acc, right)
              case (acc, (("<>", right: Expression))) => NotEquals(acc, right)
              case (acc, (("!=", right: Expression))) => NotEquals(acc, right)
              case (acc, (("<", right: Expression))) => LessThan(acc, right)
              case (acc, (("<=", right: Expression))) => LessOrEqual(acc, right)
              case (acc, ((">", right: Expression))) => GreaterThan(acc, right)
              case (acc, ((">=", right: Expression))) => GreaterOrEqual(acc, right)
              case (acc, (("BETWEEN", l: Expression, r: Expression))) => And(GreaterOrEqual(acc, l), LessOrEqual(acc, r))
              case (acc, (("IN", e: Seq[_], n: Boolean))) => In(acc, e.asInstanceOf[Seq[Expression]], n)
              case (acc, (("IN", s: SelectStatement, n: Boolean))) => In(acc, Seq(s), n)
              case (acc, (("LIKE", e: Expression, n: Boolean))) => Like(acc, e, n)
            }
        } |
        "NOT" ~> parseSimpleExpression ^^ (Not(_)) |
        "EXISTS" ~> "(" ~> parseSelectStatement <~ ")" ^^ { case s => Exists(s) }
    )

    def parseAddition: Parser[Expression] =
        parseMultiplication * (
          "+" ^^^ { (a: Expression, b: Expression) => Add(a, b) } |
          "-" ^^^ { (a: Expression, b: Expression) => Subtract(a, b) } )

    def mult_expr: Parser[Expression] =
        primary_expr * (
          "*" ^^^ { (a: Expression, b: Expression) => Multiply(a, b) } |
          "/" ^^^ { (a: Expression, b: Expression) => Divide(a, b) } )

    def parsePrimaryExpression: Parser[SqlExpr] = (
        parseLiteral |
        parseKnownFunction |
        ident ~ opt( "." ~> ident  | "(" ~> repsep(parseExpression, ",") <~ ")" ) ^^ {
          case id ~ None => FieldIdent(None, id)
          case a ~ Some( b: String ) => FieldIdent(Some(a), b)
        } |
        "(" ~> (parseExpression | parseSelectStatement ^^ (_)) <~ ")" |
        "+" ~> parsePrimaryExpression ^^ (UnaryPlus(_)) |
        "-" ~> parsePrimaryExpression ^^ (UnaryMinus(_)) |
    )

    def parseKnownFunction: Parser[Expression] = (
        "COUNT(" ~> ( "*" ^^^ CountAll() | parseExpression ^^ { case expr => CountExpr(e) }) <~ ")"
        | "MIN(" ~> parseExpression <~ ")" ^^ (Min(_))
        | "MAX(" ~> parseExpression <~ ")" ^^ (Max(_))
        | "SUM(" ~>  parseExpression <~ ")" ^^ { Sum(_) }
        | "AVG(" ~> parseExpression <~ ")" ^^ { Avg(_) }
    )

    // TODO
    def parseLiteral: Parser[Expression] = (
        numericLit ^^ { case i => IntLiteral(i.toInt) } |
        floatLit ^^ { case f => FloatLiteral(f.toDouble) } |
        stringLit ^^ { case s => StringLiteral(s) } |
        "null" ^^ (_ => NullLiteral()) |
        "date" ~> stringLit ^^ (DateLiteral(_)) |
          case d ~ u => IntervalLiteral(d, u)
        }
    )

    def parseRelations: Parser[Seq[Relation]] = rep1sep(parseRelation, ",")

    def parseRelation: Parser[Relation] = (
        parseSimpleRelation ~ rep(opt(parseJoinType) ~ "JOIN" ~ parseSimpleRelation ~ "ON" ~ parseExpression ^^
      { case tpe ~ _ ~ r ~ _ ~ e => (tpe.getOrElse(InnerJoin), r, e)}) ^^ {
      case r ~ elems => elems.foldLeft(r) { case (x, r) => Join(x, r._2, r._1, r._3) }
        }
    )

    def parseSimpleRelation: Parser[SqlRelation] =
        ident ~ ("AS".? ~> ident.?) ^^ {
          case tbl ~ alias => Table(tbl, alias)
        } 
        | ("(" ~> parseSelectStatement <~ ")") ~ ("AS".? ~> ident) ^^ {
        case subq ~ alias => Subquery(subq, alias)
    }

    def parseJoinType: Parser[JoinType] = (
        "LEFT" <~ "OUTER".? | "RIGHT" <~ "OUTER".? | "FULL OUTER") ^^ {
          case "LEFT" => LeftOuterJoin()
          case "RIGHT" => RightOuterJoin()
          case "FULL OUTER" => FullOuterJoin()
        }
        | "INNER" ^^^ InnerJoin()
    )


    def parseWhere: Parser[Expression] = (
        "WHERE" ~> expr
    )

    def parseGroupBy: Parser[GroupBy] = (
        "GROUP BY" rep1sep(parseExpression, ",") ~ ("HAVING" ~> parseExpression).? ^^
        { case exp ~ hav => GroupBy(exp, hav) }
    )
    def parseOrderBy: Parser[OrderClause] = (
        "ORDER BY" ~> rep1sep(parseOrderKey ~ , ",") ^^ { case keys => OrderBy(keys) }
    )

    def parseOrderKey: Parser[(Expression, OrderType)] = (
        parseExpression <~ "ASC".? ^^ { case v => (v, ASC) }
        | parseExpression <~ "DESC" ^^ { case v => (v, DESC) }
    )
