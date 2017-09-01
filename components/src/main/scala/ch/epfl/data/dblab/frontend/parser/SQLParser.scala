package ch.epfl.data
package dblab
package frontend
package parser
import scala.util.matching.Regex
import scala.util.parsing.combinator.RegexParsers
import scala.util.parsing.combinator._
import scala.util.parsing.combinator.lexical._
import scala.util.parsing.combinator.syntactical._
import scala.util.parsing.combinator.token._
import scala.util.parsing.input.CharArrayReader.EofCh
import SQLAST._
import ch.epfl.data.dblab.frontend.optimizer.{ SQLToCalc, SQLUtils }
import SQLUtils._
import ch.epfl.data.dblab.frontend.parser.SQLParser.ident

/**
 * A simple SQL parser.
 * Based on: https://github.com/stephentu/scala-sql-parser but heavily modified for our needs
 *
 * @author Yannis Klonatos
 * @author Florian Chlan
 * @author Parand Alizadeh
 * @author Mohsen Ferdosi
 */
object SQLParser extends StandardTokenParsers {

  def parse(statement: String): TopLevelStatement = {
    phrase(parseQuery)(new lexical.Scanner(statement)) match {
      case Success(r, q) => r
      case failure       => throw new Exception("Unable to parse SQL query!\n" + failure)
    }
  }

  def parseStream(statement: String): TopLevelStatement = {
    phrase(parseStreamQuery)(new lexical.Scanner(statement)) match {
      case Success(r, q) => r
      case failure       => throw new Exception("Unable to parse SQL query!\n" + failure)
    }
  }

  def parseQuery: Parser[TopLevelStatement] =
    parseSelectStatement ~ ((("UNION" ~ "ALL".?) | "INTERSECT" | "EXCEPT" | ";") ~ parseQuery.?).? ^^ {
      case stmt ~ None => stmt
      case stmt ~ Some(bottomQuery) => bottomQuery match {
        case "UNION" ~ Some("ALL") ~ query => UnionIntersectSequence(stmt, query.get, UNIONALL)
        case "UNION" ~ None ~ query        => UnionIntersectSequence(stmt, query.get, UNION)
        case "INTERSECT" ~ query           => UnionIntersectSequence(stmt, query.get, INTERSECT)
        case "EXCEPT" ~ query              => UnionIntersectSequence(stmt, query.get, EXCEPT)
        case ";" ~ query => query match {
          case Some(q) => UnionIntersectSequence(stmt, q, SEQUENCE)
          case None    => stmt
        }
      }
    }

  def parseStreamQuery: Parser[TopLevelStatement] =
    parseIncludeStatement.? ~ parseCreate.? ~ parseQuery ^^ {
      case incOpt ~ streamsOpt ~ body => IncludeStatement("", incOpt.getOrElse(Seq()) ++ streamsOpt.getOrElse(Seq()), body)
    }
  def parseIncludeStatement: Parser[List[CreateStatement]] =
    "INCLUDE" ~ stringLit <~ ";".? ^^ {
      case inc ~ lit => phrase(parseCreate)(new lexical.Scanner(scala.io.Source.fromFile(lit).mkString)) match {
        case Success(r, q) => r
        case failure       => throw new Exception("Unable to parse the include file SQL query!\n" + failure)
      }
    }

  def parseCreate: Parser[List[CreateStatement]] =
    rep(parseCreateStreamOrTable | parseCreateFunction) ^^ {
      case cs => cs
    }

  def parseCreateFunction: Parser[CreateFunction] =
    "CREATE" ~> "FUNCTION" ~> ident ~ "(" ~ parseStreamColumns ~ ")" ~ "RETURNS" ~ parseStreamDataType ~ "AS" ~ "EXTERNAL" ~ stringLit <~ ";" ^^ {
      case name ~ _ ~ cols ~ _ ~ _ ~ ret ~ _ ~ _ ~ ext => CreateFunction(name, cols, ret, ext)
    }

  def parseCreateStreamOrTable: Parser[CreateStream] =
    "CREATE" ~> ("STREAM" | "TABLE") ~ ident ~ "(" ~ parseStreamColumns.? ~ ")" ~ ("FROM" ~> parseSrcStatement).? <~ ";" ^^ {
      case sot ~ nameStr ~ _ ~ colsOpt ~ _ ~ restOpt => CreateStream(sot, nameStr.toUpperCase, colsOpt.getOrElse(Seq()), restOpt.getOrElse(""))

    }

  def parseSrcStatement: Parser[String] =
    //TODO socket
    "FILE" ~ stringLit ~ parseByteParams ^^ {
      case _ ~ s ~ param => s + param
    }
  def parseByteParams: Parser[String] =
    parseFramingStatement ~ parseAdaptorStatement ^^ {
      case f ~ a => f + a
    }

  def parseFramingStatement: Parser[String] = (
    "FIXEDWIDTH" ~ numericLit ^^ { case _ ~ num => "FIXEDWIDTH " + num }
    | "LINE" ~ "DELIMITED" ^^ { case l ~ d => l + d }
    | stringLit ~ "DELIMITED" ^^ { case s ~ d => s + d })

  def parseAdaptorStatement: Parser[String] =
    ident ~ ("(" ~> parseAdaptorParams.? <~ ")").? ^^ {
      case id ~ param => id + param
    }
  def parseAdaptorParams: Parser[String] = (
    ident ~ ":=" ~ stringLit ^^ {
      case id ~ sv ~ str => id + sv + str
    }) | (stringLit ^^ {
      case s => s
    })
  def parseStreamColumns: Parser[Seq[(String, String)]] =
    ident ~ (parseStreamDataType) ~ ("," ~> parseStreamColumns).? ^^ {
      case col ~ colType ~ Some(cols) => (col.toUpperCase, colType) +: cols
      case col ~ colType ~ None       => Seq((col.toUpperCase(), colType))
    }
  // TODO change it to SC type or DDL type
  def parseStreamDataType: Parser[String] =
    "DECIMAL" ~ ("(" ~ parseLiteral ~ "," ~ parseLiteral ~ ")").? ^^^ { "DECIMAL" } |
      "NUMERIC" ~ "(" ~ parseLiteral ~ "," ~ parseLiteral ~ ")" ^^^ { "NUMERIC" } |
      "INT" ^^^ {
        "INTEGER"
      } |
      "DATE" ^^^ {
        "DATE"
      } |
      "STRING" ^^^ {
        "STRING"
      } |
      "FLOAT" ^^^ {
        "FLOAT"
      } | "CHAR" ~ ("(" ~ numericLit ~ ")").? ^^^ {
        "CHAR"
      } | "VARCHAR" ~ "(" ~ numericLit ~ ")" ^^^ {
        "VARCHAR"
      }
  /** ********************************************************/
  /* Parse parts of individual select statements in a query */
  /** ********************************************************/
  def parseSelectStatement: Parser[SelectStatement] =
    parseAllCTEs ~ "SELECT" ~ parseProjections ~ "FROM".? ~ parseFrom.? ~ parseWhere.? ~ parseGroupBy.? ~ parseHaving.? ~ parseOrderBy.? ~ parseLimit.? ^^ {
      case withs ~ _ ~ pro ~ _ ~ Some(tab) ~ whe ~ grp ~ hav ~ ord ~ lim => {
        val aliases = pro.extractAliases ++ withs.map(w => w.subquery.extractAliases).flatten ++ tab.extractSubqueries.map(sq => sq.subquery.extractAliases).flatten
        SelectStatement(withs, pro, Some(tab), whe, grp, hav, ord, lim, aliases)
      }
      case withs ~ _ ~ pro ~ _ ~ None ~ whe ~ grp ~ hav ~ ord ~ lim => {
        val aliases = pro.extractAliases ++ withs.map(w => w.subquery.extractAliases).flatten
        SelectStatement(withs, pro, None, whe, grp, hav, ord, lim, aliases)
      }

    }

  def parseAllCTEs: Parser[List[View]] =
    opt("WITH" ~> rep1sep(parseCTE, ",")) ^^ {
      case Some(ctes) => ctes
      case None       => List[View]()
    }

  def parseCTE: Parser[View] =
    ident ~ "AS" ~ "(" ~ parseQuery <~ ")" ^^ { case name ~ _ ~ _ ~ stmt => View(stmt, name) }

  def parseProjections: Parser[Projections] = (
    rep1sep(parseAliasedExpression, ",") ^^ { case lst => ExpressionProjections(lst) })

  def parseAliasedExpression: Parser[(Expression, Option[String])] =
    parseExpression ~ parseAlias.? ^^ { case expr ~ alias => (expr, alias) }

  def parseAlias: Parser[String] =
    ("AS".? ~> ident) ^^ { case ident => ident.toUpperCase() } |
      ("AS".? ~> stringLit) ^^ { case lit => lit }

  def parseFrom: Parser[Relation] =
    parseSingleRelation ~ parseOtherRelations ^^ {
      case rel ~ elems => elems.foldLeft(rel) { case (x, r) => Join(x, r._2, r._1, r._3) }
    }

  def parseOtherRelations: Parser[Seq[Tuple3[JoinType, Relation, Expression]]] =
    rep(opt(parseJoinType) ~ "JOIN" ~ parseSingleRelation ~ "ON" ~ parseExpression ^^ {
      case tpe ~ _ ~ r ~ _ ~ e => (tpe.getOrElse(InnerJoin), r, e)
    }
      | "," ~> parseSingleRelation ^^ {
        case r => (InnerJoin, r, Equals(IntLiteral(1), IntLiteral(1)))
      }
      | "NATURAL" ~> "JOIN" ~> parseSingleRelation ^^ {
        case r => (NaturalJoin, r, null)
      })

  def parseSingleRelation: Parser[Relation] = (
    ident ~ parseAlias.? ^^ {
      case tbl ~ alias => SQLTable(tbl.toUpperCase(), alias)
    }
    | ("(" ~> parseQuery <~ ")") ~ parseAlias ^^ {
      case subq ~ alias => Subquery(subq, alias)
    }
    | parseQuery ^^ { case q => Subquery(q, null) })

  def parseJoinType: Parser[JoinType] = (
    "LEFT" ~ ("OUTER" | "SEMI").? ^^ {
      case l ~ r => (l, r) match {
        case (l, None)          => LeftOuterJoin
        case (l, Some("SEMI"))  => LeftSemiJoin
        case (l, Some("OUTER")) => LeftOuterJoin
      }
    }
    | "RIGHT" ~ "OUTER".? ^^^ RightOuterJoin
    | "FULL" ~ "OUTER" ^^^ FullOuterJoin
    | "ANTI" ^^^ AntiJoin
    | "INNER" ^^^ InnerJoin)

  def parseWhere: Parser[Expression] =
    "WHERE" ~> parseExpression

  def parseGroupBy: Parser[GroupBy] =
    "GROUP" ~> "BY" ~> rep1sep(parseExpression, ",") ^^ { case exp => GroupBy(exp) }

  def parseHaving: Parser[Having] =
    "HAVING" ~> parseExpression ^^ { case exp => Having(exp) }

  def parseOrderBy: Parser[OrderBy] =
    "ORDER" ~> "BY" ~> rep1sep(parseOrderKey, ",") ^^ { case keys => OrderBy(keys) }

  def parseOrderKey: Parser[(Expression, OrderType)] =
    parseExpression ~ ("ASC" | "DESC").? ^^ {
      case v ~ Some("DESC") => (v, DESC)
      case v ~ Some("ASC")  => (v, ASC)
      case v ~ None         => (v, ASC)
    }

  def parseLimit: Parser[Limit] =
    "LIMIT" ~> numericLit ^^ { case lim => Limit(lim.toInt) }

  // ----------------------------------------------------------------------------------------------------------------------------------------------

  /** ********************/
  /* Expression parsing */
  /** ********************/
  def parseExpression: Parser[Expression] = parseOperandExpression * (
    "OR" ^^^ { (a: Expression, b: Expression) => Or(a, b) }
    | "AND" ^^^ { (a: Expression, b: Expression) => And(a, b) })

  def exprToSQLNode(acc: Expression, elem: Any): Expression = (acc, elem) match {
    case (acc, (("=", right: Expression)))                       => Equals(acc, right)
    case (acc, (("<>", right: Expression)))                      => NotEquals(acc, right)
    case (acc, (("!=", right: Expression)))                      => NotEquals(acc, right)
    case (acc, (("<", right: Expression)))                       => LessThan(acc, right)
    case (acc, (("<=", right: Expression)))                      => LessOrEqual(acc, right)
    case (acc, ((">", right: Expression)))                       => GreaterThan(acc, right)
    case (acc, ((">=", right: Expression)))                      => GreaterOrEqual(acc, right)
    case (acc, (("BETWEEN", l: Expression, r: Expression)))      => And(GreaterOrEqual(acc, l), LessOrEqual(acc, r))
    case (acc, (("IN", e: Seq[_])))                              => In(acc, e.asInstanceOf[Seq[Expression]])
    //    case (acc, (("IN", s: SelectStatement)))                    => In(acc, Seq(s)) //TODO generalize
    case (acc, (("LIKE", e: Expression)))                        => Like(acc, e)
    case (acc, (("NOT", e: Serializable)))                       => Not(exprToSQLNode(acc, e))
    case (acc, (("||", e: Expression)))                          => StringConcat(acc, e)
    case (acc, (("ISNOTNULL", null)))                            => Not(Equals(acc, NullLiteral))
    case (acc, (("ISNULL", null)))                               => Equals(acc, NullLiteral)
    case (acc, (("IN LIST", l: List[LiteralExpression])))        => InList(acc, l)
    case (acc, (("ALL", List(op: String, s: SelectStatement))))  => Not(scanForExistence(s, acc, op, true))
    case (acc, (("SOME", List(op: String, s: SelectStatement)))) => scanForExistence(s, acc, op, false)
    case (acc, (("IN", s: SelectStatement)))                     => scanForExistence(s, acc, "=", false)
  }

  def parseOperandExpression: Parser[Expression] =
    parseAddition ~ rep(parseOperation) ^^ {
      case leftOperator ~ expressions =>
        expressions.foldLeft(leftOperator)((acc, elem) => exprToSQLNode(acc, elem))
    }

  def parseCompareOperand: Parser[String] = "=" | "<>" | "!=" | "<" | "<=" | ">" | ">="

  def parseOperation: Parser[Product] = (
    (parseCompareOperand <~ "ALL" <~ "(") ~ (parseSelectStatement <~ ")") ^^ { case op ~ s => ("ALL", List(op, s)) }
    | (parseCompareOperand <~ "SOME" <~ "(") ~ (parseSelectStatement <~ ")") ^^ { case op ~ s => ("SOME", List(op, s)) }
    | parseCompareOperand ~ parseAddition ^^ { case op ~ right => (op, right) }
    | "BETWEEN" ~ parseAddition ~ "AND" ~ parseAddition ^^ {
      case op ~ a ~ _ ~ b => (op, a, b)
    }
    | "IN" ~ "(" ~ (parseSelectStatement | rep1sep(parseExpression, ",")) ~ ")" ^^ {
      case op ~ _ ~ a ~ _ => (op, a)
    }
    | "IN" ~> "LIST" ~> "(" ~> parseExpression ~ rep("," ~> parseExpression).? <~ ")" ^^ {
      case num ~ Some(l) => ("IN LIST", num :: l)
    }
    | "LIKE" ~ parseAddition ^^ { case op ~ a => (op, a) }
    | "NOT" ~ parseOperation ^^ { case op ~ a => (op, a) }
    | "||" ~ parseAddition ^^ { case op ~ a => (op, a) }
    | "IS" ~ "NOT" ~ "NULL" ^^ { case _ ~ _ ~ _ => ("ISNOTNULL", null) }
    | "IS" ~ "NULL" ^^ { case _ ~ _ => ("ISNULL", null) })

  def parseAddition: Parser[Expression] =
    parseMultiplication * (
      "+" ^^^ { (a: Expression, b: Expression) => Add(a, b) } |
      "-" ^^^ { (a: Expression, b: Expression) => Subtract(a, b) })

  def parseMultiplication: Parser[Expression] =
    parsePrimaryExpression * (
      "*" ^^^ { (a: Expression, b: Expression) => Multiply(a, b) } |
      "/" ^^^ { (a: Expression, b: Expression) => Divide(a, b) })

  def scanForExistence(q: TopLevelStatement, e: Expression, op: String, inverse: Boolean): Expression = {
    q match {
      case s: SelectStatement =>
        val target = s.projections match {
          case ExpressionProjections(List((e, _))) => e
          case _                                   => ???
        }

        def cmpFact(e1: Expression, e2: Expression): Expression = {
          val c = op match {
            case "="  => Equals(e1, e2)
            case "<>" => NotEquals(e1, e2)
            case "!=" => NotEquals(e1, e2)
            case "<"  => LessThan(e1, e2)
            case ">"  => GreaterThan(e1, e2)
            case "<=" => LessOrEqual(e1, e2)
            case ">=" => GreaterOrEqual(e1, e2)
          }
          if (inverse)
            Inverse(c)
          else
            c
        }

        if (is_agg_expr(target)) {
          cmpFact(e, s)
        } else {
          val cmp: Expression = cmpFact(e, target)
          Exists(SelectStatement(Seq(), ExpressionProjections(List((IntLiteral(1), Some("unused")))),
            s.joinTree, s.where.map(w => And(w, cmp)).fold(Some(cmp))(Some.apply _), s.groupBy, s.having, s.orderBy, s.limit, s.aliases))

        }

    }

  }

  def parsePrimaryExpression: Parser[Expression] = (
    parseLiteral
    | parseFunction ~ ("OVER" ~ "(" ~ "PARTITION" ~ "BY" ~ rep1sep(parsePrimaryExpression, ",") ~ parseOrderBy.? ~
      ("ROWS" ~ "BETWEEN" ~ rep(parsePrimaryExpression) ~ "AND" ~ rep(parsePrimaryExpression)).? ~ ")").? ^^ {
        case fun ~ _ => fun
      }
      | "*" ^^^ { StarExpression(None) }
      | ident ~ opt("." ~> (ident | "*") /*| ("(" ~> repsep(parseExpression, ",") <~ ")")*/ ) ^^ {
        case id ~ None      => FieldIdent(None, id.toUpperCase())
        case id ~ Some("*") => StarExpression(Some(id.toUpperCase()))
        case id ~ Some(b)   => FieldIdent(Some(id.toUpperCase()), b.toUpperCase())
      }
      | "(" ~> (parseExpression | parseSelectStatement) <~ ")"
      | "DISTINCT" ~> parseExpression ^^ { case e => Distinct(e) }
      | "CASE" ~> parseExpression.? ~ "WHEN" ~ parseExpression ~ "THEN" ~ parseExpression ~ "ELSE" ~ parseExpression <~ "END" ^^ {
        case variable ~ _ ~ cond ~ _ ~ thenp ~ _ ~ elsep => variable match {
          case Some(vr) => Case(Equals(vr, cond), thenp, elsep)
          case None     => Case(cond, thenp, elsep)
        }
      }
      | "+" ~> parsePrimaryExpression ^^ (UnaryPlus(_))
      | "-" ~> parsePrimaryExpression ^^ (UnaryMinus(_))
      // TODO: Casting and rounding operations are for the moment ignored.
      | "CAST" ~ "(" ~> parseExpression ~ "AS" ~ parseDataType <~ ")" ^^ {
        case expr ~ _ ~ format => format match {
          case "DATE" => DateLiteral(expr.toString.replaceAll("'", ""))
          case _      => expr
        }
      }
      | "ROUND" ~ "(" ~> parseExpression ~ "," ~ parseLiteral <~ ")" ^^ {
        case expr ~ _ ~ literal => expr
      }
      | "NOT".? ~ "EXISTS" ~ "(" ~ parseSelectStatement <~ ")" ^^ {
        case not ~ _ ~ _ ~ stmt => not match {
          case Some("NOT") => Not(Exists(stmt))
          case None        => Exists(stmt)
        }
      }
      | "NOT" ~> parseExpression ^^ {
        case exp => Not(exp)
      }
      | "ALL" ~> "(" ~> parseSelectStatement <~ ")" ^^ {
        case exp => AllExp(exp)
      }
      | "SOME" ~> "(" ~> parseSelectStatement <~ ")" ^^ {
        case exp => SomeExp(exp)
      }
      | "ANY" ~> "(" ~> parseSelectStatement <~ ")" ^^ {
        case exp => SomeExp(exp)
      }
      | "EXTRACT" ~> "(" ~> ident ~ "FROM" ~ ident ~ ("." ~ ident).? <~ ")" ^^ {
        case f ~ _ ~ s ~ Some(t) => ExtractExp(f, s + t)
        case f ~ _ ~ s ~ None    => ExtractExp(f, s)

      })

  def parseDataType: Parser[String] =
    "DECIMAL" ~ ("(" ~ parseLiteral ~ "," ~ parseLiteral ~ ")").? ^^^ {
      "DECIMAL"
    } |
      "NUMERIC" ~ "(" ~ parseLiteral ~ "," ~ parseLiteral ~ ")" ^^^ {
        "NUMERIC"
      } |
      "INT" | "INTEGER" ^^^ {
        "INTEGER"
      } |
      "DATE" ^^^ {
        "DATE"
      } |
      "FLOAT" ^^^ {
        "FLOAT"
      } |
      "STRING" ^^^ {
        "STRING"
      }

  def parseFunction: Parser[Expression] = (
    "COUNT" ~> "(" ~> ("*" ^^^ CountAll() | parseExpression ^^ { case expr => CountExpr(expr) }) <~ ")"
    | "MIN" ~> "(" ~> parseExpression <~ ")" ^^ (Min(_))
    | "MAX" ~> "(" ~> parseExpression <~ ")" ^^ (Max(_))
    | "SUM" ~> "(" ~> parseExpression <~ ")" ^^ (Sum(_))
    | "AVG" ~> "(" ~> parseExpression <~ ")" ^^ (Avg(_))
    | "YEAR" ~> "(" ~> parseExpression <~ ")" ^^ (Year(_))
    | "ABS" ~> "(" ~> parseExpression <~ ")" ^^ (Abs(_))
    | "UPPER" ~ "(" ~> parseExpression <~ ")" ^^ (Upper(_))
    | ("SUBSTRING" | "SUBSTR") ~> "(" ~> parseExpression ~ "," ~ parseExpression ~ "," ~ parseExpression <~ ")" ^^ {
      case str ~ _ ~ idx1 ~ _ ~ idx2 => Substring(str, idx1, idx2)
    }
    // TODO: Coalesce function is for the moment always returning the first argument
    | "COALESCE" ~ "(" ~> parseExpression ~ "," ~ parseExpression <~ ")" ^^ {
      case left ~ _ ~ right => left
    }
    | ident ~ "(" ~ parseExpression.? ~ (rep("," ~> parseExpression)).? <~ ")" ^^ {
      case name ~ _ ~ Some(inp) ~ Some(inps) => FunctionExp(name, inp :: inps)
      case name ~ _ ~ Some(inp) ~ None       => FunctionExp(name, List(inp))
      case name ~ _ ~ None ~ None            => FunctionExp(name, List())

    })

  def parseLiteral: Parser[Expression] = (
    numericLit ~ "DAYS".? ^^ {
      case i ~ date => date match {
        case Some(d) => IntLiteral(i.toInt)
        case None    => IntLiteral(i.toInt)
      }
    }
    | floatLit ^^ { case f => DoubleLiteral(f.toDouble) }
    | stringLit ^^ {
      case s => {
        if (s.length == 1) CharLiteral(s.charAt(0))
        else StringLiteral(s)
      }
    }
    | "NULL" ^^ { case _ => NullLiteral }
    | "DATE" ~> "(".? ~> stringLit <~ ")".? ^^ { case s => DateLiteral(s) }
    | ("INTERVAL" ~> stringLit ~ ident ^^ {
      case s ~ id => IntervalLiteral(s, id, None)
    })
    | ("INTERVAL" ~> stringLit ~ ident ~ "(" ~ numericLit <~ ")" ^^ {
      case s ~ id ~ _ ~ num => IntervalLiteral(s, id, Some(num.toInt))
    }))

  // ----------------------------------------------------------------------------------------------------------------------------------------------

  /** *****************************************/
  /* Lexical analytsis methods and variables */
  /** *****************************************/
  class SqlLexical extends StdLexical {

    case class FloatLit(chars: String) extends Token {
      override def toString = chars
    }

    override def processIdent(token: String) = {
      val tkn = {
        val str = token.toUpperCase
        if (tokens.contains(str) && str != "YEAR") str
        else token
      }
      super.processIdent(tkn)
    }

    override def whitespace: Parser[Any] = rep(
      whitespaceChar
        | '-' ~ '-' ~ rep(chrExcept(EofCh, '\n'))
        | '/' ~ '*' ~ comment)

    //    override protected def comment: Parser[Any] = (
    //      	      '*' ~ '/'  ^^ { case _ => ' '  }
    //    	    | chrExcept(EofCh) ~ comment
    //    	    )

    override def token: Parser[Token] =
      (identChar ~ rep(identChar | digit) ^^ {
        case first ~ rest => processIdent(first :: rest mkString "")
      }
        | rep1(digit) ~ opt('.' ~> rep(digit)) ^^ {
          case i ~ None    => NumericLit(i mkString "")
          case i ~ Some(d) => FloatLit(i.mkString("") + "." + d.mkString(""))
        }
        | '\'' ~ rep(chrExcept('\'', '\n', EofCh)) ~ '\'' ^^ { case '\'' ~ chars ~ '\'' => StringLit(chars mkString "") }
        | '\"' ~ rep(chrExcept('\"', '\n', EofCh)) ~ '\"' ^^ { case '\"' ~ chars ~ '\"' => StringLit(chars mkString "") }
        //        | "--" ~ rep(chrExcept('\n', EofCh)) ~ '\"' ^^ { case _ ~ chars ~ _ => comment }
        | EofCh ^^^ EOF
        | '\'' ~> failure("unclosed string literal")
        | '\"' ~> failure("unclosed string literal")
        | delim
        | failure("illegal character"))

    def regex(r: Regex): Parser[String] = new Parser[String] {
      def apply(in: Input) = {
        val source = in.source
        val offset = in.offset
        val start = offset // handleWhiteSpace(source, offset)
        (r findPrefixMatchOf (source.subSequence(start, source.length))) match {
          case Some(matched) =>
            Success(source.subSequence(start, start + matched.end).toString,
              in.drop(start + matched.end - offset))
          case None =>
            Success("", in)
        }
      }
    }
  }

  override val lexical = new SqlLexical

  def floatLit: Parser[String] =
    elem("decimal", _.isInstanceOf[lexical.FloatLit]) ^^ (_.chars)

  val tokens = List(
    "SELECT", "AS", "OR", "AND", "GROUP", "ORDER", "BY", "WHERE", "WITH", "JOIN",
    "ASC", "DESC", "FROM", "ON", "NOT", "HAVING", "EXISTS", "BETWEEN", "LIKE", "IN",
    "NULL", "LEFT", "RIGHT", "FULL", "OUTER", "SEMI", "INNER", "ANTI", "COUNT", "SUM",
    "AVG", "MIN", "MAX", "YEAR", "DATE", "TOP", "LIMIT", "CASE", "WHEN", "THEN", "ELSE",
    "END", "SUBSTRING", "SUBSTR", "UNION", "ALL", "CAST", "DECIMAL", "DISTINCT", "NUMERIC",
    "INT", "DAYS", "COALESCE", "ROUND", "OVER", "PARTITION", "BY", "ROWS", "INTERSECT",
    "UPPER", "IS", "ABS", "EXCEPT", "INCLUDE", "CREATE", "STREAM", "FILE", "DELIMITED",
    "FIXEDWIDTH", "INTERVAL", "LINE", "STRING", "FLOAT", "CHAR", "VARCHAR", "NATURAL",
    "SOME", "TABLE", "ANY", "EXTRACT", "LIST", "INTEGER", "FUNCTION", "EXTERNAL", "RETURNS")

  for (token <- tokens)
    lexical.reserved += token

  lexical.delimiters += (
    "*", "+", "-", "<", ":=", "=", "<>", "!=", "<=", ">=", ">", "||", "/", "(", ")", ",", ".", ";")
}