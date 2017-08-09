package ch.epfl.data.dblab.frontend.parser

import ch.epfl.data.dblab.frontend.parser.CalcAST._
import ch.epfl.data.dblab.frontend.parser.SQLAST.{ DateLiteral, DoubleLiteral, IntLiteral, StringLiteral }
import ch.epfl.data.dblab.frontend.parser.SQLParser.{ elem, floatLit, lexical }
import ch.epfl.data.dblab.schema.{ DateType, VarCharType }
import ch.epfl.data.sc.pardis.types._

import scala.util.matching.Regex
import scala.util.parsing.combinator.lexical.StdLexical
import scala.util.parsing.combinator.syntactical.StandardTokenParsers
import scala.util.parsing.input.CharArrayReader.EofCh

/**
 *  A Simple Calc Parser
 *
 * @author Parand Alizadeh
 */

object CalcParser extends StandardTokenParsers {

  def parse(statement: String): List[CalcExpr] = {
    phrase(parseAll)(new lexical.Scanner(statement)) match {
      case Success(r, q) => r
      case failure       => throw new Exception("Unable to parse CALC query!\n" + failure)
    }
  }

  def parseAll: Parser[List[CalcExpr]] = {
    rep(parseRel).? ~ rep(parseQuery) ^^ {
      case Some(l) ~ q => l ++ q
      case None ~ q    => q
    }
  }
  def parseRel: Parser[CalcExpr] = {
    "CREATE" ~> ("TABLE" | "STREAM") ~ ident ~ "(" ~ parseFieldList.? ~ ")" ~ ("FROM" ~> parseSrcStatement).? <~ ";" ^^ {
      case ts ~ name ~ _ ~ Some(fieldlist) ~ _ ~ Some(src) => Rel(ts, name, fieldlist, src)
      case ts ~ name ~ _ ~ Some(fieldlist) ~ _ ~ None      => Rel(ts, name, fieldlist, "")
      case ts ~ name ~ _ ~ None ~ _ ~ Some(src)            => Rel(ts, name, null, src)
      case ts ~ name ~ _ ~ None ~ _ ~ None                 => Rel(ts, name, null, "")
    }
  }

  def parseFieldList: Parser[List[VarT]] = {
    ident ~ parseDataType ~ ("," ~> parseFieldList).? ^^ {
      case name ~ tp ~ Some(fields) => VarT(name, tp) :: fields
      case name ~ tp ~ None         => List(VarT(name, tp))
    }
  }

  def parseDataType: Parser[Tpe] = { //TODO pardisType
    "INT" ^^^ {
      IntType
    } |
      ("DATE" ~> "(".? ~> stringLit <~ ")".? ^^^ {
        DateType
      }) |
      "DATE" ^^^ {
        DateType
      } |
      "STRING" ^^^ {
        StringType
      } |
      "FLOAT" ^^^ {
        DoubleType
      } | ("CHAR" ~> ("(" ~> numericLit <~ ")").? ^^ {
        case Some(n) => VarCharType(n.toInt) // FIXME add chars with a variable number of args
        case None    => VarCharType(1) // TODO change to CharType
      }) | ("VARCHAR" ~> "(" ~> numericLit <~ ")" ^^ {
        case n => VarCharType(n.toInt)
      })
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

  def parseQuery: Parser[CalcExpr] =
    ("DECLARE" ~> "QUERY" ~> ident ~ ":=" ~ parseCalcExpr <~ ";" ^^ {
      case name ~ _ ~ exp => CalcQuery(name, exp)
    }) | (((ident <~ ":") ~ parseCalcExpr) ^^ { case name ~ exp => CalcQuery(name, exp) })

  def parseCalcExpr: Parser[CalcExpr] =
    parseAddition ^^ {
      case ivc => ivc
    }

  def parseAddition: Parser[CalcExpr] =
    parseMultiplication * (
      "+" ^^^ { (a: CalcExpr, b: CalcExpr) => CalcSum(List(a, b)) } |
      "-" ^^^ { (a: CalcExpr, b: CalcExpr) => CalcSum(List(a, CalcNeg(b))) })

  def parseMultiplication: Parser[CalcExpr] =
    parseIvcCalcExpr * (
      "*" ^^^ { (a: CalcExpr, b: CalcExpr) => CalcProd(List(a, b)) })

  def parseIvcCalcExpr: Parser[CalcExpr] = {
    ("NEG" ~> "*" ~> parseIvcCalcExpr ^^ {
      case ivc => CalcNeg(ivc)
    }) |
      ("(" ~> parseCalcExpr <~ ")" ^^
        { case x => x }) |
        ("-" ~> parseIvcCalcExpr ^^ {
          case ivc => CalcNeg(ivc)
        }) |
        ("{" ~> parseValueExpr <~ "}" ^^ {
          case ve => CalcValue(ve)
        }) |
        ("AGGSUM" ~> "(" ~> "[" ~> (parseVarList).? ~ "]" ~ "," ~ parseCalcExpr <~ ")" ^^ {
          case Some(l) ~ _ ~ _ ~ calc => AggSum(l, calc)
          case None ~ _ ~ _ ~ calc    => AggSum(List(), calc)
        }) |
        (parseRelationDef ^^ {
          case r => r
        }) |
        (parseDeltaRelationDef ^^ {
          case dr => dr
        }) |
        (parseExternalDef ^^ {
          case e => e
        }) |
        ("{" ~> parseValueExpr ~ parseComparison ~ parseValueExpr <~ "}" ^^ {
          case v1 ~ c ~ v2 => Cmp(c, v1, v2)
        }) |
        ("(" ~> ident ~ (parseDataType).? ~ "^=" ~ parseIvcCalcExpr <~ ")" ^^ {
          case id ~ Some(t) ~ _ ~ ivc => Lift(VarT(id, t), ivc)
          case id ~ None ~ _ ~ ivc    => Lift(VarT(id, null), ivc)
        }) |
        ("EXISTS" ~> "(" ~> parseCalcExpr <~ ")" ^^ {
          case c => Exists(c)
        }) |
        (parseValueLeaf ^^ {
          case vl => CalcValue(vl)
        }) |
        ("{" ~> parseValueExpr ~ "IN" ~ "[" ~ parseValExpList <~ "]" <~ "}" ^^ {
          case id ~ _ ~ _ ~ l => In(id, l)
        })

    //TODO

    /*("DOMAIN" ~> "(" ~> parseCalcExpr <~ ")" ^^ {
        Dom
      }*/

  }

  def parseComparison: Parser[Cmp_t] = {
    ("=" ^^^ {
      Eq
    }) |
      ("!=" ^^^ {
        Neq
      }) |
      ("<" ^^^ {
        Lt
      }) |
      ("<=" ^^^ {
        Lte
      }) |
      (">" ^^^ {
        Gt
      }) |
      (">=" ^^^ {
        Gte
      })
  }
  def parseExternalDef: Parser[External] = {
    parseExternalWithoutMeta ~ (":".? ~> "(" ~> parseCalcExpr <~ ")").? ^^ {
      case et ~ Some(calc) => External(et.name, et.inps, et.outs, et.tp, Some(calc))
      case et ~ None       => et
    }
  }

  def parseExternalWithoutMeta: Parser[External] = {
    (ident ~ "[" ~ parseVarList.? ~ "]" ~ "[" ~ parseVarList.? ~ "]" ^^ {
      case id ~ _ ~ Some(l1) ~ _ ~ _ ~ Some(l2) ~ _ => External(id, l1, l2, null, None)
      case id ~ _ ~ Some(l1) ~ _ ~ _ ~ None ~ _     => External(id, l1, List(), null, None)
      case id ~ _ ~ None ~ _ ~ _ ~ Some(l2) ~ _     => External(id, List(), l2, null, None)
      case id ~ _ ~ None ~ _ ~ _ ~ None ~ _         => External(id, List(), List(), null, None)

    }) |
      (ident ~ "(" ~ parseDataType ~ ")" ~ "[" ~ parseVarList.? ~ "]" ~ "[" ~ parseVarList.? ~ "]" ^^ {
        case id ~ _ ~ tp ~ _ ~ _ ~ Some(l1) ~ _ ~ _ ~ Some(l2) ~ _ => External(id, l1, l2, tp, None)
        case id ~ _ ~ tp ~ _ ~ _ ~ Some(l1) ~ _ ~ _ ~ None ~ _     => External(id, l1, List(), tp, None)
        case id ~ _ ~ tp ~ _ ~ _ ~ None ~ _ ~ _ ~ Some(l2) ~ _     => External(id, List(), l2, tp, None)
        case id ~ _ ~ tp ~ _ ~ _ ~ None ~ _ ~ _ ~ None ~ _         => External(id, List(), List(), tp, None)
      })

  }
  def parseDeltaRelationDef: Parser[Rel] = {
    ("(" ~> "DELTA" ~> ident ~ ")" ~ "(" ~ ")" ^^ {
      case id ~ _ ~ _ ~ _ => Rel("Delta Rel", id, List(), "") //TODO type ?
    }) |
      ("(" ~> "DELTA" ~> ident ~ ")" ~ "(" ~ parseVarList ~ ")" ^^ {
        case id ~ _ ~ _ ~ l ~ _ => Rel("Delta Rel", id, l, "")
      })
  }

  def parseRelationDef: Parser[Rel] = {
    (ident ~ "(" ~ ")" ^^ {
      case id ~ _ ~ _ => Rel("Rel", id, List(), "")
    }) |
      (ident ~ "(" ~ parseVarList ~ ")" ^^ {
        case id ~ _ ~ l ~ _ => Rel("Rel", id, l, "")
      })
  }

  def parseValueExpr: Parser[ArithExpr] =
    parseValueAddition ^^ {
      case e => e
    }

  def parseValueAddition: Parser[ArithExpr] =
    parseValueMultiplication * (
      "+" ^^^ { (a: ArithExpr, b: ArithExpr) => ArithSum(List(a, b)) } |
      "-" ^^^ { (a: ArithExpr, b: ArithExpr) => ArithSum(List(a, ArithNeg(b))) })

  def parseValueMultiplication: Parser[ArithExpr] =
    parsePrimaryValueExpr * (
      "*" ^^^ { (a: ArithExpr, b: ArithExpr) => ArithProd(List(a, b)) })

  def parsePrimaryValueExpr: Parser[ArithExpr] = {
    ("(" ~> parseValueExpr <~ ")" ^^ {
      case ve => ve
    }) |
      ("-" ~> parseValueExpr ^^ {
        case v => ArithNeg(v)
      }) |
      (parseValueLeaf ^^ {
        case v => v
      })

  }

  def parseValueLeaf: Parser[ArithExpr] = {
    (parseConstant ^^ {
      case c => c
    }) |
      (ident ~ (":" ~> parseDataType).? ^^ {
        case id ~ Some(t) => ArithVar(VarT(id, t))
        case id ~ None    => ArithVar(VarT(id, null))
      }) |
      (parseFuncDef ^^ {
        case func => func
      })

  }

  def parseConstant: Parser[ArithConst] = {
    (numericLit ^^ {
      case i => ArithConst(IntLiteral(i.toInt))
    }) |
      (stringLit ^^ {
        case s => ArithConst(StringLiteral(s))
      }) |
      (floatLit ^^ {
        case f => ArithConst(DoubleLiteral(f.toDouble))
      }) |
      ("DATE" ~> "(" ~> stringLit <~ ")" ^^ {
        case d => ArithConst(DateLiteral(d))
      })
  }

  def parseFuncDef: Parser[ArithFunc] = {
    "[" ~> (ident | "/") ~ ":" ~ parseDataType ~ "]" ~ "(" ~ parseValExpList.? <~ ")" ^^ {
      case id ~ _ ~ tp ~ _ ~ _ ~ Some(varl) => ArithFunc(id, varl, tp)
      case id ~ _ ~ tp ~ _ ~ _ ~ None       => ArithFunc(id, List(), tp)
    }
  }

  def parseValExpList: Parser[List[ArithExpr]] = {
    rep(parseValueExpr <~ ",").? ~ parseValueExpr.? ^^ {
      case Some(vl) ~ Some(v) => vl :+ v
      case Some(vl) ~ None    => vl
      case None ~ None        => List()
      case None ~ Some(v)     => List(v)

    }
  }

  def parseVarList: Parser[List[VarT]] = {
    (ident ~ (":" ~> parseDataType).? ~ ("," ~> parseVarList).?) ^^ {
      case id ~ Some(t) ~ Some(l) => VarT(id, t) :: l
      case id ~ Some(t) ~ None    => List(VarT(id, t))
      case id ~ None ~ Some(l)    => VarT(id, null) :: l
      case id ~ None ~ None       => List(VarT(id, null))

    }

  }

  class CalcLexical extends StdLexical {

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

    //        override protected def comment: Parser[Any] = (
    //          	      '*' ~ '/'  ^^ { case _ => ' '  }
    //        	    | chrExcept(EofCh) ~ comment
    //        	    )

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

  override val lexical = new CalcLexical

  def floatLit: Parser[String] =
    elem("decimal", _.isInstanceOf[lexical.FloatLit]) ^^ (_.chars)

  val tokens = List("CREATE", "TABLE", "STREAM", "FROM", "INT", "DATE", "STRING", "FLOAT", "CHAR", "VARCHAR",
    "FILE", "FIXEDWIDTH", "DELIMITED", "LINE", "DECLARE", "QUERY", "NEG", "AGGSUM", "EXISTS", "DELTA", "IN")

  for (token <- tokens)
    lexical.reserved += token

  lexical.delimiters += (
    "*", "+", "-", "<", "=", "<>", "!=", "<=", ">=", ">", "||", "/", "(", ")", ",", ".", ";", ":=", "^=", "[", "]", ":", "{", "}")
}
