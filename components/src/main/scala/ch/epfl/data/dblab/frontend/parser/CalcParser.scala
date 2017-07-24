package ch.epfl.data.dblab.frontend.parser

import ch.epfl.data.dblab.frontend.parser.CalcAST._
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

  def parse(statement: String): CalcExpr = {
    phrase(parseRel | parseQuery)(new lexical.Scanner(statement)) match {
      case Success(r, q) => r
      case failure       => throw new Exception("Unable to parse CALC query!\n" + failure)
    }
  }

  def parseRel: Parser[CalcExpr] = {
    "CREATE" ~> ("TABLE" | "STREAM") ~ ident ~ "(" ~ parseFieldList.? ~ ")" ~ ("FROM" ~> parseSrcStatement).? ^^ {
      case ts ~ name ~ _ ~ Some(fieldlist) ~ _ ~ Some(src) => Rel(ts, name, fieldlist, src)
      case ts ~ name ~ _ ~ Some(fieldlist) ~ _ ~ None      => Rel(ts, name, fieldlist, "")
      case ts ~ name ~ _ ~ None ~ _ ~ Some(src)            => Rel(ts, name, null, src)
      case ts ~ name ~ _ ~ None ~ _ ~ None                 => Rel(ts, name, null, "")
    }
  }

  def parseFieldList: Parser[List[VarT]] = {
    ident ~ parseDataType ~ ("," ~> parseFieldList).? ^^ {
      case name ~ tp ~ Some(fields) => fields :+ VarT(name, tp)
      case name ~ tp ~ None         => List(VarT(name, tp))
    }
  }

  def parseDataType: Parser[Tpe] = { //TODO pardisType
    "INT" ^^^ {
      IntType
    } |
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
    "DECLARE" ~> "QUERY" ~> ident ~ ":=" ~ parseCalcExpr ^^ {
      case name ~ _ ~ exp => CalcQuery(name, exp)
    }

  def parseCalcExpr: Parser[CalcExpr] =
    parseIvcCalcExpr ^^ {
      case ivc => ivc
    }

  def parseIvcCalcExpr: Parser[CalcExpr] = {
    ("NEG" ~> "*" ~> parseIvcCalcExpr ^^ {
      case ivc => CalcNeg(ivc)
    }) |
      ("(" ~> parseIvcCalcExpr <~ ")" ^^ {
        case ivc => ivc
      }) |
      (parseIvcCalcExpr ~ "*" ~ parseIvcCalcExpr ^^ {
        case i1 ~ _ ~ i2 => CalcProd(List(i1, i2))
      }) |
      (parseIvcCalcExpr ~ "+" ~ parseIvcCalcExpr ^^ {
        case i1 ~ _ ~ i2 => CalcSum(List(i1, i2))
      }) |
      (parseIvcCalcExpr ~ "-" ~ parseIvcCalcExpr ^^ {
        case i1 ~ _ ~ i2 => CalcSum(List(i1, CalcNeg(i2)))
      }) |
      ("-" ~> parseIvcCalcExpr ^^ {
        case ivc => CalcNeg(ivc)
      }) |
      ("[" ~> parseValueExpr <~ "]" ^^ {
        case ve => CalcValue(ve)
      }) |
      (parseValueLeaf ^^{
        case vl => CalcValue(vl)
      }) |
      ("AGGSUM" ~> "(" ~> "[" ~> (parseVarList).? ~ "]" ~ "," ~ parseIvcCalcExpr <~ ")" ^^{
        case Some(l) ~_~_~ calc => AggSum(l, calc)
        case None ~_~_~ calc => AggSum(List(), calc)
      }) |
      (parseRelationDef ^^ {
        case r => r
      }) |
      (parseDeltaRelationDef ^^ {
        case dr => dr
      })



  }

  def parseDeltaRelationDef: Parser[Rel]= {
    ("(" ~> "DELTA" ~> ident ~ ")" ~ "(" ~ ")" ^^ {
      case id ~ _ ~ _ ~_ => Rel("Delta Rel", id , List(), "")
    }) |
      ("(" ~> "DELTA" ~> ident ~ ")"~ "(" ~ parseVarList ~ ")" ^^{
        case id ~_ ~ _ ~ l ~_ => Rel("Delta Rel", id, l, "")
      } )
  }

  def parseRelationDef: Parser[Rel] ={
    (ident ~ "(" ~ ")" ^^ {
      case id ~ _ ~ _ => Rel("Rel", id , List(), "")
    }) |
      (ident ~ "(" ~ parseVarList ~ ")" ^^{
        case id ~_~ l ~_ => Rel("Rel", id, l, "")
      } )
  }
  def parseValueExpr: Parser[ArithExpr] = {
    ("(" ~> parseValueExpr <~ ")" ^^ {
      case ve => ve
    }) |
      (parseValueExpr ~ "*" ~ parseValueExpr ^^ {
        case v1 ~ _ ~ v2 => ArithProd(List(v1, v2))
      }) |
      (parseValueExpr ~ "+" ~ parseValueExpr ^^ {
        case v1 ~ _ ~ v2 => ArithSum(List(v1, v2))
      }) |
      (parseValueExpr ~ "-" ~ parseValueExpr ^^ {
        case v1 ~ _ ~ v2 => ArithSum(List(v1, ArithNeg(v2)))
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
    })|
      (ident ~ (":" ~> parseDataType).? ^^ {
        case id ~ Some(t) => ArithVar(VarT(id , t))
    })|
      (parseFuncDef ^^{
        case func => func
    })

  }



  def parseConstant: Parser[ArithConst] = { //TODO
    ???
  }


  def parseFuncDef: Parser[ArithFunc] = {
    "[" ~> (ident|"/") ~ ":" ~ parseDataType ~ "]" ~ "(" ~ parseValExpList.? <~ ")" ^^{
      case id ~_~ tp ~_~_~ Some(varl) => ArithFunc(id, varl, tp)
      case id ~_~ tp ~_~_~ None => ArithFunc(id, List(), tp)
    }
  }

  def parseValExpList: Parser[List[ArithExpr]] = {
    rep(parseValueExpr <~ ",").? ~ parseValueExpr.? ^^ {
      case Some(vl) ~ Some(v) => vl :+ v
      case Some(vl) ~ None => vl
      case None ~ None => List()
      case None ~ Some(v) => List(v)

    }
  }

  def parseVarList: Parser[List[VarT]] = {
    (ident ~ (":" ~> parseDataType).? ~ parseVarList.?) ^^ {
      case id ~ Some(t) ~ Some(l) => l :+ VarT(id, t)
      case id ~ Some(t) ~ None => List(VarT(id, t))
      case id ~ None ~ Some(l) => l :+ VarT(id, null)
      case id ~ None ~ None => List(VarT(id, null))

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

  override val lexical = new CalcLexical

  val tokens = List("CREATE", "TABLE", "STREAM", "AGGSUM")

  for (token <- tokens)
    lexical.reserved += token

  lexical.delimiters += (
    "*", "+", "-", "<", "=", "<>", "!=", "<=", ">=", ">", "||", "/", "(", ")", ",", ".", ";", ":=")
}
