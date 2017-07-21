package ch.epfl.data.dblab.frontend.parser

import ch.epfl.data.dblab.frontend.parser.CalcAST.{CalcExpr, Rel, VarT}

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
    "CREATE" ~> "TABLE" | "STREAM" ~ ident ~ "(" ~ parseFieldList.? ~ ")" ~ ("FROM" ~> parseSrcStatement).? ^^ {
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

  def parseDataType: Parser[String] = { //TODO pardisType
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

  def parseAdaptorStatement: Parser[String] = (
    ident <~ ("(" <~ ")").? ^^ {
      case id => id
    }
    | ident ~ "(" ~ parseAdaptorParams ~ ")" ^^ {
      case id ~ _ ~ param ~ _ => id + param
    })
  def parseAdaptorParams: Parser[String] = (
    rep(ident ~ "SET VALUE" ~ stringLit ~ ",").? ~ ident ~ "SET VALUE" ~ stringLit ^^ {
      case Some(s) ~ id ~ sv ~ str => s + id + sv + str
    }
    | stringLit ^^ {
      case s => s
    })


  def parseQuery: Parser[CalcExpr] = {

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

  val tokens = List("CREATE" , "TABLE" , "STREAM")

  for (token <- tokens)
    lexical.reserved += token

  lexical.delimiters += (
    "*", "+", "-", "<", "=", "<>", "!=", "<=", ">=", ">", "||", "/", "(", ")", ",", ".", ";")
}
