package ch.epfl.data
package dblab
package frontend
package parser

import schema._
import scala.language.postfixOps
import scala.util.parsing.combinator._
import scala.util.parsing.combinator.syntactical._
import DDLAST._

/**
 * A DDL parser.
 * Based on: https://github.com/folone/DDL-comparer/blob/master/src/main/scala/DDLParser.scala with extensive modifications
 *
 * @author Yannis Klonatos
 */
object DDLParser extends JavaTokenParsers {

  val tableName = """(?!(?i)KEY)(?!(?i)PRIMARY)(?!(?i)UNIQUE)(`)?[a-zA-Z_0-9]+(`)?""".r
  val schemaName = tableName
  val columnName = tableName
  val default = tableName
  val keyName = tableName

  // Handle comments
  protected override val whiteSpace = """(\s|--.*|#.*|(?m)/\*(\*(?!/)|[^*])*\*/;*)+""".r

  def intType: Parser[DDLInteger] =
    "INTEGER" ^^ { case _ => new DDLInteger() }
  def charType: Parser[DDLChar] =
    "CHAR" ~ "(" ~> wholeNumber <~ ")" ^^ { case num => DDLChar(num.toInt) }
  def varcharType: Parser[DDLVarChar] =
    "VARCHAR" ~ "(" ~> wholeNumber <~ ")" ^^ { case num => DDLVarChar(num.toInt) }
  def decimalType: Parser[DDLDecimal] =
    "DECIMAL" ~ "(" ~> wholeNumber ~ "," ~ wholeNumber <~ ")" ^^ {
      case num ~ "," ~ num2 => DDLDecimal(num.toInt, num2.toInt)
    }
  def dateType: Parser[DDLDate] =
    "DATE" ^^ { case _ => new DDLDate() }
  def dataType: Parser[AttributeType] = intType | charType | varcharType | decimalType | dateType

  val statementTermination = ";"
  val columnDelimiter = """,*""".r

  def cleanString(str: String) = str.replaceAll("`", "")

  def column = columnName ~ dataType ~
    ("""(?i)NOT NULL""".r?) ~
    ("""(?i)AUTO_INCREMENT""".r?) ~
    ((("""(?i)DEFAULT""".r) ~ default)?) ~
    rep(annotations) ~
    columnDelimiter

  def uniqueOrPk = ("""(?i)(PRIMARY|UNIQUE)""".r?) ~ ("""(?i)KEY""".r) ~ "(" ~ repsep(columnName, ",") ~ ")" ~ columnDelimiter

  def fk = """FOREIGN KEY""".r ~ keyName ~ "(" ~ repsep(columnName, ",") ~ ")" ~ """(?i)REFERENCES""".r ~ tableName ~ columnDelimiter

  def constraint = (uniqueOrPk | fk)

  def compressed = "COMPRESSED"

  def annotations = compressed

  def createTable = ("""(?i)CREATE TABLE""".r) ~ ("""(?i)IF NOT EXISTS""".r?) ~ tableName ~
    "(" ~ (column*) ~ (constraint*) ~ ")" ^^ {
      case _ ~ _ ~ name ~ "(" ~ columns ~ cons ~ ")" => {
        val constraints = cons map {
          case (kind: Option[String]) ~ _ ~ "(" ~ (column: List[String]) ~ ")" ~ _ =>
            kind match {
              case Some(x) if x.equalsIgnoreCase("primary") => ConstraintOp(true, DDLPrimaryKey(name, column))
              case Some(x) if x.equalsIgnoreCase("unique")  => ConstraintOp(true, DDLUniqueKey(name, column))
            }
          case _ ~ (kn: String) ~ "(" ~ (columnName: List[String]) ~ ")" ~ _ ~ tableName ~ _ =>
            ConstraintOp(true, DDLForeignKey(name, kn, columnName, tableName))
        }

        val columnsData = columns map {
          case colName ~ colType ~ notNull ~ autoInc ~ isDefault ~ annotations ~ _ =>
            Column(cleanString(colName), colType, notNull.isDefined,
              autoInc.isDefined, isDefault.map(_._2), annotations)
        }
        DDLTable(cleanString(name), columnsData, constraints.toSet)
      }
    }

  def alterTableAddPrimary = ("ALTER TABLE" ~> tableName) ~ ("ADD" ~ "PRIMARY KEY (" ~> repsep(columnName, ",") <~ ")") ^^ {
    case tn ~ cols => ConstraintOp(true, DDLPrimaryKey(tn, cols))
  }
  def alterTableAddForeign = ("ALTER TABLE" ~> tableName) ~ ("ADD" ~ "FOREIGN KEY " ~> keyName) ~ ("(" ~> repsep(columnName, ",") <~ ")") ~ ("REFERENCES" ~> tableName) ^^ {
    case tn ~ kn ~ cols ~ ftn => ConstraintOp(true, DDLForeignKey(tn, kn, cols, ftn))
  }

  def alterTableAdd = alterTableAddPrimary | alterTableAddForeign

  // TODO -- Add handling for droping foreign keys
  def alterTableDrop: Parser[CatalogOp] =
    "ALTER TABLE" ~> tableName <~ "DROP" ~ "PRIMARY" ~ "KEY" ^^ {
      case nm => ConstraintOp(false, DDLPrimaryKey(nm, null))
    }

  def alterTable = alterTableAdd | alterTableDrop

  def dropTable = """(?i)DROP TABLE""".r ~> tableName ^^ { DropTable(_) }

  def useSchema: Parser[CatalogOp] = "USE" ~> schemaName ^^ { UseSchema(_) }

  def statement = (useSchema | createTable | alterTable | dropTable) ~ statementTermination ^^ {
    case res ~ _ => res
  }

  def ddlDef = statement*

  def parse(sql: String) = parseAll(ddlDef, sql) map (_.toList) getOrElse (
    throw new Exception("Unable to parse DDL or constraints statement!"))
}